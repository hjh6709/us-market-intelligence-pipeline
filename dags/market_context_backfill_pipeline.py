"""Backfill economic-event market context as event-symbol Airflow work items."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta

import psycopg
from airflow.sdk import Param, dag, get_current_context, task

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.market_context_backfill import (
    MarketContextWorkItem,
    collect_market_context_work_item,
    select_market_context_work,
)
from src.pipeline_run_tracking import (
    PipelineCheck,
    PipelineRun,
    PipelineWorkItem,
    canonical_config_hash,
    finish_pipeline_run,
    mark_work_item,
    record_pipeline_check,
    start_pipeline_run,
)


DEFAULT_SYMBOLS = ["SPY", "QQQ", "IWM", "TLT", "XLF", "SMH", "GLD", "NVDA", "AAPL", "JPM"]


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def _work_item_dict(item: MarketContextWorkItem, pipeline_run_id: str) -> dict:
    values = asdict(item)
    values["release_date"] = item.release_date.isoformat()
    values["released_at"] = item.released_at.isoformat()
    values["pipeline_run_id"] = pipeline_run_id
    return values


def _restore_work_item(values: dict) -> MarketContextWorkItem:
    item_values = {key: value for key, value in values.items() if key != "pipeline_run_id"}
    item_values["release_date"] = date.fromisoformat(item_values["release_date"])
    item_values["released_at"] = datetime.fromisoformat(
        item_values["released_at"].replace("Z", "+00:00")
    )
    return MarketContextWorkItem(**item_values)


@dag(
    dag_id="market_context_backfill_pipeline",
    description="Collect ±event market bars and derive coverage-aware 3m/5m bars",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    params={
        "event_types": Param(
            ["CPI", "EMPLOYMENT", "PCE", "FOMC"],
            type="array",
            items={"type": "string"},
            minItems=1,
            uniqueItems=True,
        ),
        "release_from": Param("2022-01-01", type="string", format="date"),
        "release_to": Param("2026-08-26", type="string", format="date"),
        "symbols": Param(
            DEFAULT_SYMBOLS,
            type="array",
            items={"type": "string", "pattern": r"^[A-Z][A-Z0-9.-]{0,9}$"},
            minItems=1,
            uniqueItems=True,
        ),
        "feed": Param("sip", type="string", enum=["sip", "iex"]),
        "data_cutoff": Param(
            "2026-09-03T00:00:00Z", type="string", format="date-time"
        ),
    },
    tags=["market-data", "economic-events", "backfill", "assignment"],
)
def build_market_context_backfill_pipeline():
    @task(task_id="validate_run_config")
    def validate_run_config() -> dict:
        context = get_current_context()
        config = {
            key: context["params"][key]
            for key in (
                "event_types",
                "release_from",
                "release_to",
                "symbols",
                "feed",
                "data_cutoff",
            )
        }
        cutoff = datetime.fromisoformat(config["data_cutoff"].replace("Z", "+00:00"))
        if cutoff.tzinfo is None:
            raise ValueError("data_cutoff must include a timezone")
        select_market_context_work(config)
        config["pipeline_run_id"] = context["run_id"]
        return config

    @task(task_id="register_run", pool="postgres_write_pool")
    def register_run(config: dict) -> dict:
        now = datetime.now(UTC)
        cutoff = datetime.fromisoformat(config["data_cutoff"].replace("Z", "+00:00"))
        start_pipeline_run(
            _database_url(),
            PipelineRun(
                pipeline_run_id=config["pipeline_run_id"],
                dag_id="market_context_backfill_pipeline",
                config={key: value for key, value in config.items() if key != "pipeline_run_id"},
                config_hash=canonical_config_hash(
                    {key: value for key, value in config.items() if key != "pipeline_run_id"}
                ),
                data_cutoff=cutoff,
                code_version=os.environ.get("GIT_COMMIT", "local"),
                status="RUNNING",
                started_at=now,
            ),
        )
        return config

    @task(task_id="build_work_items")
    def build_work_items(config: dict) -> list[dict]:
        return [
            _work_item_dict(item, config["pipeline_run_id"])
            for item in select_market_context_work(config)
        ]

    @task(
        task_id="collect_market_context",
        pool="alpaca_api_pool",
        max_active_tis_per_dag=4,
    )
    def collect_market_context(values: dict) -> dict:
        from src.historical_bars import AlpacaHistoricalBarsClient
        from src.live_market_smoke import load_credentials

        context = get_current_context()
        attempt = int(context["ti"].try_number)
        pipeline_run_id = values["pipeline_run_id"]
        item = _restore_work_item(values)
        database_url = _database_url()
        stage = "MARKET_CONTEXT"
        mark_work_item(
            database_url,
            PipelineWorkItem(
                pipeline_run_id=pipeline_run_id,
                economic_event_id=item.event_id,
                symbol=item.symbol,
                stage=stage,
                status="RUNNING",
                attempt_count=attempt,
            ),
        )
        try:
            key_id, secret_key = load_credentials()
            result = collect_market_context_work_item(
                item,
                client=AlpacaHistoricalBarsClient(
                    key_id, secret_key, timeout_seconds=30.0
                ),
                database_url=database_url,
                provider_available_until=datetime.fromisoformat(
                    context["params"]["data_cutoff"].replace("Z", "+00:00")
                ),
            )
            unavailable = result.coverage_status in {
                "MARKET_CLOSED",
                "NO_MARKET_DATA",
                "FUTURE_SESSION_UNAVAILABLE",
            }
            mark_work_item(
                database_url,
                PipelineWorkItem(
                    pipeline_run_id=pipeline_run_id,
                    economic_event_id=item.event_id,
                    symbol=item.symbol,
                    stage=stage,
                    status="DATA_NOT_AVAILABLE" if unavailable else "SUCCEEDED",
                    attempt_count=attempt,
                    input_count=result.session_1m_rows + result.daily_rows,
                    output_count=(
                        result.session_1m_rows
                        + result.derived_3m_rows
                        + result.derived_5m_rows
                        + result.daily_rows
                    ),
                ),
            )
            check_status = "PASS" if result.coverage_status == "COMPLETE" else "WARN"
            record_pipeline_check(
                database_url,
                PipelineCheck(
                    pipeline_run_id=pipeline_run_id,
                    economic_event_id=item.event_id,
                    symbol=item.symbol,
                    stage=stage,
                    check_name="collection",
                    expected_value="COMPLETE",
                    actual_value=result.coverage_status,
                    status=check_status,
                    alert_status="RESOLVED" if attempt > 1 else "NONE",
                    checked_at=datetime.now(UTC),
                ),
            )
            return asdict(result)
        except Exception as error:
            mark_work_item(
                database_url,
                PipelineWorkItem(
                    pipeline_run_id=pipeline_run_id,
                    economic_event_id=item.event_id,
                    symbol=item.symbol,
                    stage=stage,
                    status="FAILED",
                    attempt_count=attempt,
                    error_code=type(error).__name__,
                    error_message=str(error),
                ),
            )
            record_pipeline_check(
                database_url,
                PipelineCheck(
                    pipeline_run_id=pipeline_run_id,
                    economic_event_id=item.event_id,
                    symbol=item.symbol,
                    stage=stage,
                    check_name="collection",
                    expected_value="successful provider request and storage",
                    actual_value=type(error).__name__,
                    status="FAIL",
                    alert_status="OPEN",
                    checked_at=datetime.now(UTC),
                ),
            )
            finish_pipeline_run(database_url, pipeline_run_id, "FAILED", datetime.now(UTC))
            raise

    @task(task_id="verify_run", pool="postgres_write_pool")
    def verify_run(results: list[dict], config: dict) -> dict:
        pipeline_run_id = config["pipeline_run_id"]
        database_url = _database_url()
        with psycopg.connect(database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, COUNT(*)
                    FROM pipeline_work_items
                    WHERE pipeline_run_id = %s
                    GROUP BY status
                    """,
                    (pipeline_run_id,),
                )
                statuses = dict(cursor.fetchall())
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM pipeline_run_checks
                    WHERE pipeline_run_id = %s
                      AND status = 'FAIL' AND alert_status = 'OPEN'
                    """,
                    (pipeline_run_id,),
                )
                open_alerts = int(cursor.fetchone()[0])
        accepted = statuses.get("SUCCEEDED", 0) + statuses.get("DATA_NOT_AVAILABLE", 0)
        if accepted != len(results) or statuses.get("FAILED", 0) or open_alerts:
            finish_pipeline_run(database_url, pipeline_run_id, "FAILED", datetime.now(UTC))
            raise RuntimeError(
                f"pipeline verification failed: expected={len(results)} "
                f"accepted={accepted} failed={statuses.get('FAILED', 0)} "
                f"open_alerts={open_alerts}"
            )
        return {
            "pipeline_run_id": pipeline_run_id,
            "selected_work_items": len(results),
            "accepted_work_items": accepted,
            "open_alerts": open_alerts,
        }

    @task(task_id="finish_run", pool="postgres_write_pool")
    def finish_run(summary: dict) -> dict:
        finish_pipeline_run(
            _database_url(),
            summary["pipeline_run_id"],
            "SUCCEEDED",
            datetime.now(UTC),
        )
        return summary

    config = validate_run_config()
    registered = register_run(config)
    work_items = build_work_items(registered)
    results = collect_market_context.expand(values=work_items)
    verified = verify_run(results, registered)
    finish_run(verified)


market_context_backfill_pipeline = build_market_context_backfill_pipeline()
