"""Safely prove OPEN-to-RESOLVED alert recovery without calling Alpaca."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import psycopg

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file
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


EVENT_ID = "FOMC|2026-07|2026-07-29T18:00:00Z"
SYMBOL = "SPY"
STAGE = "ALERT_DRILL"


class DrillDatabase(Protocol):
    def begin(self) -> None: ...
    def record_failure(self, error: Exception) -> None: ...
    def record_success(self, rows: list[dict]) -> None: ...
    def snapshot(self) -> dict: ...


class DrillClient(Protocol):
    def fetch(self) -> list[dict]: ...


def run_alert_drill(
    *,
    database: DrillDatabase,
    first_client: DrillClient,
    retry_client: DrillClient,
) -> tuple[dict, dict]:
    """Execute the expected failure and one successful retry."""

    database.begin()
    try:
        first_client.fetch()
    except Exception as error:
        database.record_failure(error)
    else:
        raise RuntimeError("first alert-drill request did not fail")

    failure = database.snapshot()
    if (failure.get("work_status"), failure.get("alert_status")) != (
        "FAILED",
        "OPEN",
    ):
        raise RuntimeError("failure did not create FAILED/OPEN state")

    rows = retry_client.fetch()
    database.record_success(rows)
    recovery = database.snapshot()
    if (recovery.get("work_status"), recovery.get("alert_status")) != (
        "SUCCEEDED",
        "RESOLVED",
    ):
        raise RuntimeError("retry did not create SUCCEEDED/RESOLVED state")
    if int(recovery.get("business_key_duplicates", -1)) != 0:
        raise RuntimeError("retry created duplicate business keys")
    return failure, recovery


class Always503Client:
    def fetch(self) -> list[dict]:
        raise RuntimeError("simulated provider HTTP 503")


class FixtureBarsClient:
    def __init__(self, path: Path) -> None:
        self._path = path

    def fetch(self) -> list[dict]:
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        if not isinstance(payload, list) or not payload:
            raise ValueError("alert drill fixture must be a non-empty list")
        return payload


class PostgresDrillDatabase:
    def __init__(self, database_url: str, pipeline_run_id: str) -> None:
        self.database_url = database_url
        self.pipeline_run_id = pipeline_run_id

    def ensure_schema(self) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                for path in (
                    Path("db/migrations/001_market_bars.sql"),
                    Path("db/migrations/005_derived_bar_coverage.sql"),
                    Path("db/migrations/006_pipeline_runs.sql"),
                ):
                    cursor.execute(path.read_text(encoding="utf-8"))

    def begin(self) -> None:
        now = datetime.now(UTC)
        config = {"fixture": "scripts/fixtures/alert_drill_bars.json", "attempts": 2}
        start_pipeline_run(
            self.database_url,
            PipelineRun(
                pipeline_run_id=self.pipeline_run_id,
                dag_id="safe_pipeline_alert_drill",
                config=config,
                config_hash=canonical_config_hash(config),
                data_cutoff=now,
                code_version=os.environ.get("GIT_COMMIT", "local"),
                status="RUNNING",
                started_at=now,
            ),
        )
        mark_work_item(
            self.database_url,
            PipelineWorkItem(
                pipeline_run_id=self.pipeline_run_id,
                economic_event_id=EVENT_ID,
                symbol=SYMBOL,
                stage=STAGE,
                status="RUNNING",
                attempt_count=1,
            ),
        )

    def record_failure(self, error: Exception) -> None:
        mark_work_item(
            self.database_url,
            PipelineWorkItem(
                pipeline_run_id=self.pipeline_run_id,
                economic_event_id=EVENT_ID,
                symbol=SYMBOL,
                stage=STAGE,
                status="FAILED",
                attempt_count=1,
                error_code="HTTP_503",
                error_message=str(error),
            ),
        )
        record_pipeline_check(
            self.database_url,
            PipelineCheck(
                pipeline_run_id=self.pipeline_run_id,
                economic_event_id=EVENT_ID,
                symbol=SYMBOL,
                stage=STAGE,
                check_name="provider_request",
                expected_value="HTTP 200",
                actual_value="HTTP 503",
                status="FAIL",
                alert_status="OPEN",
                checked_at=datetime.now(UTC),
            ),
        )

    def record_success(self, rows: list[dict]) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(
                        """
                        INSERT INTO market_bars (
                            symbol, bar_start, timeframe, open, high, low, close,
                            volume, trade_count, vwap, source, feed, is_final,
                            condition_policy, spark_batch_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, 'synthetic_fixture', 'sip', TRUE,
                            'alert_drill_fixture_v1', -1
                        )
                        ON CONFLICT (symbol, bar_start, timeframe, source, feed)
                        DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            trade_count = EXCLUDED.trade_count,
                            vwap = EXCLUDED.vwap,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            row["symbol"], row["bar_start"], row["timeframe"],
                            row["open"], row["high"], row["low"], row["close"],
                            row["volume"], row["trade_count"], row["vwap"],
                        ),
                    )
        mark_work_item(
            self.database_url,
            PipelineWorkItem(
                pipeline_run_id=self.pipeline_run_id,
                economic_event_id=EVENT_ID,
                symbol=SYMBOL,
                stage=STAGE,
                status="SUCCEEDED",
                attempt_count=2,
                input_count=len(rows),
                output_count=len(rows),
            ),
        )
        record_pipeline_check(
            self.database_url,
            PipelineCheck(
                pipeline_run_id=self.pipeline_run_id,
                economic_event_id=EVENT_ID,
                symbol=SYMBOL,
                stage=STAGE,
                check_name="provider_request",
                expected_value="HTTP 200",
                actual_value="fixture response accepted",
                status="PASS",
                alert_status="RESOLVED",
                checked_at=datetime.now(UTC),
            ),
        )
        finish_pipeline_run(
            self.database_url,
            self.pipeline_run_id,
            "SUCCEEDED",
            datetime.now(UTC),
        )

    def snapshot(self) -> dict:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, attempt_count, error_code
                    FROM pipeline_work_items
                    WHERE pipeline_run_id = %s AND economic_event_id = %s
                      AND symbol = %s AND stage = %s
                    """,
                    (self.pipeline_run_id, EVENT_ID, SYMBOL, STAGE),
                )
                work_status, attempt_count, error_code = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT status, alert_status
                    FROM pipeline_run_checks
                    WHERE pipeline_run_id = %s AND economic_event_id = %s
                      AND symbol = %s AND stage = %s
                      AND check_name = 'provider_request'
                    """,
                    (self.pipeline_run_id, EVENT_ID, SYMBOL, STAGE),
                )
                check_status, alert_status = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM market_bars
                    WHERE source = 'synthetic_fixture'
                      AND condition_policy = 'alert_drill_fixture_v1'
                    """
                )
                stored_rows = int(cursor.fetchone()[0])
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT symbol, bar_start, timeframe, source, feed
                        FROM market_bars
                        WHERE source = 'synthetic_fixture'
                          AND condition_policy = 'alert_drill_fixture_v1'
                        GROUP BY symbol, bar_start, timeframe, source, feed
                        HAVING COUNT(*) > 1
                    ) duplicate_keys
                    """
                )
                duplicates = int(cursor.fetchone()[0])
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "work_item": {"economic_event_id": EVENT_ID, "symbol": SYMBOL, "stage": STAGE},
            "work_status": work_status,
            "attempt_count": attempt_count,
            "error_code": error_code,
            "check_status": check_status,
            "alert_status": alert_status,
            "stored_rows": stored_rows,
            "business_key_duplicates": duplicates,
            "data_label": "committed synthetic fixture; no external API request",
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output-dir", type=Path, default=Path("docs/evidence/sixth-assignment")
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env = _read_env_file(args.env_file)
    database_url = os.environ.get("DATABASE_URL") or env.get("DATABASE_URL") or DEFAULT_DATABASE_URL
    store = PostgresDrillDatabase(
        database_url,
        pipeline_run_id=f"alert-drill-{uuid4().hex[:12]}",
    )
    store.ensure_schema()
    failure, recovery = run_alert_drill(
        database=store,
        first_client=Always503Client(),
        retry_client=FixtureBarsClient(
            Path("scripts/fixtures/alert_drill_bars.json")
        ),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "alert-failure.json": failure,
        "alert-recovery.json": recovery,
    }
    for filename, payload in outputs.items():
        content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        if "postgresql://" in content or "api_key=" in content.lower():
            raise RuntimeError("credential-like value found in public evidence")
        (args.output_dir / filename).write_text(content, encoding="utf-8")
    print(json.dumps({"failure": failure, "recovery": recovery}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
