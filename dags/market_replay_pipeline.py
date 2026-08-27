"""Parameter-driven Alpaca SIP replay through Kafka, Spark, and PostgreSQL."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import timedelta

from airflow.sdk import Param, dag, get_current_context, task

from src.airflow_market_replay import (
    MarketReplayConfig,
    build_consumer_args,
    build_replay_args,
    build_spark_args,
    validate_run_configs,
    verify_spark_result,
    verify_stored_result as verify_postgres_result,
)


LOGGER = logging.getLogger(__name__)


def _log_summary(summary: dict) -> None:
    LOGGER.info("pipeline_summary=%s", json.dumps(summary, ensure_ascii=False))


@dag(
    dag_id="market_sip_replay_pipeline",
    description="Replay parameterized Alpaca trades through Kafka and Spark",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    params={
        "tickers": Param(
            ["SPY", "QQQ", "SMH", "NVDA"],
            type="array",
            items={"type": "string", "pattern": r"^[A-Z][A-Z0-9.-]{0,9}$"},
            minItems=1,
            uniqueItems=True,
            title="Tickers",
        ),
        "start": Param(
            "2026-08-12T12:25:00Z",
            type="string",
            format="date-time",
            title="Start time (UTC)",
        ),
        "end": Param(
            "2026-08-12T12:35:00Z",
            type="string",
            format="date-time",
            title="End time (UTC)",
        ),
        "feed": Param("sip", type="string", enum=["sip", "iex"], title="Feed"),
    },
    tags=["market-data", "kafka", "spark", "assignment"],
)
def build_market_sip_replay_pipeline():
    @task(task_id="validate_run_config")
    def validate_task() -> list[dict]:
        context = get_current_context()
        configs = validate_run_configs(
            context["params"],
            run_id=context["run_id"],
        )
        summaries = [asdict(config) for config in configs]
        _log_summary(
            {
                "step": "validated_configs",
                "tickers": [config.ticker for config in configs],
            }
        )
        return summaries

    @task(task_id="replay_trades_to_kafka")
    def replay_task(config_values: dict) -> dict:
        from src.historical_market_replay import run

        config = MarketReplayConfig(**config_values)
        replay_summary = run(build_replay_args(config))
        if int(replay_summary["published_trades"]) < 1:
            raise RuntimeError("Alpaca replay published no trades")
        _log_summary(replay_summary)
        return {"config": config_values, "replay": replay_summary}

    @task(task_id="verify_kafka_delivery")
    def consumer_task(replay_bundle: dict) -> dict:
        from src.kafka_trace_consumer import run

        replay_summary = replay_bundle["replay"]
        consumer_summary = run(build_consumer_args(replay_summary))
        if int(consumer_summary["consumer_received"]) != int(
            consumer_summary["expected_count"]
        ):
            raise RuntimeError("Kafka consumer count did not match published count")
        _log_summary(consumer_summary)
        return {**replay_bundle, "consumer": consumer_summary}

    @task(task_id="build_minute_bars_with_spark")
    def spark_task(delivery_bundle: dict) -> dict:
        from src.spark_sip_trade_batch import run

        config = MarketReplayConfig(**delivery_bundle["config"])
        spark_summary = run(build_spark_args(config, delivery_bundle["replay"]))
        verify_spark_result(
            {
                **delivery_bundle["replay"],
                **delivery_bundle["consumer"],
                **spark_summary,
            }
        )
        _log_summary(spark_summary)
        return {**delivery_bundle, "spark": spark_summary}

    @task(task_id="verify_stored_result")
    def postgres_task(spark_bundle: dict) -> dict:
        config = MarketReplayConfig(**spark_bundle["config"])
        spark_args = build_spark_args(config, spark_bundle["replay"])
        postgres_summary = verify_postgres_result(
            config,
            spark_bundle["spark"],
            database_url=spark_args.database_url,
        )
        _log_summary(postgres_summary)
        return {
            "config": spark_bundle["config"],
            "replay": spark_bundle["replay"],
            "consumer": spark_bundle["consumer"],
            "spark": spark_bundle["spark"],
            "postgres": postgres_summary,
        }

    validated = validate_task()
    replayed = replay_task.expand(config_values=validated)
    delivered = consumer_task.expand(replay_bundle=replayed)
    processed = spark_task.expand(delivery_bundle=delivered)
    postgres_task.expand(spark_bundle=processed)


market_sip_replay_pipeline = build_market_sip_replay_pipeline()
