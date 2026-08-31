"""Measured archive replay experiment shared by local and GCP runs."""

from __future__ import annotations

import json
import time
import uuid
from argparse import Namespace
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

import psycopg
from confluent_kafka import Consumer

from src.archive_kafka_replay import replay_archive
from src.kafka_publisher import KafkaPublisher
from src.kafka_trace_consumer import count_trace_messages
from src.market_trade_archive import ArchiveManifest
from src.spark_sip_trade_batch import run as run_spark_batch


@dataclass(frozen=True)
class ExperimentResult:
    experiment_run_id: str
    dataset_id: str
    environment: str
    status: str
    raw_input_trades: int
    kafka_published: int
    kafka_consumed: int
    spark_input: int
    spark_invalid: int
    spark_duplicates: int
    spark_output_bars: int
    postgres_stored_bars: int
    postgres_business_key_duplicates: int
    duration_seconds: float
    events_per_second: float
    error_type: str | None = None
    error_message: str | None = None


def write_result(result: ExperimentResult, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)


def _business_key_duplicates(database_url: str) -> int:
    query = """
        SELECT COUNT(*)
        FROM (
            SELECT symbol, bar_start, timeframe, source, feed
            FROM market_bars
            GROUP BY symbol, bar_start, timeframe, source, feed
            HAVING COUNT(*) > 1
        ) duplicated
    """
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return int(cursor.fetchone()[0])


def run_experiment(
    manifests: Sequence[ArchiveManifest],
    *,
    dataset_id: str,
    environment: str,
    bootstrap_servers: str,
    topic: str,
    database_url: str,
    consumer_timeout_seconds: float = 600.0,
    experiment_run_id: str | None = None,
) -> ExperimentResult:
    if not manifests:
        raise ValueError("experiment requires at least one archive partition")
    run_id = experiment_run_id or f"experiment-{uuid.uuid4()}"
    started = time.monotonic()
    expected = sum(item.row_count for item in manifests)
    symbols = sorted({item.partition.symbol for item in manifests})
    publisher = KafkaPublisher(bootstrap_servers, topic=topic)
    replay = None
    try:
        replay = replay_archive(manifests, publisher=publisher, trace_id=run_id)
    finally:
        publisher.close(timeout_seconds=120.0)
    offset_ranges = publisher.offset_ranges

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"experiment-count-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumed, _ = count_trace_messages(
            consumer,
            topic=topic,
            trace_id=run_id,
            expected_count=expected,
            offset_ranges=offset_ranges,
            timeout_seconds=consumer_timeout_seconds,
        )
    finally:
        consumer.close()
    if consumed != expected:
        raise RuntimeError(f"Kafka count mismatch: expected {expected}, consumed {consumed}")

    spark = run_spark_batch(
        Namespace(
            trace_id=run_id,
            topic=topic,
            symbols=symbols,
            offset_ranges=offset_ranges,
            bootstrap_servers=bootstrap_servers,
            database_url=database_url,
        )
    )
    spark_input = int(spark["spark_input_trades"])
    spark_unique = int(spark["spark_valid_unique_trades"])
    result = ExperimentResult(
        experiment_run_id=run_id,
        dataset_id=dataset_id,
        environment=environment,
        status="succeeded",
        raw_input_trades=expected,
        kafka_published=replay.published_trades,
        kafka_consumed=consumed,
        spark_input=spark_input,
        spark_invalid=int(spark["spark_invalid_trades"]),
        spark_duplicates=spark_input - int(spark["spark_invalid_trades"]) - spark_unique,
        spark_output_bars=int(spark["spark_output_bars"]),
        postgres_stored_bars=int(spark["postgres_upserted_bars"]),
        postgres_business_key_duplicates=_business_key_duplicates(database_url),
        duration_seconds=round(time.monotonic() - started, 6),
        events_per_second=round(
            expected / (time.monotonic() - started), 3
        ),
    )
    return result
