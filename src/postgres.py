"""Transactional PostgreSQL sink for finalized Spark market bars."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


MARKET_BAR_COLUMNS = (
    "symbol",
    "bar_start",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "source",
    "feed",
    "is_final",
    "condition_policy",
)

RECORD_VALIDATION_RUN_SQL = "SELECT record_validation_run(%s, %s, %s, %s, %s)"

RECORD_VALIDATION_BAR_SQL = """
SELECT record_validation_reconstructed_bar(
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s
)
"""


def validation_bar_rows(
    batch_df: DataFrame,
    spark_batch_id: int,
    *,
    validation_run_id: str,
) -> list[tuple]:
    """Convert raw-derived finalized bars into validation-storage rows."""
    if not validation_run_id:
        raise ValueError("validation_run_id must not be empty")
    records = []
    selected = batch_df.select(
        F.col("symbol"),
        F.unix_micros("bar_start").alias("bar_start_micros"),
        *(F.col(column) for column in MARKET_BAR_COLUMNS[2:]),
    )
    for row in selected.toLocalIterator():
        if not row.is_final:
            raise ValueError("PostgreSQL sink accepts final bars only")
        if row.bar_start_micros is None:
            raise ValueError("PostgreSQL sink requires bar_start")
        seconds, microseconds = divmod(row.bar_start_micros, 1_000_000)
        bar_start = datetime.fromtimestamp(seconds, UTC).replace(
            microsecond=microseconds
        )
        records.append(
            (
                validation_run_id,
                row.symbol,
                bar_start,
                row.timeframe,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.trade_count,
                row.vwap,
                row.source,
                row.feed,
                row.is_final,
                row.condition_policy,
                spark_batch_id,
            )
        )
    return records


def record_validation_bars(
    batch_df: DataFrame,
    spark_batch_id: int,
    *,
    validation_run_id: str,
    workload_id: str,
    processor_version: str,
    checkpoint_namespace: str,
    source_manifest_identity: str | None,
    database_url: str,
) -> int:
    """Atomically record one immutable validation run and its derived bars."""
    if not all((validation_run_id, workload_id, processor_version, checkpoint_namespace)):
        raise ValueError("validation run lineage fields must not be empty")
    rows = validation_bar_rows(
        batch_df,
        spark_batch_id,
        validation_run_id=validation_run_id,
    )

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(
                RECORD_VALIDATION_RUN_SQL,
                (
                    validation_run_id,
                    workload_id,
                    processor_version,
                    checkpoint_namespace,
                    source_manifest_identity,
                ),
            )
            if rows:
                cursor.executemany(RECORD_VALIDATION_BAR_SQL, rows)
    return len(rows)


def postgres_bar_sink(
    database_url: str,
    *,
    validation_run_id: str,
    workload_id: str,
    processor_version: str,
    checkpoint_namespace: str,
    source_manifest_identity: str | None = None,
) -> Callable[[DataFrame, int], int]:
    """Bind validation lineage and a secret-bearing DSN for Spark."""
    return partial(
        record_validation_bars,
        database_url=database_url,
        validation_run_id=validation_run_id,
        workload_id=workload_id,
        processor_version=processor_version,
        checkpoint_namespace=checkpoint_namespace,
        source_manifest_identity=source_manifest_identity,
    )
