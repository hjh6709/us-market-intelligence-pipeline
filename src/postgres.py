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

UPSERT_MARKET_BAR_SQL = """
INSERT INTO market_bars (
    symbol, bar_start, timeframe, open, high, low, close,
    volume, trade_count, vwap, source, feed, is_final,
    condition_policy, spark_batch_id
) VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s
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
    is_final = EXCLUDED.is_final,
    condition_policy = EXCLUDED.condition_policy,
    spark_batch_id = EXCLUDED.spark_batch_id,
    updated_at = CURRENT_TIMESTAMP
"""


def market_bar_rows(batch_df: DataFrame, spark_batch_id: int) -> list[tuple]:
    """Convert one small finalized-bar micro-batch into DB parameter rows."""
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


def upsert_market_bars(
    batch_df: DataFrame,
    spark_batch_id: int,
    *,
    database_url: str,
) -> int:
    """Atomically upsert one Spark micro-batch and return its row count."""
    rows = market_bar_rows(batch_df, spark_batch_id)
    if not rows:
        return 0

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.executemany(UPSERT_MARKET_BAR_SQL, rows)
    return len(rows)


def postgres_bar_sink(database_url: str) -> Callable[[DataFrame, int], int]:
    """Bind a secret-bearing DSN once without logging it from the runner."""
    return partial(upsert_market_bars, database_url=database_url)
