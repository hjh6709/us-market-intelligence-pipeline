"""Write one deterministic final bar for PostgreSQL failure/recovery evidence."""

from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal

import psycopg
from pyspark.sql import functions as F

from src.postgres import upsert_market_bars
from src.spark_session import create_local_spark


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://market:market@localhost:55432/market",
)


def main() -> int:
    spark = create_local_spark("postgres-sink-recovery-probe")
    try:
        batch = spark.createDataFrame(
            [
                (
                    "NVDA",
                    datetime(2026, 8, 19, 13, 30),
                    "1m",
                    Decimal("100.000000"),
                    Decimal("105.000000"),
                    Decimal("99.000000"),
                    Decimal("102.000000"),
                    11,
                    4,
                    Decimal("101.727273"),
                    "alpaca",
                    "iex",
                    True,
                    "all_valid_trades_v1",
                )
            ],
            """
            symbol string, bar_start timestamp, timeframe string,
            open decimal(18,6), high decimal(18,6), low decimal(18,6),
            close decimal(18,6), volume long, trade_count long,
            vwap decimal(18,6), source string, feed string,
            is_final boolean, condition_policy string
            """,
        ).withColumn(
            "bar_start",
            F.to_timestamp(F.lit("2026-08-19T13:30:00Z")),
        )
        try:
            written = upsert_market_bars(
                batch,
                9001,
                database_url=DATABASE_URL,
            )
        except psycopg.OperationalError as error:
            print(
                json.dumps(
                    {
                        "status": "database_unavailable",
                        "error_type": type(error).__name__,
                        "spark_batch_id": 9001,
                    }
                )
            )
            return 2

        print(
            json.dumps(
                {
                    "status": "upsert_succeeded",
                    "rows_written": written,
                    "spark_batch_id": 9001,
                    "business_key": [
                        "NVDA",
                        "2026-08-19T13:30:00Z",
                        "1m",
                        "alpaca",
                        "iex",
                    ],
                }
            )
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
