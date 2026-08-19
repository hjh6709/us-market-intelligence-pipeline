import os
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from pyspark.sql import functions as F

from src.postgres import upsert_market_bars
from src.spark_session import create_local_spark


RUN_POSTGRES_INTEGRATION = os.environ.get("RUN_POSTGRES_INTEGRATION") == "1"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://market:market@localhost:55432/market",
)


@unittest.skipUnless(
    RUN_POSTGRES_INTEGRATION,
    "set RUN_POSTGRES_INTEGRATION=1 to test a local PostgreSQL service",
)
class PostgresMarketBarsIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_local_spark("postgres-market-bars-integration")
        migration = Path("db/migrations/001_market_bars.sql").read_text()
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(migration)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute("TRUNCATE market_bars")

    def bars_frame(self, specs: list[tuple[str, Decimal]]):
        rows = []
        for symbol, close in specs:
            rows.append(
                (
                    symbol,
                    datetime(2026, 8, 19, 13, 30),
                    "1m",
                    Decimal("100.000000"),
                    Decimal("105.000000"),
                    Decimal("99.000000"),
                    close,
                    11,
                    4,
                    Decimal("101.727273"),
                    "alpaca",
                    "iex",
                    True,
                    "all_valid_trades_v1",
                )
            )
        return self.spark.createDataFrame(
            rows,
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

    def read_bars(self) -> list[tuple]:
        with psycopg.connect(DATABASE_URL) as connection:
            return connection.execute(
                """
                SELECT symbol, close, spark_batch_id
                FROM market_bars
                ORDER BY symbol
                """
            ).fetchall()

    def test_replay_keeps_row_count_and_later_batch_updates_value(self) -> None:
        original = self.bars_frame(
            [("NVDA", Decimal("102.000000")), ("SPY", Decimal("101.500000"))]
        )

        self.assertEqual(
            upsert_market_bars(original, 10, database_url=DATABASE_URL),
            2,
        )
        self.assertEqual(
            upsert_market_bars(original, 11, database_url=DATABASE_URL),
            2,
        )
        self.assertEqual(len(self.read_bars()), 2)

        corrected = self.bars_frame(
            [("NVDA", Decimal("103.000000")), ("SPY", Decimal("101.500000"))]
        )
        upsert_market_bars(corrected, 12, database_url=DATABASE_URL)

        self.assertEqual(
            self.read_bars(),
            [
                ("NVDA", Decimal("103.000000"), 12),
                ("SPY", Decimal("101.500000"), 12),
            ],
        )

    def test_constraint_failure_rolls_back_entire_micro_batch(self) -> None:
        invalid_batch = self.bars_frame(
            [("NVDA", Decimal("102.000000")), ("SPY", Decimal("106.000000"))]
        )

        with self.assertRaises(psycopg.errors.CheckViolation):
            upsert_market_bars(invalid_batch, 20, database_url=DATABASE_URL)

        self.assertEqual(self.read_bars(), [])

    def test_connection_failure_can_retry_same_batch_after_recovery(self) -> None:
        batch = self.bars_frame([("NVDA", Decimal("102.000000"))])

        with self.assertRaises(psycopg.OperationalError):
            upsert_market_bars(
                batch,
                30,
                database_url="postgresql://market:market@127.0.0.1:1/market",
            )

        self.assertEqual(
            upsert_market_bars(batch, 30, database_url=DATABASE_URL),
            1,
        )
        self.assertEqual(self.read_bars()[0][0], "NVDA")


if __name__ == "__main__":
    unittest.main()
