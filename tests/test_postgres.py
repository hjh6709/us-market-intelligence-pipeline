import unittest
from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from src.postgres import market_bar_rows
from src.spark_session import create_local_spark


class PostgresMarketBarTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_local_spark("postgres-market-bar-unit-test")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_serializes_final_bar_with_spark_batch_id(self) -> None:
        bar_start = datetime(2026, 8, 19, 13, 30)
        frame = self.spark.createDataFrame(
            [
                (
                    "NVDA",
                    bar_start,
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

        rows = market_bar_rows(frame, spark_batch_id=42)

        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0],
            (
                "NVDA",
                bar_start.replace(tzinfo=UTC),
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
                42,
            ),
        )

    def test_rejects_non_final_bar_before_database_write(self) -> None:
        frame = self.spark.createDataFrame(
            [
                (
                    "NVDA",
                    datetime(2026, 8, 19, 13, 30),
                    "1m",
                    Decimal("100"),
                    Decimal("100"),
                    Decimal("100"),
                    Decimal("100"),
                    1,
                    1,
                    Decimal("100"),
                    "alpaca",
                    "iex",
                    False,
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
        )

        with self.assertRaisesRegex(ValueError, "final bars"):
            market_bar_rows(frame, spark_batch_id=7)

    def test_preserves_utc_instant_across_spark_python_boundary(self) -> None:
        frame = self.spark.range(1).select(
            F.lit("NVDA").alias("symbol"),
            F.to_timestamp(F.lit("2026-08-19T13:30:00Z")).alias("bar_start"),
            F.lit("1m").alias("timeframe"),
            F.lit(100).cast("decimal(18,6)").alias("open"),
            F.lit(105).cast("decimal(18,6)").alias("high"),
            F.lit(99).cast("decimal(18,6)").alias("low"),
            F.lit(102).cast("decimal(18,6)").alias("close"),
            F.lit(11).cast("long").alias("volume"),
            F.lit(4).cast("long").alias("trade_count"),
            F.lit(101.727273).cast("decimal(18,6)").alias("vwap"),
            F.lit("alpaca").alias("source"),
            F.lit("iex").alias("feed"),
            F.lit(True).alias("is_final"),
            F.lit("all_valid_trades_v1").alias("condition_policy"),
        )

        rows = market_bar_rows(frame, spark_batch_id=8)

        self.assertEqual(
            rows[0][1],
            datetime(2026, 8, 19, 13, 30, tzinfo=UTC),
        )


if __name__ == "__main__":
    unittest.main()
