import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from src.derived_bars import aggregate_derived_bars
from src.historical_bars import HistoricalBar


def minute_bar(minute: int, *, volume: int = 10) -> HistoricalBar:
    price = Decimal(100 + minute)
    return HistoricalBar(
        symbol="TLT",
        bar_start=datetime(2026, 7, 29, 18, minute, tzinfo=UTC),
        open=price,
        high=price + 2,
        low=price - 1,
        close=price + 1,
        volume=volume,
        trade_count=minute + 1,
        vwap=price + Decimal("0.5"),
    )


class DerivedBarsTest(unittest.TestCase):
    def test_aggregates_complete_three_minute_ohlcv_and_vwap(self) -> None:
        bars = [
            minute_bar(0, volume=10),
            minute_bar(1, volume=20),
            minute_bar(2, volume=30),
        ]

        result = aggregate_derived_bars(bars, 3)[0]

        self.assertEqual(result.timeframe, "3m")
        self.assertEqual(result.open, Decimal("100"))
        self.assertEqual(result.high, Decimal("104"))
        self.assertEqual(result.low, Decimal("99"))
        self.assertEqual(result.close, Decimal("103"))
        self.assertEqual(result.volume, 60)
        self.assertEqual(result.trade_count, 6)
        self.assertEqual(result.vwap, Decimal("101.8333333333333333333333333"))
        self.assertEqual(result.coverage_status, "COMPLETE")

    def test_keeps_partial_five_minute_bucket_without_filling_missing_minutes(self) -> None:
        bars = [minute_bar(0), minute_bar(2), minute_bar(4)]

        result = aggregate_derived_bars(bars, 5)[0]

        self.assertEqual(result.source_bar_count, 3)
        self.assertEqual(result.expected_bar_count, 5)
        self.assertEqual(result.coverage_status, "PARTIAL")
        self.assertEqual(result.volume, 30)

    def test_uses_wall_clock_bucket_boundaries(self) -> None:
        bars = [minute_bar(4), minute_bar(5), minute_bar(6)]

        result = aggregate_derived_bars(bars, 5)

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0].bar_start,
            datetime(2026, 7, 29, 18, 0, tzinfo=UTC),
        )
        self.assertEqual(
            result[1].bar_start,
            datetime(2026, 7, 29, 18, 5, tzinfo=UTC),
        )

    def test_rejects_duplicate_source_minute(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate minute bar"):
            aggregate_derived_bars([minute_bar(0), minute_bar(0)], 3)

    def test_migration_records_derived_bar_coverage(self) -> None:
        migration = Path("db/migrations/005_derived_bar_coverage.sql").read_text()

        self.assertIn("source_bar_count SMALLINT", migration)
        self.assertIn("expected_bar_count SMALLINT", migration)
        self.assertIn("coverage_status TEXT", migration)


if __name__ == "__main__":
    unittest.main()
