import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.event_strategy_backtest import calculate_strategy_result


class EventStrategyBacktestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.entry_at = datetime(2026, 8, 12, 12, 30, tzinfo=UTC)

    def test_long_signal_uses_only_pre_release_return_and_subtracts_cost(self) -> None:
        result = calculate_strategy_result(
            economic_event_id="event",
            symbol="NVDA",
            entry_at=self.entry_at,
            exit_at=self.entry_at + timedelta(minutes=60),
            entry_price=Decimal("100"),
            exit_price=Decimal("101"),
            pre_return_pct=Decimal("0.5"),
            post_return_pct=Decimal("1.0"),
            benchmark_return_pct=Decimal("0.2"),
            pre_coverage="COMPLETE",
            post_coverage="COMPLETE",
        )

        self.assertEqual(result.signal, 1)
        self.assertEqual(result.gross_return_pct, Decimal("1.0"))
        self.assertEqual(result.net_return_pct, Decimal("0.9"))
        self.assertEqual(result.coverage_status, "COMPLETE")

    def test_short_signal_reverses_post_release_return(self) -> None:
        result = calculate_strategy_result(
            economic_event_id="event",
            symbol="SPY",
            entry_at=self.entry_at,
            exit_at=self.entry_at + timedelta(minutes=60),
            entry_price=Decimal("100"),
            exit_price=Decimal("98"),
            pre_return_pct=Decimal("-0.5"),
            post_return_pct=Decimal("-2.0"),
            benchmark_return_pct=Decimal("-2.0"),
            pre_coverage="PARTIAL_MARKET_COVERAGE",
            post_coverage="COMPLETE",
        )

        self.assertEqual(result.signal, -1)
        self.assertEqual(result.net_return_pct, Decimal("1.9"))
        self.assertEqual(result.coverage_status, "PARTIAL_MARKET_COVERAGE")

    def test_missing_baseline_is_not_traded(self) -> None:
        result = calculate_strategy_result(
            economic_event_id="event",
            symbol="SMH",
            entry_at=self.entry_at,
            exit_at=self.entry_at + timedelta(minutes=60),
            entry_price=None,
            exit_price=Decimal("101"),
            pre_return_pct=None,
            post_return_pct=None,
            benchmark_return_pct=None,
            pre_coverage="NO_MARKET_DATA",
            post_coverage="COMPLETE",
        )

        self.assertEqual(result.signal, 0)
        self.assertIsNone(result.net_return_pct)
        self.assertEqual(result.coverage_status, "NOT_ELIGIBLE")


if __name__ == "__main__":
    unittest.main()
