import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.macro_event_impact import (
    BarPoint,
    calculate_event_impacts,
    calculate_and_store,
    calculate_metric,
)


class MacroEventImpactTest(unittest.TestCase):
    def setUp(self) -> None:
        self.released_at = datetime(2026, 8, 12, 12, 30, tzinfo=UTC)

    def test_post_return_uses_last_pre_release_close_as_baseline(self) -> None:
        bars = [
            BarPoint(
                self.released_at - timedelta(minutes=1),
                Decimal("99"),
                Decimal("100"),
                10,
            ),
            BarPoint(
                self.released_at,
                Decimal("100"),
                Decimal("101"),
                20,
            ),
            BarPoint(
                self.released_at + timedelta(minutes=1),
                Decimal("101"),
                Decimal("102"),
                30,
            ),
        ]
        metrics = calculate_event_impacts(
            "CPI|2026-07|2026-08-12T12:30:00Z",
            self.released_at,
            {symbol: bars for symbol in ("SPY", "QQQ", "SMH", "NVDA")},
        )
        qqq_post_5m = next(
            metric
            for metric in metrics
            if metric.symbol == "QQQ" and metric.window_name == "POST_5M"
        )

        self.assertEqual(qqq_post_5m.open_price, Decimal("100"))
        self.assertEqual(qqq_post_5m.close_price, Decimal("102"))
        self.assertEqual(qqq_post_5m.return_pct, Decimal("2.00"))
        self.assertEqual(qqq_post_5m.volume, 50)
        self.assertEqual(qqq_post_5m.benchmark_return_pct, Decimal("2.00"))
        self.assertEqual(qqq_post_5m.market_relative_return_pct, Decimal("0.00"))

    def test_marks_sparse_window_partial_without_filling_missing_minutes(self) -> None:
        metric = calculate_metric(
            economic_event_id="event",
            symbol="SMH",
            window_name="POST_5M",
            window_start=self.released_at,
            window_end=self.released_at + timedelta(minutes=5),
            bars=[
                BarPoint(
                    self.released_at + timedelta(minutes=4),
                    Decimal("100"),
                    Decimal("101"),
                    10,
                )
            ],
            opening_price=Decimal("100"),
        )

        self.assertEqual(metric.coverage_status, "PARTIAL_MARKET_COVERAGE")
        self.assertEqual(metric.coverage_reason, "bars=1/5;endpoint_lag_minutes=0")
        self.assertEqual(metric.volume, 10)

    def test_missing_pre_release_baseline_does_not_invent_return(self) -> None:
        post_bar = BarPoint(
            self.released_at,
            Decimal("100"),
            Decimal("101"),
            10,
        )

        metrics = calculate_event_impacts(
            "event",
            self.released_at,
            {symbol: [post_bar] for symbol in ("SPY", "QQQ", "SMH", "NVDA")},
        )
        post_metric = next(
            metric
            for metric in metrics
            if metric.symbol == "QQQ" and metric.window_name == "POST_5M"
        )

        self.assertIsNone(post_metric.return_pct)
        self.assertIsNone(post_metric.open_price)
        self.assertEqual(post_metric.coverage_status, "MISSING_PRE_RELEASE_BASELINE")

    @patch("src.macro_event_impact.psycopg.connect")
    def test_analysis_queries_are_filtered_to_selected_event_and_symbol(
        self, connect: MagicMock
    ) -> None:
        connection = connect.return_value.__enter__.return_value
        connection.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("event", self.released_at)])),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]
        connection.cursor.return_value.__enter__.return_value = MagicMock()

        event_count, requested_impact_count = calculate_and_store(
            "postgresql://unused",
            event_ids=["event"],
            symbols=["NVDA"],
        )

        event_sql, event_params = connection.execute.call_args_list[0].args
        bars_sql, bars_params = connection.execute.call_args_list[1].args
        self.assertIn("economic_event_id = ANY", event_sql)
        self.assertEqual(event_params[-1], ["event"])
        self.assertIn("symbol = ANY", bars_sql)
        self.assertEqual(set(bars_params[0]), {"SPY", "NVDA"})
        self.assertEqual(event_count, 1)
        self.assertEqual(requested_impact_count, 4)


if __name__ == "__main__":
    unittest.main()
