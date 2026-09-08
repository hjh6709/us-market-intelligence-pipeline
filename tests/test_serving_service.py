import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from src.serving_repository import (
    CrossAssetImpactRecord,
    EventRecord,
    ImpactRecord,
    HistoricalImpactRecord,
    MacroContextRecord,
    StrategyRecord,
    StrategySummaryRecord,
    OverviewMetricsRecord,
)
from src.serving_service import ServingNotFoundError, ServingService


class FakeRepository:
    event = EventRecord(
        event_id="event-1",
        event_type="CPI",
        reference_period="2026-07",
        released_at=datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc),
        source="BLS",
        quality_status="READY",
        forecast=None,
        actual=None,
        surprise=None,
    )
    strategy = StrategyRecord(
        signal=1,
        entry_price=Decimal("123.45"),
        exit_price=Decimal("122.80"),
        gross_return_pct=Decimal("-0.526529"),
        transaction_cost_bps=Decimal("10"),
        net_return_pct=Decimal("-0.626529"),
        coverage_status="COMPLETE",
    )

    def get_event(self, event_id):
        return self.event if event_id == "event-1" else None

    def list_symbols(self, event_id):
        return ["NVDA"] if event_id == "event-1" else []

    def get_impacts(self, event_id, symbol):
        if event_id != "event-1" or symbol != "NVDA":
            return []
        return [
            ImpactRecord(
                window_name="POST_60M",
                return_pct=Decimal("-0.52"),
                market_return_pct=Decimal("-0.10"),
                excess_return_pct=Decimal("-0.42"),
                volume=1000,
                realized_volatility=Decimal("0.03"),
                coverage_status="COMPLETE",
            )
        ]

    def get_macro_context(self, event_id):
        return [
            MacroContextRecord(
                series_id="CPIAUCSL",
                series_name="Consumer Price Index",
                observation_date=date(2026, 7, 1),
                value=Decimal("325.1"),
                vintage_date=date(2026, 8, 12),
            )
        ]

    def get_strategy_result(self, event_id, symbol):
        if event_id == "event-1" and symbol == "NVDA":
            return self.strategy
        return None

    def get_strategy_summary(self):
        return StrategySummaryRecord(2020, 1988, Decimal("-0.1565"), 782)

    def get_historical_impacts(self, event_type, symbol, window_name):
        return [
            HistoricalImpactRecord(
                "event-1", self.event.released_at, Decimal("0.5"), Decimal("0.2"), "COMPLETE"
            )
        ]

    def get_cross_asset_impacts(self, event_id, window_name):
        return [CrossAssetImpactRecord("NVDA", Decimal("0.5"), Decimal("0.2"), "COMPLETE")]

    def get_overview_metrics(self):
        return OverviewMetricsRecord(202, 10, 2020, 308512, 112593, 70090, 2020, 8080, 2020, 1988)

    def get_event_type_counts(self):
        return {"CPI": 55, "EMPLOYMENT": 55, "PCE": 55, "FOMC": 37}

    def list_supported_symbols(self):
        return ["AAPL", "NVDA", "SPY"]

    def list_events(self, event_type=None, released_from=None, released_to=None):
        return [self.event]


class ServingServiceTest(unittest.TestCase):
    def test_detail_keeps_signal_simulation_and_order_action_separate(self):
        detail = ServingService(FakeRepository()).get_event_symbol_detail(
            "event-1", "NVDA"
        )

        self.assertEqual(detail.research_signal, "LONG")
        self.assertEqual(detail.simulation.net_return_pct, Decimal("-0.626529"))
        self.assertEqual(detail.execution_readiness.order_action, "NO_TRADE")
        self.assertEqual(detail.execution_readiness.stage, "RESEARCH_ONLY")
        self.assertEqual(detail.provenance.source, "alpaca")
        self.assertEqual(detail.provenance.feed, "sip")
        self.assertEqual(detail.provenance.analysis_version, "multi_event_sip_v1")
        self.assertEqual(detail.provenance.strategy_version, "v1")

        summary = ServingService(FakeRepository()).get_strategy_summary()
        self.assertEqual(summary.provenance.analysis_version, "multi_event_sip_v1")
        self.assertEqual(summary.provenance.strategy_name, "pre60_momentum_post60")

    def test_missing_event_and_symbol_are_distinct_not_found_errors(self):
        service = ServingService(FakeRepository())

        with self.assertRaisesRegex(ServingNotFoundError, "event"):
            service.get_event_symbol_detail("missing", "NVDA")
        with self.assertRaisesRegex(ServingNotFoundError, "symbol"):
            service.get_event_symbol_detail("event-1", "AAPL")

    def test_comparison_views_are_historical_and_versioned(self):
        service = ServingService(FakeRepository())

        historical = service.get_historical_comparison("CPI", "NVDA", "POST_60M")
        cross_asset = service.get_cross_asset_comparison("event-1", "POST_60M")

        self.assertEqual(historical.provenance.analysis_version, "multi_event_sip_v1")
        self.assertEqual(historical.points[0].return_pct, Decimal("0.5"))
        self.assertEqual(cross_asset.points[0].symbol, "NVDA")
        self.assertEqual(cross_asset.window_name, "POST_60M")

    def test_overview_uses_repository_symbols_and_explicit_metric_units(self):
        overview = ServingService(FakeRepository()).get_overview(
            {"status": "SUCCEEDED", "warning_check_count": 1409}
        )

        self.assertEqual(overview.supported_symbols, ["AAPL", "NVDA", "SPY"])
        self.assertEqual(overview.event_type_counts["FOMC"], 37)
        self.assertEqual(overview.metrics[0].unit, "releases")
        self.assertEqual(overview.baseline.mean_net_return_pct, Decimal("-0.1565"))


if __name__ == "__main__":
    unittest.main()
