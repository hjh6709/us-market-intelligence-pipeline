import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient

from src.serving_api import create_app
from src.serving_models import (
    BarView,
    EventSummary,
    EventSymbolDetail,
    ExecutionReadinessView,
    ImpactView,
    ReadinessCheckView,
    SimulationView,
    StrategySummaryView,
)
from src.serving_service import ServingNotFoundError


class FakeService:
    def health(self):
        return True

    def list_events(self, event_type=None, released_from=None, released_to=None):
        return [
            EventSummary(
                event_id="event-1",
                event_type="CPI",
                reference_period="2026-07",
                released_at=datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc),
                source="BLS",
                quality_status="READY",
            )
        ]

    def list_symbols(self, event_id):
        if event_id != "event-1":
            raise ServingNotFoundError("event")
        return ["NVDA"]

    def get_event_symbol_detail(self, event_id, symbol):
        if event_id != "event-1":
            raise ServingNotFoundError("event")
        if symbol != "NVDA":
            raise ServingNotFoundError("symbol")
        return EventSymbolDetail(
            event=self.list_events()[0],
            symbol="NVDA",
            impacts=[ImpactView(window_name="POST_60M", coverage_status="COMPLETE")],
            macro_context=[],
            research_signal="LONG",
            simulation=SimulationView(
                transaction_cost_bps=Decimal("10"),
                net_return_pct=Decimal("-0.626529"),
                coverage_status="COMPLETE",
            ),
            execution_readiness=ExecutionReadinessView(
                stage="RESEARCH_ONLY",
                order_action="NO_TRADE",
                eligible_for_order=False,
                requires_human_approval=False,
                checks=[ReadinessCheckView(name="kill_switch", status="FAIL")],
                reasons=["release-level execution lock keeps this service research-only"],
            ),
        )

    def get_bars(self, event_id, symbol, timeframe):
        return [
            BarView(
                symbol=symbol,
                timeframe=timeframe,
                window_start=datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc),
                open=Decimal("180"),
                high=Decimal("181"),
                low=Decimal("179"),
                close=Decimal("180.5"),
                volume=Decimal("1000"),
                coverage_status="COMPLETE",
            )
        ]

    def get_strategy_summary(self):
        return StrategySummaryView(
            strategy_name="pre60_momentum_post60",
            strategy_version="v1",
            total_count=2020,
            eligible_count=1988,
            mean_net_return_pct=Decimal("-0.1565"),
            positive_count=782,
            positive_rate_pct=Decimal("39.34"),
        )


class ServingApiTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(create_app(FakeService()))

    def test_detail_endpoint_returns_research_and_execution_sections(self):
        response = self.client.get("/api/v1/events/event-1/symbols/NVDA")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["research_signal"], "LONG")
        self.assertEqual(
            response.json()["execution_readiness"]["order_action"], "NO_TRADE"
        )

    def test_invalid_timeframe_and_symbol_are_422(self):
        self.assertEqual(
            self.client.get(
                "/api/v1/events/event-1/symbols/NVDA/bars?timeframe=15m"
            ).status_code,
            422,
        )
        self.assertEqual(
            self.client.get("/api/v1/events/event-1/symbols/nvda").status_code,
            422,
        )

    def test_event_filters_and_health_are_available(self):
        response = self.client.get(
            "/api/v1/events",
            params={
                "event_type": "CPI",
                "released_from": date(2026, 1, 1).isoformat(),
                "released_to": date(2026, 8, 31).isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()[0]["event_id"], "event-1")
        self.assertEqual(self.client.get("/health").json()["database"], "ok")

    def test_not_found_response_does_not_expose_internal_details(self):
        response = self.client.get("/api/v1/events/missing/symbols")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "event not found"})

    def test_dashboard_contains_filters_chart_and_readiness_sections(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="event-select"', response.text)
        self.assertIn('id="price-chart"', response.text)
        self.assertIn('id="readiness-checks"', response.text)
        self.assertIn("과거 분석 결과이며 주문이 아닙니다", response.text)
        self.assertNotIn("cdn.", response.text.lower())


if __name__ == "__main__":
    unittest.main()
