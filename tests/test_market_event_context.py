import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.economic_event_schedule import load_event_catalog
from src.historical_bars import HistoricalBar
from src.market_event_context import (
    build_context_requests,
    select_daily_context,
    select_session_context,
)


def daily_bar(symbol: str, timestamp: datetime) -> HistoricalBar:
    return HistoricalBar(
        symbol=symbol,
        bar_start=timestamp,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=1000,
        trade_count=100,
        vwap=Decimal("100.5"),
    )


class MarketEventContextTest(unittest.TestCase):
    def setUp(self) -> None:
        self.release = next(
            item
            for item in load_event_catalog()
            if item.event_type == "FOMC" and item.release_date.isoformat() == "2026-07-29"
        )

    def test_builds_181_minute_and_daily_context_requests(self) -> None:
        requests = build_context_requests([self.release], ["SPY", "TLT"])

        self.assertEqual(len(requests), 2)
        session, daily = requests
        self.assertEqual(session.layer, "SESSION_1MIN")
        self.assertEqual(session.timeframe, "1Min")
        self.assertEqual(session.start, datetime(2026, 7, 29, 17, 0, tzinfo=UTC))
        self.assertEqual(session.end, datetime(2026, 7, 29, 20, 1, tzinfo=UTC))
        self.assertEqual(int((session.end - session.start).total_seconds() / 60), 181)
        self.assertEqual(daily.layer, "DAILY_15_SESSIONS")
        self.assertEqual(daily.timeframe, "1Day")

    def test_selects_observed_trading_sessions_instead_of_calendar_days(self) -> None:
        event_start = datetime(2026, 7, 29, 4, tzinfo=UTC)
        offsets = [-11, -10, -9, -8, -7, -6, -5, 0, 1, 2, 5, 6, 7, 8, 9]
        bars = [daily_bar("TLT", event_start + timedelta(days=offset)) for offset in offsets]

        selection = select_daily_context(bars, self.release, "TLT")

        self.assertTrue(selection.complete)
        self.assertEqual(len(selection.bars), 15)
        self.assertEqual(selection.sessions_before, 7)
        self.assertEqual(selection.event_session, 1)
        self.assertEqual(selection.sessions_after, 7)

    def test_session_selection_excludes_provider_end_boundary(self) -> None:
        request = build_context_requests([self.release], ["TLT"])[0]
        bars = [
            daily_bar("TLT", request.start),
            daily_bar("TLT", request.end - timedelta(minutes=1)),
            daily_bar("TLT", request.end),
        ]

        selected = select_session_context(bars, request)

        self.assertEqual([bar.bar_start for bar in selected], [request.start, request.end - timedelta(minutes=1)])

    def test_marks_daily_context_incomplete_when_future_sessions_are_unavailable(self) -> None:
        event_start = datetime(2026, 7, 29, 4, tzinfo=UTC)
        bars = [
            daily_bar("TLT", event_start + timedelta(days=offset))
            for offset in [-9, -8, -7, -6, -5, -4, -3, 0, 1, 2]
        ]

        selection = select_daily_context(bars, self.release, "TLT")

        self.assertFalse(selection.complete)
        self.assertEqual(selection.sessions_before, 7)
        self.assertEqual(selection.event_session, 1)
        self.assertEqual(selection.sessions_after, 2)


if __name__ == "__main__":
    unittest.main()
