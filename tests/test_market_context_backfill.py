import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.economic_event_schedule import load_event_catalog
from src.historical_bars import HistoricalBar
from src.market_context_backfill import (
    collect_market_context_work_item,
    select_market_context_work,
)


def bar(symbol: str, timestamp: datetime) -> HistoricalBar:
    return HistoricalBar(
        symbol=symbol,
        bar_start=timestamp,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=100,
        trade_count=10,
        vwap=Decimal("100.5"),
    )


class MarketContextBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.release = next(
            release
            for release in load_event_catalog()
            if release.event_type == "FOMC"
            and release.release_date.isoformat() == "2026-07-29"
        )

    def test_selection_builds_one_work_item_per_event_and_symbol(self) -> None:
        work = select_market_context_work(
            {
                "event_types": ["FOMC"],
                "release_from": "2026-07-29",
                "release_to": "2026-07-29",
                "symbols": ["SPY", "TLT"],
                "feed": "sip",
            }
        )

        self.assertEqual(
            [(item.event_type, item.symbol) for item in work],
            [("FOMC", "SPY"), ("FOMC", "TLT")],
        )
        self.assertEqual(work[0].event_id, self.release.event_id)

    def test_one_work_item_preserves_partial_daily_coverage(self) -> None:
        item = select_market_context_work(
            {
                "event_types": ["FOMC"],
                "release_from": "2026-07-29",
                "release_to": "2026-07-29",
                "symbols": ["TLT"],
                "feed": "sip",
            }
        )[0]
        session_start = self.release.released_at - timedelta(minutes=60)
        session_bars = [bar("TLT", session_start + timedelta(minutes=i)) for i in range(3)]
        daily_start = datetime(2026, 7, 29, 4, tzinfo=UTC)
        daily_bars = [
            bar("TLT", daily_start + timedelta(days=offset))
            for offset in [-9, -8, -7, -6, -5, -4, -3, 0, 1, 2]
        ]
        responses = iter([(session_bars, 1), (daily_bars, 1)])
        stored = []
        derived = []

        result = collect_market_context_work_item(
            item,
            client=object(),
            database_url="postgresql://test",
            provider_available_until=datetime(2026, 8, 2, tzinfo=UTC),
            fetcher=lambda *_args, **_kwargs: next(responses),
            historical_writer=lambda rows, **_kwargs: stored.extend(rows) or len(rows),
            derived_writer=lambda rows, **_kwargs: derived.extend(rows) or len(rows),
        )

        self.assertEqual(
            (result.daily_before, result.daily_event, result.daily_after),
            (7, 1, 2),
        )
        self.assertEqual(result.coverage_status, "FUTURE_SESSION_UNAVAILABLE")
        self.assertEqual(result.session_1m_rows, 3)
        self.assertEqual((result.derived_3m_rows, result.derived_5m_rows), (1, 1))
        self.assertEqual(result.daily_rows, 10)
        self.assertEqual(result.pages, 2)
        self.assertFalse(result.fallback_used)
        self.assertEqual(len(stored), 13)
        self.assertEqual(len(derived), 2)


if __name__ == "__main__":
    unittest.main()
