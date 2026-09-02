import unittest
from collections import Counter
from datetime import UTC, datetime

from src.economic_event_schedule import load_event_catalog
from src.market_event_archive_plan import build_archive_plan
from src.market_universe import load_market_universe


class EconomicEventScheduleTest(unittest.TestCase):
    def test_catalog_contains_only_confirmed_expected_events(self) -> None:
        releases = load_event_catalog()

        self.assertEqual(
            Counter(item.event_type for item in releases),
            {"CPI": 55, "EMPLOYMENT": 8, "PCE": 9, "FOMC": 5},
        )
        self.assertEqual(len({item.event_id for item in releases}), 77)
        self.assertTrue(all(item.released_at.tzinfo is UTC for item in releases))

    def test_universe_expands_roles_without_duplicate_symbols(self) -> None:
        instruments = load_market_universe()

        self.assertEqual(len(instruments), 10)
        self.assertEqual(len({item.symbol for item in instruments}), 10)
        self.assertEqual(
            {item.symbol for item in instruments},
            {"SPY", "QQQ", "IWM", "TLT", "XLF", "SMH", "GLD", "NVDA", "AAPL", "JPM"},
        )

    def test_full_plan_is_event_by_symbol_and_keeps_121_minute_buckets(self) -> None:
        releases = load_event_catalog()
        symbols = [item.symbol for item in load_market_universe()]

        plan = build_archive_plan(releases, symbols)

        self.assertEqual(len(plan), 770)
        fomc = next(
            item
            for item in plan
            if item.event_type == "FOMC"
            and item.release_date == "2026-07-29"
            and item.symbol == "TLT"
        )
        self.assertEqual(fomc.start, "2026-07-29T17:00:00Z")
        self.assertEqual(fomc.end, "2026-07-29T19:01:00Z")


if __name__ == "__main__":
    unittest.main()
