import unittest
from datetime import datetime, timezone

from src.market_event import build_market_envelope


RAW_TRADE = {
    "T": "t",
    "S": "NVDA",
    "i": 23,
    "x": "V",
    "p": 221.69,
    "s": 5,
    "c": ["@", "I"],
    "z": "C",
    "t": "2026-08-19T13:30:00.102733966Z",
}
NOW = datetime(2026, 8, 19, 13, 30, 1, tzinfo=timezone.utc)


class MarketEventTest(unittest.TestCase):
    def test_builds_canonical_envelope_without_changing_payload(self) -> None:
        envelope = build_market_envelope(
            RAW_TRADE,
            feed="iex",
            ingested_at=NOW,
            trace_id="run-1",
        )

        self.assertEqual(envelope["payload"], RAW_TRADE)
        self.assertIsNot(envelope["payload"], RAW_TRADE)
        self.assertEqual(envelope["event_type"], "market.trade.raw")
        self.assertEqual(envelope["source_event_id"], "23")
        self.assertEqual(envelope["event_timestamp"], RAW_TRADE["t"])
        self.assertEqual(envelope["ingested_at"], "2026-08-19T13:30:01Z")
        self.assertEqual(envelope["trace_id"], "run-1")

    def test_event_id_changes_when_reused_provider_id_has_new_timestamp(self) -> None:
        first = dict(RAW_TRADE, i=1, t="2026-08-19T13:27:08Z")
        second = dict(RAW_TRADE, i=1, t="2026-08-19T13:27:13Z")

        first_id = build_market_envelope(first, "test", NOW)["event_id"]
        second_id = build_market_envelope(second, "test", NOW)["event_id"]

        self.assertNotEqual(first_id, second_id)

    def test_rejects_missing_routing_field(self) -> None:
        incomplete = {key: value for key, value in RAW_TRADE.items() if key != "S"}

        with self.assertRaisesRegex(ValueError, "Missing routing field: S"):
            build_market_envelope(incomplete, "iex", NOW)


if __name__ == "__main__":
    unittest.main()
