import unittest
from datetime import datetime, timezone

from src.market_producer import process_messages


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


class RecordingPublisher:
    def __init__(self) -> None:
        self.envelopes = []

    def publish(self, envelope) -> None:
        self.envelopes.append(envelope)


class MarketProducerTest(unittest.TestCase):
    def test_publishes_only_trade_messages(self) -> None:
        publisher = RecordingPublisher()

        count = process_messages(
            [{"T": "success", "msg": "authenticated"}, RAW_TRADE],
            publisher=publisher,
            feed="iex",
            trace_id="collector-1",
            clock=lambda: NOW,
        )

        self.assertEqual(count, 1)
        self.assertEqual(publisher.envelopes[0]["payload"], RAW_TRADE)
        self.assertEqual(publisher.envelopes[0]["trace_id"], "collector-1")

    def test_rejects_incomplete_trade_before_kafka(self) -> None:
        incomplete = {"T": "t", "S": "NVDA", "i": 1}
        publisher = RecordingPublisher()

        with self.assertRaisesRegex(ValueError, "Missing routing field: t"):
            process_messages(
                [incomplete],
                publisher=publisher,
                feed="iex",
                trace_id="collector-1",
                clock=lambda: NOW,
            )

        self.assertEqual(publisher.envelopes, [])

    def test_ignores_non_trade_data_messages(self) -> None:
        publisher = RecordingPublisher()

        count = process_messages(
            [{"T": "q", "S": "NVDA"}, {"T": "subscription", "trades": ["NVDA"]}],
            publisher=publisher,
            feed="iex",
            trace_id="collector-1",
            clock=lambda: NOW,
        )

        self.assertEqual(count, 0)
        self.assertEqual(publisher.envelopes, [])


if __name__ == "__main__":
    unittest.main()
