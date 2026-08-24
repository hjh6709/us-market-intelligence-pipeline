import io
import json
import unittest
from datetime import UTC, datetime
from urllib.error import HTTPError

from src.historical_market_replay import (
    AlpacaHistoricalClient,
    HistoricalTradeError,
    _replay_delay_seconds,
    fetch_all_trades,
    normalize_historical_trade,
    parse_args,
    publish_historical_trades,
)


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


class RecordingOpener:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.responses.pop(0))


class RecordingPublisher:
    def __init__(self) -> None:
        self.envelopes = []

    def publish(self, envelope) -> None:
        self.envelopes.append(envelope)


class HistoricalMarketReplayTest(unittest.TestCase):
    def test_fetches_all_pages_with_explicit_iex_window(self) -> None:
        opener = RecordingOpener(
            [
                {
                    "trades": [self.trade(1, "2026-08-19T19:50:01Z")],
                    "next_page_token": "next-token",
                },
                {
                    "trades": [self.trade(2, "2026-08-19T19:52:01Z")],
                    "next_page_token": None,
                },
            ]
        )
        client = AlpacaHistoricalClient("key-id", "secret", opener=opener)

        trades, pages = fetch_all_trades(
            client,
            symbol="SMH",
            start="2026-08-19T19:50:00Z",
            end="2026-08-19T19:53:00Z",
            feed="iex",
            limit=1000,
            max_pages=3,
        )

        self.assertEqual([trade["i"] for trade in trades], [1, 2])
        self.assertEqual(pages, 2)
        first_url = opener.requests[0][0].full_url
        second_url = opener.requests[1][0].full_url
        self.assertIn("feed=iex", first_url)
        self.assertIn("start=2026-08-19T19%3A50%3A00Z", first_url)
        self.assertNotIn("key-id", first_url)
        self.assertNotIn("secret", first_url)
        self.assertIn("page_token=next-token", second_url)

    def test_http_error_does_not_expose_credentials(self) -> None:
        def rejecting_opener(request, timeout):
            raise HTTPError(request.full_url, 401, "unauthorized", {}, io.BytesIO())

        client = AlpacaHistoricalClient(
            "private-key-id",
            "private-secret",
            opener=rejecting_opener,
        )

        with self.assertRaises(HistoricalTradeError) as raised:
            client.fetch_page(
                symbol="SMH",
                start="2026-08-19T19:50:00Z",
                end="2026-08-19T19:53:00Z",
                feed="iex",
                limit=1000,
            )

        message = str(raised.exception)
        self.assertNotIn("private-key-id", message)
        self.assertNotIn("private-secret", message)

    def test_normalizes_actual_trade_and_publishes_canonical_envelope(self) -> None:
        raw_trade = self.trade(7, "2026-08-19T19:51:23.123456Z")
        publisher = RecordingPublisher()

        published = publish_historical_trades(
            "SMH",
            [raw_trade],
            publisher,
            feed="iex",
            trace_id="historical-evidence",
            clock=lambda: datetime(2026, 8, 20, tzinfo=UTC),
        )

        self.assertEqual(published, 1)
        payload = publisher.envelopes[0]["payload"]
        self.assertEqual(
            normalize_historical_trade("SMH", raw_trade),
            {
                "T": "t",
                "S": "SMH",
                "i": 7,
                "x": "V",
                "p": 100.25,
                "s": 4,
                "c": ["@"],
                "t": "2026-08-19T19:51:23.123456Z",
                "z": "C",
            },
        )
        self.assertEqual(payload["S"], "SMH")
        self.assertEqual(payload["t"], "2026-08-19T19:51:23.123456Z")

    def test_calculates_replay_delay_from_event_time_and_speed(self) -> None:
        first = datetime(2026, 8, 19, 19, 50, tzinfo=UTC)
        event = datetime(2026, 8, 19, 19, 50, 20, tzinfo=UTC)

        self.assertEqual(
            _replay_delay_seconds(first, event, speed_multiplier=10, elapsed=1.25),
            0.75,
        )
        self.assertEqual(
            _replay_delay_seconds(first, event, speed_multiplier=10, elapsed=3),
            0,
        )

    def test_cli_keeps_unthrottled_default_and_accepts_load_speed(self) -> None:
        base = [
            "--start",
            "2026-08-19T19:50:00Z",
            "--end",
            "2026-08-19T19:56:00Z",
        ]

        self.assertIsNone(parse_args(base).speed_multiplier)
        self.assertEqual(
            parse_args([*base, "--speed-multiplier", "50"]).speed_multiplier,
            50,
        )

    @staticmethod
    def trade(trade_id: int, timestamp: str) -> dict:
        return {
            "t": timestamp,
            "x": "V",
            "p": 100.25,
            "s": 4,
            "c": ["@"],
            "i": trade_id,
            "z": "C",
        }


if __name__ == "__main__":
    unittest.main()
