import json
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

from src.historical_bars import (
    AlpacaHistoricalBarsClient,
    HistoricalBarError,
    fetch_all_bars,
    normalize_bar,
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


def raw_bar(timestamp: str, close: float = 101.0) -> dict:
    return {
        "t": timestamp,
        "o": 100.0,
        "h": 102.0,
        "l": 99.0,
        "c": close,
        "v": 1200,
        "n": 24,
        "vw": 100.5,
    }


class HistoricalBarsTest(unittest.TestCase):
    def test_fetches_all_sip_pages_with_explicit_contract(self) -> None:
        opener = RecordingOpener(
            [
                {
                    "bars": {"SPY": [raw_bar("2026-08-12T12:29:00Z")]},
                    "next_page_token": "next",
                },
                {
                    "bars": {"QQQ": [raw_bar("2026-08-12T12:30:00Z")]},
                    "next_page_token": None,
                },
            ]
        )
        client = AlpacaHistoricalBarsClient("key", "secret", opener=opener)

        bars, pages = fetch_all_bars(
            client,
            symbols=["SPY", "QQQ"],
            start=datetime(2026, 8, 12, 12, 29, tzinfo=UTC),
            end=datetime(2026, 8, 12, 12, 31, tzinfo=UTC),
            feed="sip",
        )

        self.assertEqual(pages, 2)
        self.assertEqual([bar.symbol for bar in bars], ["SPY", "QQQ"])
        query = parse_qs(urlparse(opener.requests[0][0].full_url).query)
        self.assertEqual(query["symbols"], ["SPY,QQQ"])
        self.assertEqual(query["timeframe"], ["1Min"])
        self.assertEqual(query["feed"], ["sip"])
        self.assertEqual(query["adjustment"], ["raw"])
        self.assertNotIn("secret", opener.requests[0][0].full_url)

    def test_normalizes_provider_bar_without_float_arithmetic(self) -> None:
        bar = normalize_bar("SPY", raw_bar("2026-08-12T12:30:00Z"))

        self.assertEqual(bar.bar_start, datetime(2026, 8, 12, 12, 30, tzinfo=UTC))
        self.assertEqual(bar.close, Decimal("101.0"))
        self.assertEqual(bar.vwap, Decimal("100.5"))

    def test_rejects_inconsistent_ohlc(self) -> None:
        with self.assertRaisesRegex(HistoricalBarError, "high"):
            normalize_bar("SPY", raw_bar("2026-08-12T12:30:00Z", close=103.0))

    def test_rejects_unrequested_symbol(self) -> None:
        opener = RecordingOpener(
            [
                {
                    "bars": {"UNEXPECTED": [raw_bar("2026-08-12T12:30:00Z")]},
                    "next_page_token": None,
                }
            ]
        )
        client = AlpacaHistoricalBarsClient("key", "secret", opener=opener)

        with self.assertRaisesRegex(HistoricalBarError, "unrequested symbol"):
            fetch_all_bars(
                client,
                symbols=["SPY"],
                start=datetime(2026, 8, 12, 12, 29, tzinfo=UTC),
                end=datetime(2026, 8, 12, 12, 31, tzinfo=UTC),
                feed="sip",
            )


if __name__ == "__main__":
    unittest.main()
