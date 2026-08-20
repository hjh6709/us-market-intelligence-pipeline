import json
import unittest
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

from src.fred import (
    FredClient,
    FredContractError,
    FredRateLimitError,
    FredTimeoutError,
    FredTransportError,
    FredWindow,
)


class FakeResponse:
    def __init__(self, payload: dict | bytes) -> None:
        self.payload = (
            json.dumps(payload).encode("utf-8")
            if isinstance(payload, dict)
            else payload
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


class RecordingOpener:
    def __init__(self, responses: list[dict | bytes]) -> None:
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.responses.pop(0))


class FredClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.window = FredWindow(
            realtime_start=date(2026, 8, 14),
            realtime_end=date(2026, 8, 20),
            observation_start=date(2026, 8, 13),
            observation_end=date(2026, 8, 20),
        )

    def test_observation_request_contains_point_in_time_window(self) -> None:
        opener = RecordingOpener([{"observations": [{"value": "4.31"}]}])
        client = FredClient("private-test-key", opener=opener)

        rows = client.fetch_observations("DGS10", self.window)

        self.assertEqual(rows, [{"value": "4.31"}])
        request, timeout = opener.requests[0]
        parsed = urlparse(request.full_url)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "api.stlouisfed.org")
        self.assertEqual(parsed.path, "/fred/series/observations")
        self.assertEqual(query["series_id"], ["DGS10"])
        self.assertEqual(query["file_type"], ["json"])
        self.assertEqual(query["output_type"], ["1"])
        self.assertEqual(query["realtime_start"], ["2026-08-14"])
        self.assertEqual(query["realtime_end"], ["2026-08-20"])
        self.assertEqual(query["observation_start"], ["2026-08-13"])
        self.assertEqual(query["observation_end"], ["2026-08-20"])
        self.assertEqual(timeout, 15.0)
        self.assertNotIn("private-test-key", str(query_without_key(query)))

    def test_series_request_uses_as_of_realtime_bounds(self) -> None:
        opener = RecordingOpener([{"seriess": [{"id": "DGS10"}]}])
        client = FredClient("private-test-key", opener=opener)

        row = client.fetch_series("DGS10", as_of=date(2026, 8, 20))

        self.assertEqual(row, {"id": "DGS10"})
        parsed = urlparse(opener.requests[0][0].full_url)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/fred/series")
        self.assertEqual(query["realtime_start"], ["2026-08-20"])
        self.assertEqual(query["realtime_end"], ["2026-08-20"])

    def test_classifies_rate_limit_timeout_and_transport_errors(self) -> None:
        cases = [
            (http_error(429), FredRateLimitError),
            (TimeoutError("private-test-key"), FredTimeoutError),
            (URLError(TimeoutError("private-test-key")), FredTimeoutError),
            (http_error(503), FredTransportError),
        ]
        for failure, expected in cases:
            with self.subTest(expected=expected.__name__):
                client = FredClient(
                    "private-test-key",
                    opener=RejectingOpener(failure),
                )
                with self.assertRaises(expected) as raised:
                    client.fetch_observations("DGS10", self.window)
                self.assertNotIn("private-test-key", str(raised.exception))

    def test_rejects_other_http_errors_invalid_json_and_missing_collection(self) -> None:
        clients = [
            FredClient("private-test-key", opener=RejectingOpener(http_error(400))),
            FredClient("private-test-key", opener=RecordingOpener([b"not-json"])),
            FredClient("private-test-key", opener=RecordingOpener([{"wrong": []}])),
        ]
        for client in clients:
            with self.subTest(client=client):
                with self.assertRaises(FredContractError) as raised:
                    client.fetch_observations("DGS10", self.window)
                self.assertNotIn("private-test-key", str(raised.exception))

    def test_rejects_empty_key_and_inverted_window(self) -> None:
        with self.assertRaisesRegex(ValueError, "API key"):
            FredClient("")
        with self.assertRaisesRegex(ValueError, "realtime_start"):
            FredWindow(
                realtime_start=date(2026, 8, 21),
                realtime_end=date(2026, 8, 20),
                observation_start=date(2026, 8, 13),
                observation_end=date(2026, 8, 20),
            )


class RejectingOpener:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    def __call__(self, request, timeout):
        raise self.failure


def http_error(status: int) -> HTTPError:
    return HTTPError(
        "https://api.stlouisfed.org/fred/series/observations",
        status,
        "failed",
        {},
        None,
    )


def query_without_key(query: dict[str, list[str]]) -> dict[str, list[str]]:
    return {key: value for key, value in query.items() if key != "api_key"}


if __name__ == "__main__":
    unittest.main()
