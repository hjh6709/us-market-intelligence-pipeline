import io
import json
import unittest
from datetime import date
from decimal import Decimal
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from src.fred_client import (
    FredClient,
    FredDataError,
    normalize_observations,
    observations_as_of,
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
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.payload)


class FredClientTest(unittest.TestCase):
    def test_fetches_explicit_vintage_and_preserves_realtime_fields(self) -> None:
        opener = RecordingOpener(
            {
                "observations": [
                    {
                        "realtime_start": "2025-02-12",
                        "realtime_end": "2025-03-11",
                        "date": "2025-01-01",
                        "value": "319.086",
                    }
                ]
            }
        )
        client = FredClient("private-fred-key", opener=opener)

        observations = client.fetch_observations(
            series_id="CPIAUCSL",
            observation_start=date(2025, 1, 1),
            observation_end=date(2025, 1, 1),
            vintage_dates=[date(2025, 2, 12)],
        )

        self.assertEqual(observations[0].value, Decimal("319.086"))
        self.assertEqual(observations[0].realtime_start, date(2025, 2, 12))
        query = parse_qs(urlparse(opener.requests[0][0].full_url).query)
        self.assertEqual(query["series_id"], ["CPIAUCSL"])
        self.assertEqual(query["vintage_dates"], ["2025-02-12"])
        self.assertEqual(query["output_type"], ["1"])

    def test_converts_fred_missing_marker_to_none(self) -> None:
        observations = normalize_observations(
            {
                "observations": [
                    {
                        "realtime_start": "2025-02-12",
                        "realtime_end": "9999-12-31",
                        "date": "2025-01-01",
                        "value": ".",
                    }
                ]
            },
            series_id="CPIAUCSL",
        )

        self.assertIsNone(observations[0].value)

    def test_filters_out_revision_first_known_after_event(self) -> None:
        observations = normalize_observations(
            {
                "observations": [
                    {
                        "realtime_start": "2025-02-12",
                        "realtime_end": "2025-03-11",
                        "date": "2025-01-01",
                        "value": "319.086",
                    },
                    {
                        "realtime_start": "2025-03-12",
                        "realtime_end": "9999-12-31",
                        "date": "2025-01-01",
                        "value": "319.091",
                    },
                ]
            },
            series_id="CPIAUCSL",
        )

        known = observations_as_of(observations, date(2025, 2, 12))

        self.assertEqual([item.value for item in known], [Decimal("319.086")])

        revised = observations_as_of(observations, date(2025, 3, 12))

        self.assertEqual([item.value for item in revised], [Decimal("319.091")])

    def test_http_error_never_exposes_api_key(self) -> None:
        def rejecting_opener(request, timeout):
            raise HTTPError(request.full_url, 401, "unauthorized", {}, io.BytesIO())

        client = FredClient("private-fred-key", opener=rejecting_opener)

        with self.assertRaises(FredDataError) as raised:
            client.fetch_observations(
                series_id="CPIAUCSL",
                observation_start=date(2025, 1, 1),
                observation_end=date(2025, 1, 1),
            )

        self.assertNotIn("private-fred-key", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
