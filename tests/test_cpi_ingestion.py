import unittest
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from src.cpi_ingestion import (
    CPI_SERIES,
    CpiRelease,
    fetch_release_observations,
    load_cpi_releases,
)
from src.fred_client import MacroObservation


class RecordingFredClient:
    def __init__(self) -> None:
        self.calls = []

    def fetch_observations(self, **kwargs):
        self.calls.append(kwargs)
        return [
            MacroObservation(
                series_id=kwargs["series_id"],
                observation_date=kwargs["observation_start"],
                realtime_start=kwargs["vintage_dates"][0],
                realtime_end=kwargs["vintage_dates"][0],
                value=Decimal("100.1"),
            )
        ]


class CpiIngestionTest(unittest.TestCase):
    def test_manifest_covers_official_releases_from_2022_through_august_2026(self) -> None:
        releases = load_cpi_releases(Path("config/cpi_releases.json"))

        self.assertEqual(len(releases), 55)
        self.assertNotIn("2025-10", {release.reference_period for release in releases})
        self.assertEqual(releases[0].reference_period, "2021-12")
        self.assertEqual(releases[0].released_at, datetime(2022, 1, 12, 13, 30, tzinfo=UTC))
        self.assertEqual(releases[-1].reference_period, "2026-07")
        self.assertEqual(releases[-1].released_at, datetime(2026, 8, 12, 12, 30, tzinfo=UTC))

        released_at = [release.released_at for release in releases]
        reference_periods = [release.reference_period for release in releases]
        self.assertEqual(released_at, sorted(released_at))
        self.assertEqual(len(released_at), len(set(released_at)))
        self.assertEqual(len(reference_periods), len(set(reference_periods)))

    def test_fetches_each_series_at_the_official_release_vintage(self) -> None:
        release = CpiRelease(
            reference_period="2025-07",
            release_date=date(2025, 8, 12),
            released_at=datetime(2025, 8, 12, 12, 30, tzinfo=UTC),
            timezone="America/New_York",
            source_url="https://www.bls.gov/",
        )
        client = RecordingFredClient()

        observations = fetch_release_observations(client, [release])

        self.assertEqual(len(observations), len(CPI_SERIES))
        self.assertEqual(
            {call["series_id"] for call in client.calls},
            set(CPI_SERIES),
        )
        self.assertTrue(
            all(call["vintage_dates"] == [date(2025, 8, 12)] for call in client.calls)
        )


if __name__ == "__main__":
    unittest.main()
