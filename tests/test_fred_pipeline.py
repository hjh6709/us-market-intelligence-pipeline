import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from src.fred import FredWindow
from src.fred_pipeline import ingest_fred_series, resolve_fred_window
from src.macro_repository import MacroUpsertResult


FIXTURE_ROOT = Path("tests/fixtures/fred")


class FredWindowTest(unittest.TestCase):
    def test_daily_window_overlaps_seven_realtime_days_and_two_years(self) -> None:
        window = resolve_fred_window(date(2026, 8, 20), {})

        self.assertEqual(window.realtime_start, date(2026, 8, 14))
        self.assertEqual(window.realtime_end, date(2026, 8, 20))
        self.assertEqual(window.observation_start, date(2024, 8, 20))
        self.assertEqual(window.observation_end, date(2026, 8, 20))

    def test_manual_window_requires_all_four_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "all four"):
            resolve_fred_window(
                date(2026, 8, 20),
                {"realtime_start": "2026-08-14"},
            )

    def test_manual_window_parses_explicit_backfill(self) -> None:
        window = resolve_fred_window(
            date(2026, 8, 20),
            {
                "realtime_start": "2025-01-01",
                "realtime_end": "2025-01-31",
                "observation_start": "2024-01-01",
                "observation_end": "2025-01-31",
            },
        )

        self.assertEqual(window.realtime_start, date(2025, 1, 1))
        self.assertEqual(window.observation_start, date(2024, 1, 1))

    def test_manual_window_rejects_future_observation_end(self) -> None:
        with self.assertRaisesRegex(ValueError, "observation_end"):
            resolve_fred_window(
                date(2026, 8, 20),
                {
                    "realtime_start": "2025-01-01",
                    "realtime_end": "2025-01-31",
                    "observation_start": "2024-01-01",
                    "observation_end": "2025-02-01",
                },
            )


class FredIngestionTest(unittest.TestCase):
    def test_fetches_normalizes_and_upserts_one_series_once(self) -> None:
        client = FakeClient(
            self.fixture("series.json")["seriess"][0],
            self.fixture("observations.json")["observations"],
        )
        repository = RecordingRepository()
        window = FredWindow(
            realtime_start=date(2026, 8, 14),
            realtime_end=date(2026, 8, 20),
            observation_start=date(2024, 8, 20),
            observation_end=date(2026, 8, 20),
        )

        summary = ingest_fred_series(
            "DGS10",
            window,
            client=client,
            database_url="postgresql://local/not-logged",
            clock=lambda: datetime(2026, 8, 20, 2, 0, tzinfo=UTC),
            repository=repository,
        )

        self.assertEqual(client.series_calls, [("DGS10", date(2026, 8, 20))])
        self.assertEqual(client.observation_calls, [("DGS10", window)])
        self.assertEqual(len(repository.calls), 1)
        series, observations, database_url = repository.calls[0]
        self.assertEqual(series.series_id, "DGS10")
        self.assertEqual(len(observations), 2)
        self.assertIsNone(observations[1].value)
        self.assertEqual(database_url, "postgresql://local/not-logged")
        self.assertEqual(
            summary.as_dict(),
            {
                "series_id": "DGS10",
                "raw_count": 2,
                "normalized_count": 2,
                "missing_count": 1,
                "upserted_count": 2,
                "observation_start": "2024-08-20",
                "observation_end": "2026-08-20",
            },
        )
        serialized = json.dumps(summary.as_dict())
        self.assertNotIn("not-logged", serialized)
        self.assertNotIn("payload", serialized)

    @staticmethod
    def fixture(name: str) -> dict:
        return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


class FakeClient:
    def __init__(self, series: dict, observations: list[dict]) -> None:
        self.series = series
        self.observations = observations
        self.series_calls = []
        self.observation_calls = []

    def fetch_series(self, series_id: str, *, as_of: date) -> dict:
        self.series_calls.append((series_id, as_of))
        return dict(self.series)

    def fetch_observations(self, series_id: str, window: FredWindow) -> list[dict]:
        self.observation_calls.append((series_id, window))
        return [dict(row) for row in self.observations]


class RecordingRepository:
    def __init__(self) -> None:
        self.calls = []

    def __call__(self, series, observations, *, database_url):
        observations = list(observations)
        self.calls.append((series, observations, database_url))
        return MacroUpsertResult(
            series_id=series.series_id,
            observation_count=len(observations),
            missing_count=sum(row.value is None for row in observations),
        )


if __name__ == "__main__":
    unittest.main()
