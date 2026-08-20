import json
import unittest
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from src.macro_models import FRED_SERIES, MacroObservation, MacroSeries


FIXTURE_ROOT = Path("tests/fixtures/fred")
INGESTED_AT = datetime(2026, 8, 20, 2, 0, tzinfo=UTC)


class MacroModelsTest(unittest.TestCase):
    def test_registry_contains_only_the_nine_project_series(self) -> None:
        self.assertEqual(
            FRED_SERIES,
            (
                "CPIAUCSL",
                "CPILFESL",
                "PCEPI",
                "PCEPILFE",
                "UNRATE",
                "DFF",
                "DGS2",
                "DGS10",
                "VIXCLS",
            ),
        )

    def test_builds_immutable_series_from_public_fixture(self) -> None:
        payload = self.fixture("series.json")["seriess"][0]

        series = MacroSeries.from_fred(payload, INGESTED_AT)

        self.assertEqual(series.series_id, "DGS10")
        self.assertEqual(series.frequency, "Daily")
        self.assertEqual(series.units, "Percent")
        self.assertEqual(series.observation_start, date(1962, 1, 2))
        self.assertEqual(series.observation_end, date(2026, 8, 19))
        self.assertEqual(series.last_updated.utcoffset(), -timedelta(hours=5))
        self.assertEqual(series.source, "fred")
        self.assertEqual(series.ingested_at, INGESTED_AT)
        with self.assertRaises(FrozenInstanceError):
            series.title = "changed"

    def test_parses_numeric_and_missing_observations(self) -> None:
        rows = self.fixture("observations.json")["observations"]

        numeric = MacroObservation.from_fred("DGS10", rows[0], INGESTED_AT)
        missing = MacroObservation.from_fred("DGS10", rows[1], INGESTED_AT)

        self.assertEqual(numeric.value, Decimal("4.31"))
        self.assertIsNone(missing.value)
        self.assertEqual(numeric.observation_date, date(2026, 8, 18))
        self.assertEqual(numeric.realtime_start, date(2026, 8, 20))
        self.assertEqual(numeric.realtime_end, date(9999, 12, 31))
        self.assertEqual(numeric.source, "fred")

    def test_rejects_invalid_numeric_value(self) -> None:
        payload = dict(
            self.fixture("observations.json")["observations"][0],
            value="not-a-number",
        )

        with self.assertRaisesRegex(ValueError, "numeric value"):
            MacroObservation.from_fred("DGS10", payload, INGESTED_AT)

    def test_rejects_inverted_realtime_range(self) -> None:
        payload = dict(
            self.fixture("observations.json")["observations"][0],
            realtime_start="2026-08-21",
            realtime_end="2026-08-20",
        )

        with self.assertRaisesRegex(ValueError, "realtime_start"):
            MacroObservation.from_fred("DGS10", payload, INGESTED_AT)

    def test_rejects_timezone_naive_ingestion_and_last_updated(self) -> None:
        series_payload = dict(
            self.fixture("series.json")["seriess"][0],
            last_updated="2026-08-19 15:16:01",
        )
        observation_payload = self.fixture("observations.json")["observations"][0]

        with self.assertRaisesRegex(ValueError, "last_updated"):
            MacroSeries.from_fred(series_payload, INGESTED_AT)
        with self.assertRaisesRegex(ValueError, "ingested_at"):
            MacroObservation.from_fred(
                "DGS10",
                observation_payload,
                datetime(2026, 8, 20, 2, 0),
            )

    @staticmethod
    def fixture(name: str) -> dict:
        return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
