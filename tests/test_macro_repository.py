import json
import unittest
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from src.macro_models import MacroObservation, MacroSeries
from src.macro_repository import (
    macro_observation_rows,
    macro_series_row,
    upsert_macro_batch,
)


FIXTURE_ROOT = Path("tests/fixtures/fred")
INGESTED_AT = datetime(2026, 8, 20, 2, 0, tzinfo=UTC)


class MacroRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        series_payload = self.fixture("series.json")["seriess"][0]
        observation_payloads = self.fixture("observations.json")["observations"]
        self.series = MacroSeries.from_fred(series_payload, INGESTED_AT)
        self.observations = [
            MacroObservation.from_fred("DGS10", payload, INGESTED_AT)
            for payload in observation_payloads
        ]

    def test_serializes_series_parameters_in_sql_column_order(self) -> None:
        self.assertEqual(
            macro_series_row(self.series),
            (
                "DGS10",
                "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
                "Daily",
                "Percent",
                "Not Seasonally Adjusted",
                date(1962, 1, 2),
                date(2026, 8, 19),
                datetime(
                    2026,
                    8,
                    19,
                    15,
                    16,
                    1,
                    tzinfo=timezone(-timedelta(hours=5)),
                ),
                "Public contract fixture with shortened notes.",
                "fred",
                INGESTED_AT,
            ),
        )

    def test_serializes_observations_and_preserves_missing_value(self) -> None:
        rows = macro_observation_rows(self.series, self.observations)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            rows[0],
            (
                "DGS10",
                date(2026, 8, 18),
                Decimal("4.31"),
                date(2026, 8, 20),
                date(9999, 12, 31),
                "fred",
                INGESTED_AT,
            ),
        )
        self.assertIsNone(rows[1][2])

    def test_rejects_mixed_series_before_connecting(self) -> None:
        mixed = MacroObservation(
            series_id="DGS2",
            observation_date=self.observations[0].observation_date,
            value=self.observations[0].value,
            realtime_start=self.observations[0].realtime_start,
            realtime_end=self.observations[0].realtime_end,
            source="fred",
            ingested_at=INGESTED_AT,
        )

        with patch("src.macro_repository.psycopg.connect") as connect:
            with self.assertRaisesRegex(ValueError, "same series"):
                upsert_macro_batch(
                    self.series,
                    [self.observations[0], mixed],
                    database_url="postgresql://local/not-used",
                )
        connect.assert_not_called()

    @staticmethod
    def fixture(name: str) -> dict:
        return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
