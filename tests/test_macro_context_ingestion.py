import unittest
from datetime import date
from decimal import Decimal

from src.fred_client import MacroObservation
from src.macro_context_ingestion import MACRO_SERIES, select_latest_available


class MacroContextIngestionTest(unittest.TestCase):
    def test_catalog_contains_required_project_series(self) -> None:
        self.assertEqual(
            set(MACRO_SERIES),
            {
                "CPIAUCSL",
                "CPILFESL",
                "PCEPI",
                "PCEPILFE",
                "UNRATE",
                "PAYEMS",
                "DFF",
                "DGS2",
                "DGS10",
                "VIXCLS",
            },
        )

    def test_selects_latest_observation_known_on_release_date(self) -> None:
        observations = [
            MacroObservation(
                series_id="UNRATE",
                observation_date=date(2023, 11, 1),
                realtime_start=date(2023, 12, 8),
                realtime_end=date(2024, 1, 4),
                value=Decimal("3.7"),
            ),
            MacroObservation(
                series_id="UNRATE",
                observation_date=date(2023, 12, 1),
                realtime_start=date(2024, 1, 5),
                realtime_end=date(2024, 2, 1),
                value=Decimal("3.7"),
            ),
            MacroObservation(
                series_id="UNRATE",
                observation_date=date(2024, 1, 1),
                realtime_start=date(2024, 2, 2),
                realtime_end=date(9999, 12, 31),
                value=Decimal("3.7"),
            ),
        ]

        selected = select_latest_available(observations, as_of=date(2024, 1, 11))

        self.assertEqual(selected.observation_date, date(2023, 12, 1))
        self.assertLessEqual(selected.realtime_start, date(2024, 1, 11))
        self.assertGreaterEqual(selected.realtime_end, date(2024, 1, 11))

    def test_rejects_context_when_no_value_was_known_at_release(self) -> None:
        observations = [
            MacroObservation(
                series_id="UNRATE",
                observation_date=date(2024, 1, 1),
                realtime_start=date(2024, 2, 2),
                realtime_end=date(9999, 12, 31),
                value=Decimal("3.7"),
            )
        ]

        with self.assertRaisesRegex(
            ValueError, "no point-in-time observation was available"
        ):
            select_latest_available(observations, as_of=date(2024, 1, 11))


if __name__ == "__main__":
    unittest.main()
