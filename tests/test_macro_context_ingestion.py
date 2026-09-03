import unittest
from datetime import UTC, date, datetime
from decimal import Decimal

from src.fred_client import MacroObservation
from src.macro_context_ingestion import (
    MACRO_SERIES,
    fetch_event_macro_context,
    select_latest_available,
)
from src.cpi_ingestion import CpiRelease
from src.economic_event_schedule import EconomicRelease


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

    def test_daily_series_uses_only_dates_before_the_release_day(self) -> None:
        class RecordingClient:
            def __init__(self) -> None:
                self.observation_end = None

            def fetch_observations(self, **kwargs):
                self.observation_end = kwargs["observation_end"]
                return [
                    MacroObservation(
                        series_id="DGS2",
                        observation_date=date(2024, 1, 10),
                        realtime_start=date(2024, 1, 11),
                        realtime_end=date(9999, 12, 31),
                        value=Decimal("4.3"),
                    )
                ]

        client = RecordingClient()
        release = CpiRelease(
            reference_period="2023-12",
            release_date=date(2024, 1, 11),
            released_at=datetime(2024, 1, 11, 13, 30, tzinfo=UTC),
            timezone="America/New_York",
            source_url="https://www.bls.gov/example",
        )

        contexts = fetch_event_macro_context(
            client,
            [release],
            series={"DGS2": MACRO_SERIES["DGS2"]},
        )

        self.assertEqual(client.observation_end, date(2024, 1, 10))
        self.assertEqual(contexts[0].observation_date, date(2024, 1, 10))

    def test_context_accepts_non_cpi_economic_releases(self) -> None:
        class RecordingClient:
            def fetch_observations(self, **kwargs):
                return [
                    MacroObservation(
                        series_id=kwargs["series_id"],
                        observation_date=date(2024, 1, 31),
                        realtime_start=date(2024, 2, 2),
                        realtime_end=date(9999, 12, 31),
                        value=Decimal("3.7"),
                    )
                ]

        release = EconomicRelease(
            event_type="EMPLOYMENT",
            reference_period="2024-01",
            release_date=date(2024, 2, 2),
            released_at=datetime(2024, 2, 2, 13, 30, tzinfo=UTC),
            timezone="America/New_York",
            source="BLS",
            source_url="https://www.bls.gov/example",
        )

        contexts = fetch_event_macro_context(
            RecordingClient(),
            [release],
            series={"UNRATE": MACRO_SERIES["UNRATE"]},
        )

        self.assertEqual(contexts[0].economic_event_id, release.event_id)
        self.assertEqual(contexts[0].series_id, "UNRATE")


if __name__ == "__main__":
    unittest.main()
