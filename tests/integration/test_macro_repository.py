import json
import os
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg

from src.macro_models import MacroObservation, MacroSeries
from src.macro_repository import read_macro_quality, upsert_macro_batch


RUN_INTEGRATION = os.environ.get("RUN_MACRO_POSTGRES_INTEGRATION") == "1"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://market:market@localhost:55432/market",
)
FIXTURE_ROOT = Path("tests/fixtures/fred")
INGESTED_AT = datetime(2026, 8, 20, 2, 0, tzinfo=UTC)


@unittest.skipUnless(
    RUN_INTEGRATION,
    "set RUN_MACRO_POSTGRES_INTEGRATION=1 for the macro repository",
)
class MacroRepositoryIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        migration = Path("db/migrations/002_macro_observations.sql").read_text()
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(migration)

    def setUp(self) -> None:
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute("TRUNCATE macro_observations, macro_series")
        series_payload = self.fixture("series.json")["seriess"][0]
        observation_payloads = self.fixture("observations.json")["observations"]
        self.series = MacroSeries.from_fred(series_payload, INGESTED_AT)
        self.observations = [
            MacroObservation.from_fred("DGS10", payload, INGESTED_AT)
            for payload in observation_payloads
        ]

    def test_replay_is_idempotent_and_revision_updates_same_business_key(self) -> None:
        first = upsert_macro_batch(
            self.series,
            self.observations,
            database_url=DATABASE_URL,
        )
        second = upsert_macro_batch(
            self.series,
            self.observations,
            database_url=DATABASE_URL,
        )

        self.assertEqual(first.observation_count, 2)
        self.assertEqual(second.observation_count, 2)
        self.assertEqual(self.counts(), (1, 2))

        revised = replace(self.observations[0], value=Decimal("4.35"))
        upsert_macro_batch(
            self.series,
            [revised, self.observations[1]],
            database_url=DATABASE_URL,
        )

        with psycopg.connect(DATABASE_URL) as connection:
            value = connection.execute(
                """
                SELECT value
                FROM macro_observations
                WHERE series_id = 'DGS10'
                  AND observation_date = '2026-08-18'
                  AND realtime_start = '2026-08-20'
                """
            ).fetchone()[0]
        self.assertEqual(value, Decimal("4.35"))
        self.assertEqual(self.counts(), (1, 2))

    def test_invalid_realtime_row_rolls_back_entire_direct_sql_batch(self) -> None:
        upsert_macro_batch(self.series, [], database_url=DATABASE_URL)

        with self.assertRaises(psycopg.errors.CheckViolation):
            with psycopg.connect(DATABASE_URL) as connection:
                connection.execute(
                    """
                    INSERT INTO macro_observations (
                        series_id, observation_date, value,
                        realtime_start, realtime_end, source, ingested_at
                    ) VALUES
                        ('DGS10', '2026-08-17', 4.30,
                         '2026-08-20', '9999-12-31', 'fred', %s),
                        ('DGS10', '2026-08-18', 4.31,
                         '2026-08-21', '2026-08-20', 'fred', %s)
                    """,
                    (INGESTED_AT, INGESTED_AT),
                )

        self.assertEqual(self.counts(), (1, 0))

    def test_quality_preserves_sql_null_and_reports_missing_series(self) -> None:
        upsert_macro_batch(
            self.series,
            self.observations,
            database_url=DATABASE_URL,
        )

        quality = read_macro_quality(
            database_url=DATABASE_URL,
            expected_series=("DGS10", "DGS2"),
        )

        self.assertEqual(quality.series_count, 1)
        self.assertEqual(quality.observation_count, 2)
        self.assertEqual(quality.missing_count, 1)
        self.assertEqual(quality.missing_series, ("DGS2",))
        with psycopg.connect(DATABASE_URL) as connection:
            missing = connection.execute(
                "SELECT count(*) FROM macro_observations WHERE value IS NULL"
            ).fetchone()[0]
        self.assertEqual(missing, 1)

    @staticmethod
    def fixture(name: str) -> dict:
        return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))

    @staticmethod
    def counts() -> tuple[int, int]:
        with psycopg.connect(DATABASE_URL) as connection:
            series = connection.execute("SELECT count(*) FROM macro_series").fetchone()[
                0
            ]
            observations = connection.execute(
                "SELECT count(*) FROM macro_observations"
            ).fetchone()[0]
        return series, observations


if __name__ == "__main__":
    unittest.main()
