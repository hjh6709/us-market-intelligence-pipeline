import hashlib
import os
import unittest
from pathlib import Path

import psycopg


RUN_POSTGRES_INTEGRATION = os.environ.get("RUN_POSTGRES_INTEGRATION") == "1"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://market:market@localhost:55432/market",
)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@unittest.skipUnless(
    RUN_POSTGRES_INTEGRATION,
    "set RUN_POSTGRES_INTEGRATION=1 to test a local PostgreSQL service",
)
class EventSessionFoundationPostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            for migration in sorted(Path("db/migrations").glob("*.sql")):
                connection.execute(migration.read_text(encoding="utf-8"))

    def setUp(self) -> None:
        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            connection.execute(
                """
                TRUNCATE economic_surprises, economic_consensus_snapshots,
                         economic_release_observations, economic_event_markers,
                         economic_event_lifecycle_versions, canonical_economic_events,
                         validation_reconstructed_bars
                RESTART IDENTITY CASCADE
                """
            )

    def connection(self):
        return psycopg.connect(DATABASE_URL, autocommit=True)

    def insert_event(self, connection, event_id: str = "event:cpi") -> None:
        connection.execute(
            """
            INSERT INTO canonical_economic_events (
                economic_event_id, event_type, reference_period,
                official_source, official_source_url
            ) VALUES (%s, 'CPI', '2026-08', 'BLS', 'https://www.bls.gov/cpi/')
            """,
            (event_id,),
        )

    def insert_primary_marker(self, connection, event_id: str = "event:cpi") -> None:
        connection.execute(
            """
            INSERT INTO economic_event_markers (
                economic_event_marker_id, economic_event_id, marker_kind,
                marker_role, marker_at, source, source_url, payload_sha256,
                first_observed_at
            ) VALUES (
                %s, %s, 'RELEASE', 'PRIMARY', '2026-08-12 12:30:00+00',
                'BLS', 'https://www.bls.gov/cpi/', %s,
                '2026-08-12 12:30:02+00'
            )
            """,
            (f"{event_id}:release", event_id, digest(f"{event_id}:marker")),
        )

    def record_observation(
        self,
        connection,
        *,
        event_id: str = "event:cpi",
        revision_number: int = 0,
        revision_type: str = "INITIAL",
        value: str = "0.3",
        unit: str = "PERCENT",
        payload: str = "actual-v1",
    ) -> int:
        return connection.execute(
            """
            SELECT record_economic_release_observation(
                %s, 'CPI_HEADLINE_MOM', %s, NULL, %s, %s, %s,
                '2026-08-12 12:30:00+00', '2026-08-12 12:30:03+00',
                'BLS', 'https://www.bls.gov/cpi/', %s
            )
            """,
            (event_id, revision_number, revision_type, value, unit, digest(payload)),
        ).fetchone()[0]

    def insert_consensus(
        self,
        connection,
        *,
        snapshot_at: str,
        value: str = "0.2",
        unit: str = "PERCENT",
        payload: str = "consensus",
    ) -> int:
        return connection.execute(
            """
            INSERT INTO economic_consensus_snapshots (
                economic_event_id, observation_code, value, unit, provider,
                contributor_count, snapshot_at, provider_updated_at,
                first_observed_at, source_url, payload_sha256
            ) VALUES (
                'event:cpi', 'CPI_HEADLINE_MOM', %s, %s, 'CONSENSUS_VENDOR',
                42, %s, %s, '2026-09-10 00:00:00+00',
                'https://consensus.invalid/snapshot', %s
            ) RETURNING consensus_snapshot_id
            """,
            (value, unit, snapshot_at, snapshot_at, digest(payload)),
        ).fetchone()[0]

    def test_upcoming_event_exists_before_release(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            connection.execute(
                """
                INSERT INTO economic_event_lifecycle_versions (
                    economic_event_id, lifecycle_version, scheduled_at,
                    released_at, event_status, source, source_url,
                    published_at, first_observed_at, payload_sha256
                ) VALUES (
                    'event:cpi', 1, '2026-09-11 12:30:00+00', NULL,
                    'SCHEDULED', 'BLS', 'https://www.bls.gov/cpi/',
                    '2026-09-01 12:00:00+00', '2026-09-01 12:01:00+00', %s
                )
                """,
                (digest("scheduled"),),
            )
            row = connection.execute(
                "SELECT event_status, released_at FROM economic_event_lifecycle_versions"
            ).fetchone()
        self.assertEqual(row, ("SCHEDULED", None))

    def test_scheduled_event_cannot_claim_an_actual_release_time(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO economic_event_lifecycle_versions (
                        economic_event_id, lifecycle_version, scheduled_at,
                        released_at, event_status, source, source_url,
                        published_at, first_observed_at, payload_sha256
                    ) VALUES (
                        'event:cpi', 1, '2026-09-11 12:30:00+00',
                        '2026-09-11 12:30:00+00', 'SCHEDULED', 'BLS',
                        'https://www.bls.gov/cpi/', '2026-09-01 12:00:00+00',
                        '2026-09-01 12:01:00+00', %s
                    )
                    """,
                    (digest("invalid-scheduled-release"),),
                )

    def test_same_revision_same_payload_is_idempotent(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            first = self.record_observation(connection)
            second = self.record_observation(connection)
            count = connection.execute(
                "SELECT count(*) FROM economic_release_observations"
            ).fetchone()[0]
        self.assertEqual(first, second)
        self.assertEqual(count, 1)

    def test_same_revision_conflicting_value_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_observation(connection)
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "CONFLICTING_SOURCE_FACT"):
                self.record_observation(connection, value="0.4", payload="conflict")

    def test_later_revision_is_preserved_separately(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            initial = self.record_observation(connection)
            revision = self.record_observation(
                connection,
                revision_number=1,
                revision_type="REVISION",
                value="0.4",
                payload="actual-v2",
            )
            count = connection.execute(
                "SELECT count(*) FROM economic_release_observations"
            ).fetchone()[0]
        self.assertNotEqual(initial, revision)
        self.assertEqual(count, 2)

    def test_observation_is_append_only(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            observation_id = self.record_observation(connection)
            with self.assertRaisesRegex(psycopg.errors.ObjectNotInPrerequisiteState, "append-only"):
                connection.execute(
                    "UPDATE economic_release_observations SET value=0.4 WHERE release_observation_id=%s",
                    (observation_id,),
                )

    def test_canonical_selector_excludes_post_release_consensus(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            before = self.insert_consensus(connection, snapshot_at="2026-08-12 12:29:00+00", payload="before")
            self.insert_consensus(connection, snapshot_at="2026-08-12 12:31:00+00", value="0.4", payload="after")
            selected = connection.execute(
                "SELECT select_canonical_prerelease_consensus('event:cpi', 'CPI_HEADLINE_MOM')"
            ).fetchone()[0]
        self.assertEqual(selected, before)

    def test_surprise_rejects_a_non_latest_eligible_consensus(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            actual = self.record_observation(connection)
            stale = self.insert_consensus(
                connection,
                snapshot_at="2026-08-12 12:28:00+00",
                payload="stale",
            )
            self.insert_consensus(
                connection,
                snapshot_at="2026-08-12 12:29:00+00",
                payload="latest",
            )
            with self.assertRaisesRegex(
                psycopg.errors.CheckViolation,
                "latest pre-release consensus",
            ):
                connection.execute(
                    """
                    INSERT INTO economic_surprises (
                        economic_event_id, observation_code, unit,
                        actual_observation_id, consensus_snapshot_id,
                        surprise_value, algorithm_version
                    ) VALUES (
                        'event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT',
                        %s, %s, 0.1, 'canonical_event_surprise_v1'
                    )
                    """,
                    (actual, stale),
                )

    def test_surprise_rejects_cross_event_lineage(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_event(connection, "event:other")
            self.insert_primary_marker(connection)
            actual = self.record_observation(connection, event_id="event:other")
            consensus = self.insert_consensus(
                connection,
                snapshot_at="2026-08-12 12:29:00+00",
            )
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                connection.execute(
                    """
                    INSERT INTO economic_surprises (
                        economic_event_id, observation_code, unit,
                        actual_observation_id, consensus_snapshot_id,
                        surprise_value, algorithm_version
                    ) VALUES (
                        'event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT',
                        %s, %s, 0.1, 'canonical_event_surprise_v1'
                    )
                    """,
                    (actual, consensus),
                )

    def test_surprise_requires_initial_actual_latest_pre_release_consensus_and_arithmetic(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            actual = self.record_observation(connection)
            consensus = self.insert_consensus(connection, snapshot_at="2026-08-12 12:29:00+00")
            surprise_id = connection.execute(
                """
                INSERT INTO economic_surprises (
                    economic_event_id, observation_code, unit,
                    actual_observation_id, consensus_snapshot_id,
                    surprise_value, algorithm_version
                ) VALUES (
                    'event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT',
                    %s, %s, 0.1, 'canonical_event_surprise_v1'
                ) RETURNING economic_surprise_id
                """,
                (actual, consensus),
            ).fetchone()[0]
        self.assertIsInstance(surprise_id, int)

    def test_revised_actual_cannot_masquerade_as_initial_surprise(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            revised = self.record_observation(
                connection,
                revision_number=1,
                revision_type="REVISION",
                value="0.4",
                payload="revision",
            )
            consensus = self.insert_consensus(connection, snapshot_at="2026-08-12 12:29:00+00")
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO economic_surprises (
                        economic_event_id, observation_code, unit,
                        actual_observation_id, consensus_snapshot_id,
                        surprise_value, algorithm_version
                    ) VALUES ('event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT', %s, %s, 0.2, 'v1')
                    """,
                    (revised, consensus),
                )

    def test_unit_mismatch_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                self.insert_consensus(
                    connection,
                    snapshot_at="2026-08-12 12:29:00+00",
                    unit="BASIS_POINTS",
                )

    def test_stored_surprise_must_equal_actual_minus_consensus(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            actual = self.record_observation(connection)
            consensus = self.insert_consensus(connection, snapshot_at="2026-08-12 12:29:00+00")
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO economic_surprises (
                        economic_event_id, observation_code, unit,
                        actual_observation_id, consensus_snapshot_id,
                        surprise_value, algorithm_version
                    ) VALUES ('event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT', %s, %s, 9.9, 'v1')
                    """,
                    (actual, consensus),
                )

    def test_calendar_and_marker_constraints_are_behavioral(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO economic_event_markers (
                        economic_event_marker_id, economic_event_id, marker_kind,
                        marker_role, marker_at, source, source_url,
                        payload_sha256, first_observed_at
                    ) VALUES (
                        'event:cpi:statement', 'event:cpi', 'STATEMENT', 'PRIMARY',
                        '2026-08-12 18:00:00+00', 'FED', 'https://federalreserve.gov/',
                        %s, '2026-08-12 18:00:01+00'
                    )
                    """,
                    (digest("second-primary"),),
                )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO trading_sessions (
                        market_code, session_date, opens_at, closes_at,
                        session_day_type, exchange_timezone, calendar_source,
                        calendar_snapshot_id, generated_at, payload_sha256
                    ) VALUES (
                        'US_EQUITIES', '2026-08-12',
                        '2026-08-13 13:30:00+00', '2026-08-13 20:00:00+00',
                        'REGULAR', 'America/New_York', 'TEST', 'sha256:calendar',
                        '2026-08-01 00:00:00+00', %s
                    )
                    """,
                    (digest("calendar"),),
                )

    def test_duplicate_marker_kind_and_untrusted_timezone_are_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_primary_marker(connection)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO economic_event_markers (
                        economic_event_marker_id, economic_event_id, marker_kind,
                        marker_role, marker_at, source, source_url,
                        payload_sha256, first_observed_at
                    ) VALUES (
                        'event:cpi:release-duplicate', 'event:cpi', 'RELEASE',
                        'SECONDARY', '2026-08-12 12:31:00+00', 'BLS',
                        'https://www.bls.gov/cpi/', %s,
                        '2026-08-12 12:31:01+00'
                    )
                    """,
                    (digest("duplicate-kind"),),
                )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO trading_sessions (
                        market_code, session_date, opens_at, closes_at,
                        session_day_type, exchange_timezone, calendar_source,
                        calendar_snapshot_id, generated_at, payload_sha256
                    ) VALUES (
                        'US_EQUITIES', '2026-08-12',
                        '2026-08-12 13:30:00+00', '2026-08-12 20:00:00+00',
                        'REGULAR', 'UTC', 'TEST', 'snapshot-1',
                        '2026-08-01 00:00:00+00', %s
                    )
                    """,
                    (digest("wrong-timezone"),),
                )


if __name__ == "__main__":
    unittest.main()
