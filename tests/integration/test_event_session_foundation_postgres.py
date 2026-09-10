import hashlib
import os
import unittest
from pathlib import Path

import psycopg


RUN_POSTGRES_INTEGRATION = os.environ.get("RUN_POSTGRES_INTEGRATION") == "1"
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
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
        with self.connection() as connection:
            connection.execute(
                """
                TRUNCATE economic_surprises, economic_consensus_snapshots,
                         economic_release_observations, economic_event_markers,
                         economic_event_lifecycle_versions, canonical_economic_events,
                         trading_sessions, calendar_snapshots
                RESTART IDENTITY CASCADE
                """
            )

    def connection(self):
        return psycopg.connect(DATABASE_URL, autocommit=True)

    def insert_event(self, connection, event_id="event:cpi", event_type="CPI") -> None:
        connection.execute(
            """
            INSERT INTO canonical_economic_events (
                economic_event_id, event_type, reference_period,
                official_source, official_source_url
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (event_id, event_type, "2026-08", "OFFICIAL", "https://official.invalid/"),
        )

    def record_lifecycle(
        self,
        connection,
        version: int,
        status: str,
        *,
        scheduled_at="2026-08-12 12:30:00+00",
        released_at=None,
        payload=None,
        first_observed_at="2026-08-01 00:01:00+00",
    ) -> int:
        return connection.execute(
            """
            SELECT record_economic_event_lifecycle(
                'event:cpi', %s, %s, %s, %s,
                'OFFICIAL', 'https://official.invalid/',
                '2026-08-01 00:00:00+00', %s, %s
            )
            """,
            (
                version, scheduled_at, released_at, status, first_observed_at,
                digest(payload or f"{version}:{status}"),
            ),
        ).fetchone()[0]

    def insert_marker(
        self,
        connection,
        marker_id="event:cpi:release:v1",
        *,
        kind="RELEASE",
        role="PRIMARY",
        revision=1,
        marker_at="2026-08-12 12:30:00+00",
    ) -> None:
        connection.execute(
            """
            INSERT INTO economic_event_markers (
                economic_event_marker_id, economic_event_id, marker_kind,
                marker_role, marker_revision, marker_at, source, source_url,
                payload_sha256, first_observed_at
            ) VALUES (%s, 'event:cpi', %s, %s, %s, %s, 'OFFICIAL',
                      'https://official.invalid/', %s, '2026-08-12 12:31:00+00')
            """,
            (marker_id, kind, role, revision, marker_at, digest(marker_id)),
        )

    def record_observation(
        self,
        connection,
        *,
        code="CPI_HEADLINE_MOM",
        revision=0,
        revision_type="INITIAL",
        value="0.3",
        published_at="2026-08-12 12:30:00+00",
        first_observed_at="2026-08-12 12:31:00+00",
        source_revision_id=None,
        payload="observation",
    ) -> int:
        return connection.execute(
            """
            SELECT record_economic_release_observation(
                'event:cpi', %s, %s, %s, %s, %s, 'PERCENT', %s, %s,
                'OFFICIAL', 'https://official.invalid/', %s
            )
            """,
            (
                code, revision, source_revision_id, revision_type, value,
                published_at, first_observed_at, digest(payload),
            ),
        ).fetchone()[0]

    def record_consensus(
        self,
        connection,
        *,
        code="CPI_HEADLINE_MOM",
        provider="VENDOR_A",
        snapshot_at="2026-08-12 12:29:00+00",
        first_observed_at="2026-08-12 12:29:30+00",
        value="0.2",
        payload="consensus",
    ) -> int:
        return connection.execute(
            """
            SELECT record_economic_consensus_snapshot(
                'event:cpi', %s, %s, NULL, %s, 'PERCENT', 10,
                %s, %s, %s, 'https://consensus.invalid/', %s
            )
            """,
            (
                code, provider, value, snapshot_at, snapshot_at,
                first_observed_at, digest(payload),
            ),
        ).fetchone()[0]

    def insert_surprise(self, connection, actual_id, consensus_id, **overrides):
        return connection.execute(
            """
            INSERT INTO economic_surprises (
                economic_event_id, observation_code, unit,
                actual_observation_id, consensus_snapshot_id,
                surprise_value, standardized_surprise, algorithm_version
            ) VALUES ('event:cpi', 'CPI_HEADLINE_MOM', 'PERCENT', %s, %s, %s, %s,
                      'canonical_event_surprise_v1')
            RETURNING economic_surprise_id
            """,
            (
                actual_id,
                consensus_id,
                overrides.get("surprise_value", "0.1"),
                overrides.get("standardized_surprise"),
            ),
        ).fetchone()[0]

    def test_T01_invalid_event_type_is_rejected(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_event(connection, event_type="INVALID")

    def test_T02_cpi_with_fed_observation_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            with self.assertRaises((psycopg.errors.ForeignKeyViolation, psycopg.errors.CheckViolation)):
                self.record_observation(connection, code="FED_TARGET_UPPER")

    def test_T03_cpi_with_fed_consensus_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            with self.assertRaises((psycopg.errors.ForeignKeyViolation, psycopg.errors.CheckViolation)):
                self.record_consensus(connection, code="FED_TARGET_UPPER")

    def test_T04_fomc_with_fed_observation_is_accepted(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection, event_type="FOMC")
            observation_id = self.record_observation(connection, code="FED_TARGET_UPPER")
        self.assertIsInstance(observation_id, int)

    def test_T05_released_to_scheduled_transition_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_lifecycle(connection, 1, "SCHEDULED")
            self.record_lifecycle(
                connection, 2, "RELEASED", released_at="2026-08-12 12:30:02+00"
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.record_lifecycle(connection, 3, "SCHEDULED")

    def test_T06_scheduled_rescheduled_released_chain_is_accepted(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_lifecycle(connection, 1, "SCHEDULED")
            self.record_lifecycle(
                connection, 2, "RESCHEDULED", scheduled_at="2026-08-13 12:30:00+00"
            )
            self.record_lifecycle(
                connection,
                3,
                "RELEASED",
                scheduled_at="2026-08-13 12:30:00+00",
                released_at="2026-08-13 12:30:01+00",
            )
            current = connection.execute(
                "SELECT lifecycle_version, event_status FROM current_economic_event_lifecycle"
            ).fetchone()
        self.assertEqual(current, (3, "RELEASED"))

    def test_lifecycle_same_material_is_idempotent_but_conflict_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            first = self.record_lifecycle(connection, 1, "SCHEDULED")
            repeated = self.record_lifecycle(
                connection,
                1,
                "SCHEDULED",
                first_observed_at="2026-09-10 00:00:00+00",
            )
            with self.assertRaisesRegex(
                psycopg.errors.UniqueViolation, "CONFLICTING_LIFECYCLE_FACT"
            ):
                self.record_lifecycle(
                    connection,
                    1,
                    "SCHEDULED",
                    scheduled_at="2026-08-13 12:30:00+00",
                )
        self.assertEqual(first, repeated)

    def test_T07_same_observation_revision_different_published_at_conflicts(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_observation(connection)
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "CONFLICTING_SOURCE_FACT"):
                self.record_observation(
                    connection,
                    published_at="2026-08-12 12:30:01+00",
                    first_observed_at="2026-08-12 12:31:01+00",
                )

    def test_T08_same_observation_revision_different_revision_type_conflicts(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_observation(connection, revision=1, revision_type="REVISION")
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "CONFLICTING_SOURCE_FACT"):
                self.record_observation(connection, revision=1, revision_type="CORRECTION")

    def test_T09_source_revision_id_cannot_name_different_revisions(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_observation(connection, source_revision_id="native:7")
            with self.assertRaises(psycopg.errors.UniqueViolation):
                self.record_observation(
                    connection,
                    revision=1,
                    revision_type="REVISION",
                    source_revision_id="native:7",
                    payload="revision",
                )

    def test_official_observation_material_fields_define_conflict(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_observation(connection)
            for change in ({"value": "0.4"}, {"payload": "changed"}):
                with self.subTest(change=change), self.assertRaisesRegex(
                    psycopg.errors.UniqueViolation, "CONFLICTING_SOURCE_FACT"
                ):
                    self.record_observation(connection, **change)

    def test_T10_same_consensus_identity_and_content_is_idempotent(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            first = self.record_consensus(connection)
            second = self.record_consensus(
                connection, first_observed_at="2026-09-10 00:00:00+00"
            )
            count = connection.execute("SELECT count(*) FROM economic_consensus_snapshots").fetchone()[0]
        self.assertEqual(first, second)
        self.assertEqual(count, 1)

    def test_T11_same_consensus_identity_different_content_conflicts(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.record_consensus(connection)
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "CONFLICTING_SOURCE_FACT"):
                self.record_consensus(connection, value="0.4", payload="changed")

    def test_T12_selector_is_scoped_to_explicit_provider(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            vendor_a = self.record_consensus(
                connection, provider="VENDOR_A", snapshot_at="2026-08-12 12:28:00+00"
            )
            self.record_consensus(
                connection,
                provider="VENDOR_B",
                snapshot_at="2026-08-12 12:29:00+00",
                payload="vendor-b",
            )
            selected = connection.execute(
                "SELECT select_canonical_prerelease_consensus('event:cpi', 'CPI_HEADLINE_MOM', 'VENDOR_A')"
            ).fetchone()[0]
        self.assertEqual(selected, vendor_a)

    def test_T13_consensus_at_marker_time_is_excluded(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            self.record_consensus(
                connection,
                snapshot_at="2026-08-12 12:30:00+00",
                first_observed_at="2026-08-12 12:30:01+00",
            )
            selected = connection.execute(
                "SELECT select_canonical_prerelease_consensus('event:cpi', 'CPI_HEADLINE_MOM', 'VENDOR_A')"
            ).fetchone()[0]
        self.assertIsNone(selected)

    def test_T14_consensus_first_observed_before_snapshot_is_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.record_consensus(
                    connection,
                    snapshot_at="2026-08-12 12:29:00+00",
                    first_observed_at="2026-08-12 12:28:59+00",
                )

    def test_T15_corrected_marker_is_new_revision_and_current(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            self.insert_marker(
                connection,
                marker_id="event:cpi:release:v2",
                revision=2,
                marker_at="2026-08-12 12:31:00+00",
            )
            rows = connection.execute(
                "SELECT marker_revision FROM economic_event_markers ORDER BY marker_revision"
            ).fetchall()
            current = connection.execute(
                "SELECT economic_event_marker_id FROM current_economic_event_markers"
            ).fetchone()[0]
        self.assertEqual(rows, [(1,), (2,)])
        self.assertEqual(current, "event:cpi:release:v2")

    def test_consensus_selector_uses_corrected_current_marker(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            self.record_consensus(
                connection,
                snapshot_at="2026-08-12 12:29:30+00",
                first_observed_at="2026-08-12 12:29:31+00",
            )
            self.insert_marker(
                connection,
                marker_id="event:cpi:release:v2",
                revision=2,
                marker_at="2026-08-12 12:29:00+00",
            )
            selected = connection.execute(
                "SELECT select_canonical_prerelease_consensus("
                "'event:cpi', 'CPI_HEADLINE_MOM', 'VENDOR_A')"
            ).fetchone()[0]
        self.assertIsNone(selected)

    def test_marker_history_is_append_only(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            with self.assertRaisesRegex(
                psycopg.errors.ObjectNotInPrerequisiteState, "append-only"
            ):
                connection.execute(
                    "UPDATE economic_event_markers SET marker_at='2026-08-12 12:31:00+00'"
                )

    def test_T16_two_current_primary_markers_are_rejected(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                self.insert_marker(
                    connection,
                    marker_id="event:cpi:statement:v1",
                    kind="STATEMENT",
                    revision=1,
                )

    def test_current_primary_marker_cannot_be_demoted_by_revision(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_marker(
                    connection,
                    marker_id="event:cpi:release:v2",
                    role="SECONDARY",
                    revision=2,
                )

    def test_T17_initial_actual_published_before_current_marker_is_rejected_for_surprise(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            actual = self.record_observation(
                connection, published_at="2026-08-12 12:29:59+00"
            )
            consensus = self.record_consensus(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_surprise(connection, actual, consensus)

    def test_T18_standardized_surprise_must_be_null(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            actual = self.record_observation(connection)
            consensus = self.record_consensus(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_surprise(
                    connection, actual, consensus, standardized_surprise="1.25"
                )

    def test_surprise_rejects_revised_actual(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            revised = self.record_observation(
                connection, revision=1, revision_type="REVISION", payload="revision"
            )
            consensus = self.record_consensus(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_surprise(connection, revised, consensus)

    def test_surprise_rejects_wrong_arithmetic(self) -> None:
        with self.connection() as connection:
            self.insert_event(connection)
            self.insert_marker(connection)
            actual = self.record_observation(connection)
            consensus = self.record_consensus(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_surprise(
                    connection, actual, consensus, surprise_value="0.2"
                )

    def test_T21_calendar_snapshot_metadata_is_append_only(self) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                INSERT INTO calendar_snapshots (
                    calendar_snapshot_id, market_code, exchange_timezone,
                    calendar_source, generated_at, payload_sha256
                ) VALUES ('calendar:v1', 'US_EQUITIES', 'America/New_York',
                          'XNYS_FIXTURE', '2026-08-01 00:00:00+00', %s)
                """,
                (digest("calendar-v1"),),
            )
            with self.assertRaisesRegex(psycopg.errors.ObjectNotInPrerequisiteState, "append-only"):
                connection.execute(
                    "UPDATE calendar_snapshots SET calendar_source='CHANGED' WHERE calendar_snapshot_id='calendar:v1'"
                )

    def test_sessions_inside_calendar_snapshot_are_append_only(self) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                INSERT INTO calendar_snapshots (
                    calendar_snapshot_id, market_code, exchange_timezone,
                    calendar_source, generated_at, payload_sha256
                ) VALUES ('calendar:v1', 'US_EQUITIES', 'America/New_York',
                          'XNYS_FIXTURE', '2026-08-01 00:00:00+00', %s)
                """,
                (digest("calendar-v1"),),
            )
            connection.execute(
                """
                INSERT INTO trading_sessions (
                    calendar_snapshot_id, session_date, opens_at, closes_at,
                    session_day_type
                ) VALUES ('calendar:v1', '2026-08-12', '2026-08-12 13:30:00+00',
                          '2026-08-12 20:00:00+00', 'REGULAR')
                """
            )
            with self.assertRaisesRegex(
                psycopg.errors.ObjectNotInPrerequisiteState, "append-only"
            ):
                connection.execute(
                    "UPDATE trading_sessions SET closes_at='2026-08-12 19:00:00+00' "
                    "WHERE calendar_snapshot_id='calendar:v1'"
                )


if __name__ == "__main__":
    unittest.main()
