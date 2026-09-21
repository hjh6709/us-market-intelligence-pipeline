import hashlib
import os
import unittest
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg

from src.cpi_w1_contracts import ObservationMaterial, ObservationState, PromotionFamily
from src.cpi_w1_promoter import (
    CpiW1Promoter,
    PromotionDeterminismError,
    promotion_work_key,
)
from src.cpi_w1_release import (
    ObservationBundleCandidate,
    ObservationCandidate,
    extract_core4_from_release_html,
    extract_release_envelope,
)
from src.cpi_w1_repository import CpiW1Repository, StaleClaimError


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
class CpiW1PostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            for migration in sorted(Path("db/migrations").glob("*.sql")):
                connection.execute(migration.read_text(encoding="utf-8"))

    def setUp(self) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                TRUNCATE interpretation_subjects, source_artifacts,
                         ingestion_attempts, ingestion_work_items, ingestion_runs
                CASCADE
                """
            )

    def connection(self):
        return psycopg.connect(DATABASE_URL, autocommit=True)

    def insert_run(
        self,
        connection,
        *,
        run_id=None,
        execution_scope="ECONOMIC_COLLECT",
        run_mode="LIVE",
        replay_of_run_id=None,
        trigger_idempotency_key=None,
    ):
        run_id = run_id or uuid4()
        connection.execute(
            """
            INSERT INTO ingestion_runs (
                run_id, execution_scope, data_domain, job_type, trigger_type,
                run_mode, trigger_idempotency_key, replay_of_run_id,
                source_revision, workload_artifact_digest,
                job_contract_version, config_fingerprint
            ) VALUES (%s, %s, 'ECONOMIC', 'CPI_W1', 'MANUAL', %s, %s, %s,
                      'test-source', %s, 'cpi-w1-job-v1', %s)
            """,
            (
                run_id,
                execution_scope,
                run_mode,
                trigger_idempotency_key,
                replay_of_run_id,
                digest("workload"),
                digest("config"),
            ),
        )
        return run_id

    def insert_work(
        self,
        connection,
        run_id,
        *,
        work_item_id=None,
        execution_scope="ECONOMIC_COLLECT",
        work_key="collect:cpi:2026-08",
    ):
        work_item_id = work_item_id or uuid4()
        connection.execute(
            """
            INSERT INTO ingestion_work_items (
                work_item_id, run_id, execution_scope, data_domain, work_key
            ) VALUES (%s, %s, %s, 'ECONOMIC', %s)
            """,
            (work_item_id, run_id, execution_scope, work_key),
        )
        return work_item_id

    def insert_attempt(
        self,
        connection,
        work_item_id,
        *,
        attempt_id=None,
        execution_scope="ECONOMIC_COLLECT",
        attempt_number=1,
    ):
        attempt_id = attempt_id or uuid4()
        row = connection.execute(
            """
            SELECT state, claim_generation
              FROM ingestion_work_items
             WHERE work_item_id=%s
             FOR UPDATE
            """,
            (work_item_id,),
        ).fetchone()
        if row is None:
            raise AssertionError("work item missing")
        state, claim_generation = row
        if state == "PENDING":
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED',
                       claim_generation=%s,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (attempt_number, uuid4(), work_item_id),
            )
            claim_generation = attempt_number
        if state == "CLAIMED" and claim_generation != attempt_number:
            raise AssertionError("attempt_number must match existing claim_generation")
        connection.execute(
            """
            INSERT INTO ingestion_attempts (
                attempt_id, work_item_id, execution_scope, data_domain,
                attempt_number
            ) VALUES (%s, %s, %s, 'ECONOMIC', %s)
            """,
            (attempt_id, work_item_id, execution_scope, attempt_number),
        )
        return attempt_id

    def test_bls_reference_source_is_seeded(self) -> None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT display_name FROM data_sources WHERE source_code='BLS'"
            ).fetchone()
        self.assertEqual(row, ("U.S. Bureau of Labor Statistics",))

    def test_duplicate_run_work_key_is_rejected(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            self.insert_work(connection, run_id)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                self.insert_work(connection, run_id, work_item_id=uuid4())

    def test_replay_requires_parent_run(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_run(connection, run_mode="REPLAY")

    def test_work_scope_must_match_parent_run_scope(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection, execution_scope="ECONOMIC_COLLECT")
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                self.insert_work(
                    connection,
                    run_id,
                    execution_scope="ECONOMIC_PROMOTE",
                )

    def test_retained_artifact_requires_storage_generation(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO source_artifacts (
                        artifact_id, data_domain, source_code, artifact_contract_kind,
                        source_contract_version, locator_key, retrieval_url,
                        content_sha256, content_type, captured_at,
                        created_by_attempt_id, created_by_execution_scope,
                        content_state, storage_uri, storage_generation
                    ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                              'bls-cpi-source-v1', 'release:2026-08',
                              'https://www.bls.gov/', %s, 'text/html', CURRENT_TIMESTAMP,
                              %s, 'ECONOMIC_COLLECT', 'RETAINED',
                              'file:///tmp/object', NULL)
                    """,
                    (uuid4(), digest("artifact"), attempt_id),
                )

    def test_source_artifact_hash_cannot_be_mutated(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            artifact_id = uuid4()
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, content_sha256,
                    content_type, captured_at, created_by_attempt_id,
                    created_by_execution_scope, content_state
                ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                          'bls-cpi-source-v1', 'release:immutable', %s,
                          'text/html', CURRENT_TIMESTAMP, %s,
                          'ECONOMIC_COLLECT', 'NOT_RETAINED')
                """,
                (artifact_id, digest("original"), attempt_id),
            )
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                connection.execute(
                    "UPDATE source_artifacts SET content_sha256=%s WHERE artifact_id=%s",
                    (digest("tampered"), artifact_id),
                )

    def test_source_artifact_row_cannot_be_deleted(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            artifact_id = uuid4()
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, content_sha256,
                    content_type, captured_at, created_by_attempt_id,
                    created_by_execution_scope, content_state
                ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                          'bls-cpi-source-v1', 'release:delete-guard', %s,
                          'text/html', CURRENT_TIMESTAMP, %s,
                          'ECONOMIC_COLLECT', 'NOT_RETAINED')
                """,
                (artifact_id, digest("delete-guard"), attempt_id),
            )
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                connection.execute(
                    "DELETE FROM source_artifacts WHERE artifact_id=%s",
                    (artifact_id,),
                )

    def test_deleted_by_policy_preserves_retained_object_metadata(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            artifact_id = uuid4()
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, content_sha256,
                    content_type, captured_at, created_by_attempt_id,
                    created_by_execution_scope, content_state,
                    storage_uri, storage_generation
                ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                          'bls-cpi-source-v1', 'release:2026-08', %s,
                          'text/html', CURRENT_TIMESTAMP, %s,
                          'ECONOMIC_COLLECT', 'RETAINED',
                          'file:///tmp/object', 'generation-1')
                """,
                (artifact_id, digest("retained-body"), attempt_id),
            )
            connection.execute(
                """
                UPDATE source_artifacts
                   SET content_state='DELETED_BY_POLICY'
                 WHERE artifact_id=%s
                """,
                (artifact_id,),
            )
            row = connection.execute(
                """
                SELECT content_state, storage_uri, storage_generation
                  FROM source_artifacts WHERE artifact_id=%s
                """,
                (artifact_id,),
            ).fetchone()
        self.assertEqual(
            row,
            ('DELETED_BY_POLICY', 'file:///tmp/object', 'generation-1'),
        )

    def test_new_run_cannot_bypass_created_state(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO ingestion_runs (
                        run_id, execution_scope, data_domain, job_type, trigger_type,
                        run_mode, source_revision, workload_artifact_digest,
                        job_contract_version, config_fingerprint,
                        state, outcome, finished_at
                    ) VALUES (%s, 'ECONOMIC_COLLECT', 'ECONOMIC', 'CPI_W1', 'MANUAL',
                              'LIVE', 'test-source', %s, 'cpi-w1-job-v1', %s,
                              'TERMINAL', 'NO_WORK', CURRENT_TIMESTAMP)
                    """,
                    (uuid4(), digest('workload'), digest('config')),
                )

    def test_same_bytes_from_different_attempts_are_distinct_capture_rows(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_a = self.insert_work(connection, run_id, work_key="collect:a")
            work_b = self.insert_work(connection, run_id, work_key="collect:b")
            attempt_a = self.insert_attempt(connection, work_a)
            attempt_b = self.insert_attempt(connection, work_b)
            body_hash = digest("same-body")
            for attempt_id in (attempt_a, attempt_b):
                connection.execute(
                    """
                    INSERT INTO source_artifacts (
                        artifact_id, data_domain, source_code, artifact_contract_kind,
                        source_contract_version, locator_key, content_sha256,
                        content_type, captured_at, created_by_attempt_id,
                        created_by_execution_scope, content_state
                    ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                              'bls-cpi-source-v1', 'release:2026-08', %s,
                              'text/html', CURRENT_TIMESTAMP, %s,
                              'ECONOMIC_COLLECT', 'NOT_RETAINED')
                    """,
                    (uuid4(), body_hash, attempt_id),
                )
            count = connection.execute(
                "SELECT count(*) FROM source_artifacts WHERE content_sha256=%s",
                (body_hash,),
            ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_same_attempt_locator_and_hash_is_unique(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            params = (digest("same-body"), attempt_id)
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, content_sha256,
                    content_type, captured_at, created_by_attempt_id,
                    created_by_execution_scope, content_state
                ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                          'bls-cpi-source-v1', 'release:2026-08', %s,
                          'text/html', CURRENT_TIMESTAMP, %s,
                          'ECONOMIC_COLLECT', 'NOT_RETAINED')
                """,
                (uuid4(), *params),
            )
            with self.assertRaises(psycopg.errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO source_artifacts (
                        artifact_id, data_domain, source_code, artifact_contract_kind,
                        source_contract_version, locator_key, content_sha256,
                        content_type, captured_at, created_by_attempt_id,
                        created_by_execution_scope, content_state
                    ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                              'bls-cpi-source-v1', 'release:2026-08', %s,
                              'text/html', CURRENT_TIMESTAMP, %s,
                              'ECONOMIC_COLLECT', 'NOT_RETAINED')
                    """,
                    (uuid4(), *params),
                )

    def test_terminal_run_rejects_new_work(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            connection.execute(
                """
                UPDATE ingestion_runs
                   SET state='TERMINAL', outcome='NO_WORK', finished_at=CURRENT_TIMESTAMP
                 WHERE run_id=%s
                """,
                (run_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_work(connection, run_id)

    def test_run_cannot_terminalize_with_pending_work(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            self.insert_work(connection, run_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_runs
                       SET state='TERMINAL', outcome='FAILED', finished_at=CURRENT_TIMESTAMP
                     WHERE run_id=%s
                    """,
                    (run_id,),
                )


    def make_attempt(self, connection, scope, work_key):
        run_id = self.insert_run(connection, execution_scope=scope)
        work_id = self.insert_work(
            connection,
            run_id,
            execution_scope=scope,
            work_key=work_key,
        )
        claim_token = uuid4()
        connection.execute(
            """
            UPDATE ingestion_work_items
               SET state='CLAIMED', claim_generation=1,
                   claim_token=%s, lease_until=CURRENT_TIMESTAMP + interval '5 minutes'
             WHERE work_item_id=%s
            """,
            (claim_token, work_id),
        )
        return self.insert_attempt(
            connection,
            work_id,
            execution_scope=scope,
            attempt_number=1,
        )

    def make_artifact(self, connection, locator="release:2026-08"):
        attempt_id = self.make_attempt(
            connection, "ECONOMIC_COLLECT", f"collect:{locator}:{uuid4()}"
        )
        artifact_id = uuid4()
        connection.execute(
            """
            INSERT INTO source_artifacts (
                artifact_id, data_domain, source_code, artifact_contract_kind,
                source_contract_version, locator_key, content_sha256,
                content_type, captured_at, created_by_attempt_id,
                created_by_execution_scope, content_state
            ) VALUES (%s, 'ECONOMIC', 'BLS', 'CPI_RELEASE_HTML',
                      'bls-cpi-source-v1', %s, %s, 'text/html',
                      CURRENT_TIMESTAMP, %s, 'ECONOMIC_COLLECT', 'NOT_RETAINED')
            """,
            (artifact_id, locator, digest(str(artifact_id)), attempt_id),
        )
        return artifact_id

    def make_event(self, connection, reference_month="2026-08-01"):
        attempt_id = self.make_attempt(
            connection, "ECONOMIC_PROMOTE", f"promote:event:{reference_month}:{uuid4()}"
        )
        event_id = uuid4()
        connection.execute(
            """
            INSERT INTO core_event_occurrences (
                event_occurrence_id, event_type, reference_month,
                created_by_attempt_id
            ) VALUES (%s, 'CPI', %s, %s)
            """,
            (event_id, reference_month, attempt_id),
        )
        return event_id, attempt_id

    def make_disclosure(self, connection, attempt_id, key=None):
        disclosure_id = uuid4()
        connection.execute(
            """
            INSERT INTO event_disclosures (
                disclosure_id, source_code, canonical_disclosure_key,
                disclosure_kind, established_by_attempt_id
            ) VALUES (%s, 'BLS', %s, 'DATA_RELEASE', %s)
            """,
            (disclosure_id, key or f"bls-cpi:{uuid4()}", attempt_id),
        )
        return disclosure_id

    def insert_subject(self, connection, subject_id, subject_type):
        connection.execute(
            "INSERT INTO interpretation_subjects (subject_id, subject_type) VALUES (%s, %s)",
            (subject_id, subject_type),
        )

    def test_collector_attempt_cannot_establish_canonical_event(self) -> None:
        with self.connection() as connection:
            collect_attempt = self.make_attempt(
                connection,
                "ECONOMIC_COLLECT",
                f"collect:illegal-canonical:{uuid4()}",
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO core_event_occurrences (
                        event_occurrence_id, event_type, reference_month,
                        created_by_attempt_id
                    ) VALUES (%s, 'CPI', '2026-05-01', %s)
                    """,
                    (uuid4(), collect_attempt),
                )

    def test_stale_promoter_attempt_cannot_insert_canonical_event(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"promote:stale:{uuid4()}",
            )
            first_token = uuid4()
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED', claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second'
                 WHERE work_item_id=%s
                """,
                (first_token, work_id),
            )
            stale_attempt = self.insert_attempt(
                connection,
                work_id,
                execution_scope="ECONOMIC_PROMOTE",
                attempt_number=1,
            )
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET claim_generation=2,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            with self.assertRaises(psycopg.errors.NoDataFound):
                connection.execute(
                    """
                    INSERT INTO core_event_occurrences (
                        event_occurrence_id, event_type, reference_month,
                        created_by_attempt_id
                    ) VALUES (%s, 'CPI', '2026-04-01', %s)
                    """,
                    (uuid4(), stale_attempt),
                )

    def test_cpi_reference_month_must_be_first_day(self) -> None:
        with self.connection() as connection:
            attempt_id = self.make_attempt(
                connection, "ECONOMIC_PROMOTE", "promote:bad-reference-month"
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO core_event_occurrences (
                        event_occurrence_id, event_type, reference_month,
                        created_by_attempt_id
                    ) VALUES (%s, 'CPI', '2026-08-02', %s)
                    """,
                    (uuid4(), attempt_id),
                )

    def test_schedule_can_be_absent_without_date_pending_fact(self) -> None:
        with self.connection() as connection:
            event_id, _ = self.make_event(connection)
            count = connection.execute(
                "SELECT count(*) FROM event_schedule_assertions WHERE event_occurrence_id=%s",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_exact_and_date_only_schedule_are_distinct_evidences(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            artifact_a = self.make_artifact(connection, "schedule:exact")
            artifact_b = self.make_artifact(connection, "schedule:date-only")
            exact_id = uuid4()
            date_id = uuid4()
            self.insert_subject(connection, exact_id, "SCHEDULE_ASSERTION")
            self.insert_subject(connection, date_id, "SCHEDULE_ASSERTION")
            connection.execute(
                """
                INSERT INTO event_schedule_assertions (
                    schedule_assertion_id, event_occurrence_id, schedule_status,
                    scheduled_date, scheduled_at, schedule_timezone, time_precision,
                    source_code, source_artifact_id, extractor_contract_version,
                    accepted_by_attempt_id, accepted_at, material_fingerprint
                ) VALUES
                    (%s, %s, 'SCHEDULED', '2026-09-11',
                     '2026-09-11 12:30:00+00', 'America/New_York', 'EXACT',
                     'BLS', %s, 'schedule-v1', %s, CURRENT_TIMESTAMP, %s),
                    (%s, %s, 'SCHEDULED', '2026-09-12',
                     NULL, 'America/New_York', 'DATE_ONLY',
                     'BLS', %s, 'schedule-v1', %s, CURRENT_TIMESTAMP, %s)
                """,
                (
                    exact_id, event_id, artifact_a, promote_attempt, digest("exact"),
                    date_id, event_id, artifact_b, promote_attempt, digest("date"),
                ),
            )
            rows = connection.execute(
                """
                SELECT time_precision, scheduled_at IS NULL
                  FROM event_schedule_assertions
                 WHERE event_occurrence_id=%s
                 ORDER BY time_precision
                """,
                (event_id,),
            ).fetchall()
        self.assertEqual(rows, [('DATE_ONLY', True), ('EXACT', False)])

    def test_reschedule_is_two_assertions_not_rescheduled_state(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            for day in (11, 12):
                artifact_id = self.make_artifact(connection, f"schedule:{day}")
                assertion_id = uuid4()
                self.insert_subject(connection, assertion_id, "SCHEDULE_ASSERTION")
                connection.execute(
                    """
                    INSERT INTO event_schedule_assertions (
                        schedule_assertion_id, event_occurrence_id, schedule_status,
                        scheduled_date, scheduled_at, schedule_timezone, time_precision,
                        source_code, source_artifact_id, extractor_contract_version,
                        accepted_by_attempt_id, accepted_at, material_fingerprint
                    ) VALUES (%s, %s, 'SCHEDULED', %s, NULL,
                              'America/New_York', 'DATE_ONLY', 'BLS', %s,
                              'schedule-v1', %s, CURRENT_TIMESTAMP, %s)
                    """,
                    (
                        assertion_id,
                        event_id,
                        f"2026-09-{day:02d}",
                        artifact_id,
                        promote_attempt,
                        digest(f"schedule-{day}"),
                    ),
                )
            rows = connection.execute(
                "SELECT count(*) FROM event_schedule_assertions WHERE event_occurrence_id=%s",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(rows, 2)

    def test_one_disclosure_can_link_multiple_events_and_relation_kinds(self) -> None:
        with self.connection() as connection:
            event_a, promote_attempt = self.make_event(connection, "2026-07-01")
            event_b, _ = self.make_event(connection, "2026-08-01")
            disclosure_id = self.make_disclosure(connection, promote_attempt)
            links = (
                (event_a, "EVENT_RELEASE"),
                (event_b, "SUPPLEMENTAL_DISCLOSURE"),
                (event_b, "EVENT_RELEASE"),
            )
            for event_id, kind in links:
                link_id = uuid4()
                self.insert_subject(connection, link_id, "EVENT_DISCLOSURE_LINK")
                connection.execute(
                    """
                    INSERT INTO event_disclosure_links (
                        disclosure_link_id, event_occurrence_id, disclosure_id,
                        relation_kind, accepted_by_attempt_id, accepted_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (link_id, event_id, disclosure_id, kind, promote_attempt),
                )
            count = connection.execute(
                "SELECT count(*) FROM event_disclosure_links WHERE disclosure_id=%s",
                (disclosure_id,),
            ).fetchone()[0]
        self.assertEqual(count, 3)

    def test_marker_cannot_pin_unrelated_artifact_link(self) -> None:
        with self.connection() as connection:
            _, promote_attempt = self.make_event(connection)
            disclosure_id = self.make_disclosure(connection, promote_attempt)
            artifact_a = self.make_artifact(connection, "release:a")
            artifact_b = self.make_artifact(connection, "release:b")
            link_id = uuid4()
            self.insert_subject(connection, link_id, "DISCLOSURE_ARTIFACT_LINK")
            connection.execute(
                """
                INSERT INTO event_disclosure_artifacts (
                    disclosure_artifact_link_id, disclosure_id, artifact_id,
                    relation_kind, accepted_by_attempt_id, accepted_at
                ) VALUES (%s, %s, %s, 'RELEASE_REPRESENTATION', %s, CURRENT_TIMESTAMP)
                """,
                (link_id, disclosure_id, artifact_a, promote_attempt),
            )
            marker_id = uuid4()
            self.insert_subject(connection, marker_id, "DISCLOSURE_MARKER_ASSERTION")
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                connection.execute(
                    """
                    INSERT INTO disclosure_marker_assertions (
                        marker_assertion_id, disclosure_id, disclosure_artifact_link_id,
                        marker_semantics, marker_date, marker_at, marker_timezone,
                        time_precision, source_code, source_artifact_id,
                        extractor_contract_version, accepted_by_attempt_id,
                        accepted_at, material_fingerprint
                    ) VALUES (
                        %s, %s, %s, 'EMBARGO_LIFT', '2026-09-11',
                        '2026-09-11 12:30:00+00', 'America/New_York', 'EXACT',
                        'BLS', %s, 'release-v1', %s, CURRENT_TIMESTAMP, %s
                    )
                    """,
                    (
                        marker_id,
                        disclosure_id,
                        link_id,
                        artifact_b,
                        promote_attempt,
                        digest("marker"),
                    ),
                )

    def test_disclosure_artifact_source_must_match_disclosure_source(self) -> None:
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO data_sources (source_code, display_name) VALUES ('OTHER', 'Other') ON CONFLICT DO NOTHING"
            )
            _, promote_attempt = self.make_event(connection)
            disclosure_id = self.make_disclosure(connection, promote_attempt)
            collector_attempt = self.make_attempt(
                connection, "ECONOMIC_COLLECT", f"collect:other:{uuid4()}"
            )
            artifact_id = uuid4()
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, content_sha256,
                    content_type, captured_at, created_by_attempt_id,
                    created_by_execution_scope, content_state
                ) VALUES (%s, 'ECONOMIC', 'OTHER', 'TEST', 'v1', %s, %s,
                          'text/plain', CURRENT_TIMESTAMP, %s,
                          'ECONOMIC_COLLECT', 'NOT_RETAINED')
                """,
                (artifact_id, f"other:{artifact_id}", digest(str(artifact_id)), collector_attempt),
            )
            link_id = uuid4()
            self.insert_subject(connection, link_id, "DISCLOSURE_ARTIFACT_LINK")
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO event_disclosure_artifacts (
                        disclosure_artifact_link_id, disclosure_id, artifact_id,
                        relation_kind, accepted_by_attempt_id, accepted_at
                    ) VALUES (%s, %s, %s, 'RELEASE_REPRESENTATION', %s, CURRENT_TIMESTAMP)
                    """,
                    (link_id, disclosure_id, artifact_id, promote_attempt),
                )


    def make_observation_topology(
        self,
        connection,
        *,
        reference_month="2026-08-01",
        relation_kind="EVENT_RELEASE",
        artifact_locator=None,
    ):
        event_id, promote_attempt = self.make_event(connection, reference_month)
        disclosure_id = self.make_disclosure(connection, promote_attempt)

        disclosure_link_id = uuid4()
        self.insert_subject(connection, disclosure_link_id, "EVENT_DISCLOSURE_LINK")
        connection.execute(
            """
            INSERT INTO event_disclosure_links (
                disclosure_link_id, event_occurrence_id, disclosure_id,
                relation_kind, accepted_by_attempt_id, accepted_at
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                disclosure_link_id,
                event_id,
                disclosure_id,
                relation_kind,
                promote_attempt,
            ),
        )

        artifact_id = self.make_artifact(
            connection,
            artifact_locator or f"observation:{reference_month}:{uuid4()}",
        )
        disclosure_artifact_link_id = uuid4()
        self.insert_subject(
            connection,
            disclosure_artifact_link_id,
            "DISCLOSURE_ARTIFACT_LINK",
        )
        connection.execute(
            """
            INSERT INTO event_disclosure_artifacts (
                disclosure_artifact_link_id, disclosure_id, artifact_id,
                relation_kind, accepted_by_attempt_id, accepted_at
            ) VALUES (
                %s, %s, %s, 'RELEASE_REPRESENTATION', %s, CURRENT_TIMESTAMP
            )
            """,
            (
                disclosure_artifact_link_id,
                disclosure_id,
                artifact_id,
                promote_attempt,
            ),
        )
        return {
            "event_id": event_id,
            "promote_attempt": promote_attempt,
            "disclosure_id": disclosure_id,
            "disclosure_link_id": disclosure_link_id,
            "artifact_id": artifact_id,
            "disclosure_artifact_link_id": disclosure_artifact_link_id,
        }

    def insert_observation_assertion(
        self,
        connection,
        topology,
        *,
        assertion_state="VALUE",
        normalized_value=Decimal("2.9"),
        observation_code="CPI_HEADLINE_YOY",
        source_reason_text=None,
        event_id=None,
        disclosure_id=None,
        disclosure_link_id=None,
        artifact_id=None,
        disclosure_artifact_link_id=None,
    ):
        assertion_id = uuid4()
        self.insert_subject(
            connection,
            assertion_id,
            "OFFICIAL_OBSERVATION_ASSERTION",
        )
        connection.execute(
            """
            INSERT INTO official_observation_assertions (
                assertion_id, event_occurrence_id, event_type, disclosure_id,
                disclosure_link_id, disclosure_artifact_link_id,
                observation_code, assertion_state, normalized_value,
                source_value_text, source_reason_text, source_code,
                source_artifact_id, extractor_contract_version,
                accepted_by_attempt_id, accepted_at, material_fingerprint
            ) VALUES (
                %s, %s, 'CPI', %s, %s, %s, %s, %s, %s,
                %s, %s, 'BLS', %s, 'bls-cpi-extractor-v1',
                %s, CURRENT_TIMESTAMP, %s
            )
            """,
            (
                assertion_id,
                event_id or topology["event_id"],
                disclosure_id or topology["disclosure_id"],
                disclosure_link_id or topology["disclosure_link_id"],
                disclosure_artifact_link_id
                or topology["disclosure_artifact_link_id"],
                observation_code,
                assertion_state,
                normalized_value,
                None if normalized_value is None else str(normalized_value),
                source_reason_text,
                artifact_id or topology["artifact_id"],
                topology["promote_attempt"],
                digest(
                    f"{observation_code}:{assertion_state}:{normalized_value}"
                ),
            ),
        )
        return assertion_id

    def test_accepted_at_is_database_owned(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            disclosure_id = self.make_disclosure(connection, promote_attempt)
            disclosure_link_id = uuid4()
            self.insert_subject(
                connection,
                disclosure_link_id,
                "EVENT_DISCLOSURE_LINK",
            )
            stored, statement_transaction_time = connection.execute(
                """
                INSERT INTO event_disclosure_links (
                    disclosure_link_id, event_occurrence_id, disclosure_id,
                    relation_kind, accepted_by_attempt_id, accepted_at
                ) VALUES (
                    %s, %s, %s, 'EVENT_RELEASE', %s,
                    '2000-01-01 00:00:00+00'
                )
                RETURNING accepted_at, CURRENT_TIMESTAMP
                """,
                (
                    disclosure_link_id,
                    event_id,
                    disclosure_id,
                    promote_attempt,
                ),
            ).fetchone()
        self.assertEqual(stored, statement_transaction_time)
        self.assertNotEqual(stored.year, 2000)

    def test_official_observation_decimal_round_trips_exactly(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(connection)
            assertion_id = self.insert_observation_assertion(
                connection,
                topology,
                normalized_value=Decimal("2.9"),
            )
            value = connection.execute(
                """
                SELECT normalized_value
                  FROM official_observation_assertions
                 WHERE assertion_id=%s
                """,
                (assertion_id,),
            ).fetchone()[0]
        self.assertEqual(value, Decimal("2.9"))

    def test_value_observation_requires_numeric_value(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_observation_assertion(
                    connection,
                    topology,
                    assertion_state="VALUE",
                    normalized_value=None,
                )

    def test_explicit_unavailable_rejects_numeric_value(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_observation_assertion(
                    connection,
                    topology,
                    assertion_state="EXPLICIT_UNAVAILABLE",
                    normalized_value=Decimal("0"),
                    source_reason_text="Source explicitly states value unavailable.",
                )

    def test_explicit_unavailable_requires_nonempty_source_reason_text(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.insert_observation_assertion(
                    connection,
                    topology,
                    assertion_state="EXPLICIT_UNAVAILABLE",
                    normalized_value=None,
                    source_reason_text=None,
                )

    def test_observation_cannot_pin_disclosure_link_from_other_event(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(
                connection,
                reference_month="2026-08-01",
            )
            other_event_id, _ = self.make_event(connection, "2026-07-01")
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                self.insert_observation_assertion(
                    connection,
                    topology,
                    event_id=other_event_id,
                )

    def test_observation_cannot_pin_unrelated_artifact(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(connection)
            unrelated_artifact = self.make_artifact(
                connection,
                f"observation:unrelated:{uuid4()}",
            )
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                self.insert_observation_assertion(
                    connection,
                    topology,
                    artifact_id=unrelated_artifact,
                )

    def test_storage_can_preserve_supplemental_observation_evidence(self) -> None:
        with self.connection() as connection:
            topology = self.make_observation_topology(
                connection,
                relation_kind="SUPPLEMENTAL_DISCLOSURE",
            )
            assertion_id = self.insert_observation_assertion(
                connection,
                topology,
                observation_code="CPI_CORE_YOY",
                normalized_value=Decimal("3.1"),
            )
            relation_kind = connection.execute(
                """
                SELECT l.relation_kind
                  FROM official_observation_assertions o
                  JOIN event_disclosure_links l
                    ON l.disclosure_link_id = o.disclosure_link_id
                 WHERE o.assertion_id=%s
                """,
                (assertion_id,),
            ).fetchone()[0]
        self.assertEqual(relation_kind, "SUPPLEMENTAL_DISCLOSURE")


    def create_interpretation_request(
        self,
        connection,
        subject_id,
        *,
        requested_state="INVALID",
        expected_version=0,
        proposer="worker:proposer",
        requested_at=None,
    ):
        request_id = uuid4()
        connection.execute(
            """
            INSERT INTO interpretation_requests (
                request_id, subject_id, requested_state,
                expected_decision_version, reason_code, case_ref,
                governance_policy_version, proposer_subject, requested_at
            ) VALUES (
                %s, %s, %s, %s, 'TEST_REVIEW', 'CASE-1',
                'cpi-governance-v1', %s, COALESCE(%s, CURRENT_TIMESTAMP)
            )
            """,
            (
                request_id,
                subject_id,
                requested_state,
                expected_version,
                proposer,
                requested_at,
            ),
        )
        return request_id

    def approve_interpretation_request(
        self,
        connection,
        request_id,
        *,
        proposer="worker:proposer",
        approver="worker:approver",
        decision="APPROVE",
    ):
        connection.execute(
            """
            INSERT INTO interpretation_approvals (
                approval_id, request_id, proposer_subject,
                approver_subject, approval_decision
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (uuid4(), request_id, proposer, approver, decision),
        )

    def make_governable_observation(self, connection):
        topology = self.make_observation_topology(connection)
        return self.insert_observation_assertion(
            connection,
            topology,
            normalized_value=Decimal("2.9"),
        )

    def test_interpretation_subject_without_typed_evidence_cannot_activate(self) -> None:
        with self.connection() as connection:
            subject_id = uuid4()
            self.insert_subject(
                connection,
                subject_id,
                "OFFICIAL_OBSERVATION_ASSERTION",
            )
            request_id = self.create_interpretation_request(
                connection,
                subject_id,
            )
            self.approve_interpretation_request(connection, request_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (request_id, "worker:activator"),
                )

    def test_future_dated_interpretation_request_cannot_activate(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(
                connection,
                subject_id,
                requested_at="2099-01-01 00:00:00+00",
            )
            self.approve_interpretation_request(connection, request_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (request_id, "worker:activator"),
                )

    def test_identity_whitespace_cannot_bypass_self_approval_guard(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(connection, subject_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.approve_interpretation_request(
                    connection,
                    request_id,
                    approver="worker:proposer ",
                )

    def test_blank_serving_reason_code_is_rejected(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    SELECT apply_economic_serving_control(
                        'CPI_DOMAIN', NULL, 0, 'WITHHELD', '   ',
                        'worker:operator', 'CASE-1', NULL, NULL
                    )
                    """
                )

    def test_activation_actor_whitespace_is_rejected(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(connection, subject_id)
            self.approve_interpretation_request(connection, request_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (request_id, "worker:activator "),
                )

    def test_serving_control_actor_whitespace_is_rejected(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    SELECT apply_economic_serving_control(
                        'CPI_DOMAIN', NULL, 0, 'WITHHELD', 'TEST',
                        'worker:operator ', NULL, NULL, NULL
                    )
                    """
                )

    def test_event_reenable_rejects_malformed_knowledge_fingerprint(self) -> None:
        with self.connection() as connection:
            event_id, _ = self.make_event(connection, "2026-05-01")
            connection.execute(
                """
                SELECT apply_economic_serving_control(
                    'EVENT_OCCURRENCE', %s, 0, 'WITHHELD', 'TEST',
                    'worker:operator', NULL, NULL, NULL
                )
                """,
                (event_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    SELECT apply_economic_serving_control(
                        'EVENT_OCCURRENCE', %s, 1, 'ENABLED', 'TEST',
                        'worker:operator', 'CASE-3', 'not-a-sha256', NULL
                    )
                    """,
                    (event_id,),
                )

    def test_interpretation_self_approval_is_structurally_rejected(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(connection, subject_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.approve_interpretation_request(
                    connection,
                    request_id,
                    approver="worker:proposer",
                )

    def test_reject_vote_blocks_interpretation_activation(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(connection, subject_id)
            self.approve_interpretation_request(
                connection,
                request_id,
                approver="worker:reviewer",
                decision="REJECT",
            )
            with self.assertRaises(psycopg.Error):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (request_id, "worker:activator"),
                )

    def test_expired_request_cannot_activate(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_id = self.create_interpretation_request(
                connection,
                subject_id,
                requested_at="2020-01-01 00:00:00+00",
            )
            self.approve_interpretation_request(connection, request_id)
            with self.assertRaises(psycopg.Error):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (request_id, "worker:activator"),
                )

    def test_second_same_state_interpretation_is_rejected_as_noop(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            first = self.create_interpretation_request(connection, subject_id)
            self.approve_interpretation_request(connection, first)
            connection.execute(
                "SELECT apply_interpretation_decision(%s, %s)",
                (first, "worker:activator"),
            )

            second = self.create_interpretation_request(
                connection,
                subject_id,
                expected_version=1,
            )
            self.approve_interpretation_request(
                connection,
                second,
                approver="worker:approver-2",
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "SELECT apply_interpretation_decision(%s, %s)",
                    (second, "worker:activator"),
                )

    def test_competing_interpretation_requests_same_version_only_one_commits(self) -> None:
        with self.connection() as connection:
            subject_id = self.make_governable_observation(connection)
            request_ids = []
            for index in range(2):
                request_id = self.create_interpretation_request(
                    connection,
                    subject_id,
                    expected_version=0,
                    proposer=f"worker:proposer-{index}",
                )
                self.approve_interpretation_request(
                    connection,
                    request_id,
                    proposer=f"worker:proposer-{index}",
                    approver=f"worker:approver-{index}",
                )
                request_ids.append(request_id)

        def activate(request_id):
            try:
                with self.connection() as connection:
                    connection.execute(
                        "SELECT apply_interpretation_decision(%s, %s)",
                        (request_id, f"worker:activator-{request_id}"),
                    )
                return True
            except psycopg.Error:
                return False

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(activate, request_ids))
        self.assertEqual(sum(results), 1)

    def test_serving_control_noop_default_enabled_is_rejected(self) -> None:
        with self.connection() as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    SELECT apply_economic_serving_control(
                        'CPI_DOMAIN', NULL, 0, 'ENABLED', 'TEST',
                        'worker:operator', 'CASE-1', NULL, 'VERIFY-1'
                    )
                    """
                )

    def test_competing_serving_controls_same_version_only_one_commits(self) -> None:
        def withhold(actor):
            try:
                with self.connection() as connection:
                    connection.execute(
                        """
                        SELECT apply_economic_serving_control(
                            'CPI_DOMAIN', NULL, 0, 'WITHHELD', 'TEST',
                            %s, 'CASE-1', NULL, NULL
                        )
                        """,
                        (actor,),
                    )
                return True
            except psycopg.Error:
                return False

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(
                    withhold,
                    ("worker:operator-a", "worker:operator-b"),
                )
            )
        self.assertEqual(sum(results), 1)

    def test_event_reenable_requires_case_and_verified_fingerprint(self) -> None:
        with self.connection() as connection:
            event_id, _ = self.make_event(connection, "2026-06-01")
            connection.execute(
                """
                SELECT apply_economic_serving_control(
                    'EVENT_OCCURRENCE', %s, 0, 'WITHHELD', 'TEST',
                    'worker:operator', NULL, NULL, NULL
                )
                """,
                (event_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    SELECT apply_economic_serving_control(
                        'EVENT_OCCURRENCE', %s, 1, 'ENABLED', 'TEST',
                        'worker:operator', 'CASE-2', NULL, NULL
                    )
                    """,
                    (event_id,),
                )

    def test_abnormal_work_terminalization_requires_reason_code(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED',
                       claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            attempt_id = self.insert_attempt(connection, work_id)
            connection.execute(
                """
                UPDATE ingestion_attempts
                   SET state='TERMINAL', outcome='FAILED',
                       reason_code='TEST_FAILURE',
                       finished_at=CURRENT_TIMESTAMP
                 WHERE attempt_id=%s
                """,
                (attempt_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_work_items
                       SET state='TERMINAL', outcome='FAILED',
                           claim_token=NULL, lease_until=NULL
                     WHERE work_item_id=%s
                    """,
                    (work_id,),
                )

    def test_succeeded_work_rejects_reason_code(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED',
                       claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            attempt_id = self.insert_attempt(connection, work_id)
            connection.execute(
                """
                UPDATE ingestion_attempts
                   SET state='TERMINAL', outcome='SUCCEEDED',
                       finished_at=CURRENT_TIMESTAMP
                 WHERE attempt_id=%s
                """,
                (attempt_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_work_items
                       SET state='TERMINAL', outcome='SUCCEEDED',
                           reason_code='SHOULD_NOT_EXIST',
                           claim_token=NULL, lease_until=NULL
                     WHERE work_item_id=%s
                    """,
                    (work_id,),
                )

    def test_failed_attempt_requires_reason_code(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED',
                       claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            attempt_id = self.insert_attempt(connection, work_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_attempts
                       SET state='TERMINAL', outcome='FAILED',
                           finished_at=CURRENT_TIMESTAMP
                     WHERE attempt_id=%s
                    """,
                    (attempt_id,),
                )


    def test_work_update_refreshes_database_owned_updated_at(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            before = connection.execute(
                "SELECT updated_at FROM ingestion_work_items WHERE work_item_id=%s",
                (work_id,),
            ).fetchone()[0]
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET next_claim_at=CURRENT_TIMESTAMP + INTERVAL '1 minute',
                       updated_at='2000-01-01 00:00:00+00'
                 WHERE work_item_id=%s
                """,
                (work_id,),
            )
            after = connection.execute(
                "SELECT updated_at FROM ingestion_work_items WHERE work_item_id=%s",
                (work_id,),
            ).fetchone()[0]
        self.assertGreaterEqual(after, before)
        self.assertNotEqual(after.year, 2000)


    def test_executed_work_cannot_terminalize_without_matching_attempt(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED', claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_work_items
                       SET state='TERMINAL', outcome='FAILED',
                           reason_code='NO_ATTEMPT',
                           claim_token=NULL, lease_until=NULL
                     WHERE work_item_id=%s
                    """,
                    (work_id,),
                )

    def test_claim_token_is_immutable_within_generation(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            token = uuid4()
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED', claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (token, work_id),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "UPDATE ingestion_work_items SET claim_token=%s WHERE work_item_id=%s",
                    (uuid4(), work_id),
                )

    def test_run_outcome_must_match_terminal_work_aggregation(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            connection.execute(
                """
                UPDATE ingestion_attempts
                   SET state='TERMINAL', outcome='FAILED',
                       reason_code='TEST_FAILURE',
                       finished_at=CURRENT_TIMESTAMP
                 WHERE attempt_id=%s
                """,
                (attempt_id,),
            )
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='TERMINAL', outcome='FAILED',
                       reason_code='TEST_FAILURE',
                       claim_token=NULL, lease_until=NULL
                 WHERE work_item_id=%s
                """,
                (work_id,),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE ingestion_runs
                       SET state='TERMINAL', outcome='SUCCEEDED',
                           finished_at=CURRENT_TIMESTAMP
                     WHERE run_id=%s
                    """,
                    (run_id,),
                )

    def test_run_work_and_attempt_identity_fields_are_immutable(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "UPDATE ingestion_runs SET job_type='CHANGED' WHERE run_id=%s",
                    (run_id,),
                )
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "UPDATE ingestion_work_items SET work_key='changed' WHERE work_item_id=%s",
                    (work_id,),
                )
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            attempt_id = self.insert_attempt(connection, work_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    "UPDATE ingestion_attempts SET attempt_number=2 WHERE attempt_id=%s",
                    (attempt_id,),
                )

    def test_repository_claim_starts_run_and_creates_aligned_attempt(self) -> None:
        repository = CpiW1Repository()
        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"CPI_RELEASE_ENVELOPE_PROMOTE:{uuid4()}",
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            self.assertIsNotNone(claim)
            self.assertEqual(claim.work_item_id, work_id)
            self.assertEqual(claim.claim_generation, 1)
            run = connection.execute(
                "SELECT state, started_at FROM ingestion_runs WHERE run_id=%s",
                (run_id,),
            ).fetchone()
            self.assertEqual(run[0], "RUNNING")
            self.assertIsNotNone(run[1])
            attempt = connection.execute(
                """
                SELECT attempt_number, state
                  FROM ingestion_attempts
                 WHERE attempt_id=%s
                """,
                (claim.attempt_id,),
            ).fetchone()
            self.assertEqual(attempt, (1, "RUNNING"))

    def test_repository_reclaim_fences_old_claim(self) -> None:
        repository = CpiW1Repository()
        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"CPI_RELEASE_ENVELOPE_PROMOTE:{uuid4()}",
            )
            first = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                lease_seconds=1,
            )
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second'
                 WHERE work_item_id=%s
                """,
                (work_id,),
            )
            second = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            self.assertEqual(second.claim_generation, 2)
            self.assertNotEqual(first.claim_token, second.claim_token)
            with self.assertRaises(StaleClaimError):
                repository.terminalize_claim(
                    connection,
                    first,
                    outcome="FAILED",
                    reason_code="STALE_OWNER",
                )

    def test_reclaim_closes_expired_attempt_and_retry_preserves_history(self) -> None:
        repository = CpiW1Repository()
        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"CPI_RELEASE_ENVELOPE_PROMOTE:{uuid4()}",
            )
            first = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                lease_seconds=1,
            )
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second'
                 WHERE work_item_id=%s
                """,
                (work_id,),
            )
            second = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            old_attempt = connection.execute(
                """
                SELECT state, outcome, reason_code
                  FROM ingestion_attempts
                 WHERE attempt_id=%s
                """,
                (first.attempt_id,),
            ).fetchone()
            self.assertEqual(
                old_attempt,
                ("TERMINAL", "FAILED", "LEASE_EXPIRED_RECLAIM"),
            )
            repository.retry_claim(
                connection,
                second,
                reason_code="TRANSIENT_DEPENDENCY",
                retry_after_seconds=60,
            )
            pending = connection.execute(
                """
                SELECT state, outcome, reason_code, claim_generation,
                       claim_token, lease_until
                  FROM ingestion_work_items
                 WHERE work_item_id=%s
                """,
                (work_id,),
            ).fetchone()
            self.assertEqual(pending[:4], ("PENDING", None, None, 2))
            self.assertIsNone(pending[4])
            self.assertIsNone(pending[5])
            second_attempt = connection.execute(
                """
                SELECT state, outcome, reason_code
                  FROM ingestion_attempts
                 WHERE attempt_id=%s
                """,
                (second.attempt_id,),
            ).fetchone()
            self.assertEqual(
                second_attempt,
                ("TERMINAL", "FAILED", "TRANSIENT_DEPENDENCY"),
            )

    def test_artifact_same_identity_rejects_metadata_drift(self) -> None:
        repository = CpiW1Repository()
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(
                connection,
                run_id,
                work_key=f"collect:artifact-idempotency:{uuid4()}",
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_COLLECT",
            )
            captured_at = connection.execute(
                "SELECT CURRENT_TIMESTAMP"
            ).fetchone()[0]
            artifact_id = repository.record_source_artifact(
                connection,
                claim,
                source_code="BLS",
                artifact_contract_kind="CPI_RELEASE_HTML",
                source_contract_version="bls-cpi-source-v1",
                locator_key="CURRENT_CPI_RELEASE_HTML",
                content_sha256=digest("same-material"),
                content_type="text/html",
                captured_at=captured_at,
                retrieval_url="https://www.bls.gov/news.release/cpi.nr0.htm",
            )
            same_id = repository.record_source_artifact(
                connection,
                claim,
                source_code="BLS",
                artifact_contract_kind="CPI_RELEASE_HTML",
                source_contract_version="bls-cpi-source-v1",
                locator_key="CURRENT_CPI_RELEASE_HTML",
                content_sha256=digest("same-material"),
                content_type="text/html",
                captured_at=captured_at,
                retrieval_url="https://www.bls.gov/news.release/cpi.nr0.htm",
            )
            self.assertEqual(same_id, artifact_id)
            with self.assertRaises(Exception):
                repository.record_source_artifact(
                    connection,
                    claim,
                    source_code="BLS",
                    artifact_contract_kind="CPI_RELEASE_HTML",
                    source_contract_version="bls-cpi-source-v2",
                    locator_key="CURRENT_CPI_RELEASE_HTML",
                    content_sha256=digest("same-material"),
                    content_type="text/html",
                    captured_at=captured_at,
                    retrieval_url="https://www.bls.gov/news.release/cpi.nr0.htm",
                )

    def test_retry_delay_uses_database_clock_and_rejects_unbounded_delay(self) -> None:
        repository = CpiW1Repository()
        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"CPI_RELEASE_ENVELOPE_PROMOTE:{uuid4()}",
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            before = connection.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
            repository.retry_claim(
                connection,
                claim,
                reason_code="TRANSIENT_DEPENDENCY",
                retry_after_seconds=60,
            )
            next_claim_at = connection.execute(
                "SELECT next_claim_at FROM ingestion_work_items WHERE work_item_id=%s",
                (work_id,),
            ).fetchone()[0]
            self.assertGreaterEqual(next_claim_at, before)
            self.assertLessEqual(
                (next_claim_at - before).total_seconds(),
                61,
            )

        with self.connection() as connection:
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            self.insert_work(
                connection,
                run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=f"CPI_RELEASE_ENVELOPE_PROMOTE:{uuid4()}",
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            with self.assertRaises(ValueError):
                repository.retry_claim(
                    connection,
                    claim,
                    reason_code="TRANSIENT_DEPENDENCY",
                    retry_after_seconds=86401,
                )

    def test_release_envelope_promotion_is_atomic_and_observation_free(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        candidate = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:envelope:{uuid4()}")
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            work_id = uuid4()
            work_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                candidate.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (work_id, run_id, work_key, artifact_id),
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
            )
            result = promoter.promote_release_envelope(
                connection,
                claim,
                artifact_id=artifact_id,
                candidate=candidate,
            )
            self.assertIsNotNone(result.disclosure_link_id)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM official_observation_assertions"
                ).fetchone()[0],
                0,
            )
            state = connection.execute(
                """
                SELECT state, outcome
                  FROM ingestion_work_items
                 WHERE work_item_id=%s
                """,
                (work_id,),
            ).fetchone()
            self.assertEqual(state, ("TERMINAL", "SUCCEEDED"))

    def test_split_promotion_allows_disclosed_envelope_and_quarantined_core4(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        envelope = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:split:{uuid4()}")
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            envelope_work = uuid4()
            observation_work = uuid4()
            envelope_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            observation_key = promotion_work_key(
                PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES
                    (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s),
                    (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (
                    envelope_work,
                    run_id,
                    envelope_key,
                    artifact_id,
                    observation_work,
                    run_id,
                    observation_key,
                    artifact_id,
                ),
            )
            envelope_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
            )
            promoter.promote_release_envelope(
                connection,
                envelope_claim,
                artifact_id=artifact_id,
                candidate=envelope,
            )
            observation_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
            )
            promoter.quarantine_observation_work(
                connection,
                observation_claim,
                reason_code="UNSAFE_CORE4_MAPPING",
            )

            outcomes = dict(
                connection.execute(
                    """
                    SELECT work_key, outcome
                      FROM ingestion_work_items
                     WHERE run_id=%s
                    """,
                    (run_id,),
                ).fetchall()
            )
            self.assertEqual(
                outcomes[envelope_key],
                "SUCCEEDED",
            )
            self.assertEqual(
                outcomes[observation_key],
                "QUARANTINED",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM event_disclosure_links WHERE relation_kind='EVENT_RELEASE'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM official_observation_assertions"
                ).fetchone()[0],
                0,
            )

    def test_core4_bundle_promotes_four_assertions_atomically(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        envelope = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        bundle = extract_core4_from_release_html(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:core4:{uuid4()}")
            envelope_run = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            envelope_work = uuid4()
            envelope_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (
                    envelope_work,
                    envelope_run,
                    envelope_key,
                    artifact_id,
                ),
            )
            envelope_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
            )
            promoter.promote_release_envelope(
                connection,
                envelope_claim,
                artifact_id=artifact_id,
                candidate=envelope,
            )

            observation_run = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            observation_work = uuid4()
            observation_key = promotion_work_key(
                PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                artifact_id,
                bundle.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (
                    observation_work,
                    observation_run,
                    observation_key,
                    artifact_id,
                ),
            )
            observation_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
            )
            result = promoter.promote_observation_bundle(
                connection,
                observation_claim,
                artifact_id=artifact_id,
                candidate=bundle,
            )
            self.assertEqual(result.verified_observation_count, 4)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM official_observation_assertions"
                ).fetchone()[0],
                4,
            )

    def test_promotion_family_claim_cannot_be_swapped(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        envelope = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:wrong-family:{uuid4()}")
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            wrong_key = promotion_work_key(
                PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (uuid4(), run_id, wrong_key, artifact_id),
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
            )
            with self.assertRaises(Exception):
                promoter.promote_release_envelope(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    candidate=envelope,
                )

    def test_release_envelope_retry_converges_without_duplicate_evidence(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        candidate = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:retry:{uuid4()}")
            work_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                candidate.extractor_contract_version,
            )
            for _ in range(2):
                run_id = self.insert_run(
                    connection,
                    execution_scope="ECONOMIC_PROMOTE",
                )
                connection.execute(
                    """
                    INSERT INTO ingestion_work_items (
                        work_item_id, run_id, execution_scope, data_domain,
                        work_key, input_artifact_id
                    ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                    """,
                    (uuid4(), run_id, work_key, artifact_id),
                )
                claim = repository.claim_work_item(
                    connection,
                    execution_scope="ECONOMIC_PROMOTE",
                    work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
                )
                promoter.promote_release_envelope(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    candidate=candidate,
                )

            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM core_event_occurrences"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM event_disclosure_links"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM event_disclosure_artifacts"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM disclosure_marker_assertions"
                ).fetchone()[0],
                1,
            )
            orphan_subjects = connection.execute(
                """
                SELECT COUNT(*)
                  FROM interpretation_subjects s
                 WHERE NOT EXISTS (
                    SELECT 1 FROM event_disclosure_links l
                     WHERE l.disclosure_link_id=s.subject_id
                 )
                   AND NOT EXISTS (
                    SELECT 1 FROM event_disclosure_artifacts a
                     WHERE a.disclosure_artifact_link_id=s.subject_id
                 )
                   AND NOT EXISTS (
                    SELECT 1 FROM disclosure_marker_assertions m
                     WHERE m.marker_assertion_id=s.subject_id
                 )
                   AND NOT EXISTS (
                    SELECT 1 FROM official_observation_assertions o
                     WHERE o.assertion_id=s.subject_id
                 )
                   AND NOT EXISTS (
                    SELECT 1 FROM event_schedule_assertions q
                     WHERE q.schedule_assertion_id=s.subject_id
                 )
                """
            ).fetchone()[0]
            self.assertEqual(orphan_subjects, 0)

    def test_same_observation_parse_identity_with_different_material_fails(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        envelope = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        bundle = extract_core4_from_release_html(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:determinism:{uuid4()}")

            envelope_run = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            envelope_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (uuid4(), envelope_run, envelope_key, artifact_id),
            )
            envelope_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
            )
            promoter.promote_release_envelope(
                connection,
                envelope_claim,
                artifact_id=artifact_id,
                candidate=envelope,
            )

            def promote_bundle(candidate):
                run_id = self.insert_run(
                    connection,
                    execution_scope="ECONOMIC_PROMOTE",
                )
                key = promotion_work_key(
                    PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                    artifact_id,
                    candidate.extractor_contract_version,
                )
                connection.execute(
                    """
                    INSERT INTO ingestion_work_items (
                        work_item_id, run_id, execution_scope, data_domain,
                        work_key, input_artifact_id
                    ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                    """,
                    (uuid4(), run_id, key, artifact_id),
                )
                claim = repository.claim_work_item(
                    connection,
                    execution_scope="ECONOMIC_PROMOTE",
                    work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
                )
                return claim, promoter.promote_observation_bundle(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    candidate=candidate,
                )

            promote_bundle(bundle)
            changed = []
            for item in bundle.observations:
                if item.observation_code == "CPI_HEADLINE_MOM":
                    changed.append(
                        ObservationCandidate(
                            material=ObservationMaterial(
                                observation_code=item.observation_code,
                                assertion_state=ObservationState.VALUE,
                                normalized_value=Decimal("9.9"),
                            ),
                            source_value_text="9.9",
                        )
                    )
                else:
                    changed.append(item)
            conflicting = ObservationBundleCandidate(
                reference_month=bundle.reference_month,
                observations=tuple(changed),
                extractor_contract_version=bundle.extractor_contract_version,
            )

            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            key = promotion_work_key(
                PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                artifact_id,
                conflicting.extractor_contract_version,
            )
            work_id = uuid4()
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (work_id, run_id, key, artifact_id),
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
            )
            with self.assertRaises(PromotionDeterminismError):
                promoter.promote_observation_bundle(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    candidate=conflicting,
                )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM official_observation_assertions"
                ).fetchone()[0],
                4,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT state, outcome FROM ingestion_work_items WHERE work_item_id=%s",
                    (work_id,),
                ).fetchone(),
                ("CLAIMED", None),
            )

    def test_core4_db_failure_rolls_back_all_partial_assertions(self) -> None:
        repository = CpiW1Repository()
        promoter = CpiW1Promoter(repository)
        fixture = Path("tests/fixtures/cpi_w1/html/normal_aug_2026.html").read_bytes()
        envelope = extract_release_envelope(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        bundle = extract_core4_from_release_html(
            fixture,
            expected_reference_month=date(2026, 8, 1),
        )
        with self.connection() as connection:
            artifact_id = self.make_artifact(connection, f"release:rollback:{uuid4()}")
            envelope_run = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            envelope_key = promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                envelope.extractor_contract_version,
            )
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (uuid4(), envelope_run, envelope_key, artifact_id),
            )
            envelope_claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE.value + ":",
            )
            promoter.promote_release_envelope(
                connection,
                envelope_claim,
                artifact_id=artifact_id,
                candidate=envelope,
            )

            invalid_items = list(bundle.observations)
            last = invalid_items[-1]
            invalid_items[-1] = ObservationCandidate(
                material=ObservationMaterial(
                    observation_code=last.observation_code,
                    assertion_state=ObservationState.EXPLICIT_UNAVAILABLE,
                    normalized_value=None,
                ),
                source_value_text="-",
                source_reason_text=None,
            )
            invalid_bundle = ObservationBundleCandidate(
                reference_month=bundle.reference_month,
                observations=tuple(invalid_items),
                extractor_contract_version=bundle.extractor_contract_version,
            )
            run_id = self.insert_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
            )
            key = promotion_work_key(
                PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
                artifact_id,
                invalid_bundle.extractor_contract_version,
            )
            work_id = uuid4()
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id
                ) VALUES (%s, %s, 'ECONOMIC_PROMOTE', 'ECONOMIC', %s, %s)
                """,
                (work_id, run_id, key, artifact_id),
            )
            claim = repository.claim_work_item(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                work_key_prefix=PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":",
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                promoter.promote_observation_bundle(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    candidate=invalid_bundle,
                )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM official_observation_assertions"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT state, outcome FROM ingestion_work_items WHERE work_item_id=%s",
                    (work_id,),
                ).fetchone(),
                ("CLAIMED", None),
            )

    def test_canceled_schedule_cannot_carry_scheduled_fields(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            artifact_id = self.make_artifact(connection, "schedule:canceled-invalid")
            assertion_id = uuid4()
            self.insert_subject(connection, assertion_id, "SCHEDULE_ASSERTION")
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO event_schedule_assertions (
                        schedule_assertion_id, event_occurrence_id, schedule_status,
                        scheduled_date, scheduled_at, schedule_timezone, time_precision,
                        source_code, source_artifact_id, extractor_contract_version,
                        accepted_by_attempt_id, accepted_at, material_fingerprint
                    ) VALUES (
                        %s, %s, 'CANCELED', '2026-09-11', NULL,
                        'America/New_York', NULL, 'BLS', %s,
                        'schedule-v1', %s, CURRENT_TIMESTAMP, %s
                    )
                    """,
                    (
                        assertion_id,
                        event_id,
                        artifact_id,
                        promote_attempt,
                        digest("canceled-invalid"),
                    ),
                )

    def test_date_pending_schedule_has_no_synthetic_schedule_fields(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            artifact_id = self.make_artifact(connection, "schedule:pending")
            assertion_id = uuid4()
            self.insert_subject(connection, assertion_id, "SCHEDULE_ASSERTION")
            connection.execute(
                """
                INSERT INTO event_schedule_assertions (
                    schedule_assertion_id, event_occurrence_id, schedule_status,
                    scheduled_date, scheduled_at, schedule_timezone, time_precision,
                    source_code, source_artifact_id, extractor_contract_version,
                    accepted_by_attempt_id, accepted_at, material_fingerprint
                ) VALUES (
                    %s, %s, 'DATE_PENDING', NULL, NULL, NULL, NULL,
                    'BLS', %s, 'schedule-v1', %s, CURRENT_TIMESTAMP, %s
                )
                """,
                (
                    assertion_id,
                    event_id,
                    artifact_id,
                    promote_attempt,
                    digest("date-pending"),
                ),
            )

    def test_source_effective_exact_requires_timestamp(self) -> None:
        with self.connection() as connection:
            event_id, promote_attempt = self.make_event(connection)
            artifact_id = self.make_artifact(connection, "schedule:source-effective")
            assertion_id = uuid4()
            self.insert_subject(connection, assertion_id, "SCHEDULE_ASSERTION")
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO event_schedule_assertions (
                        schedule_assertion_id, event_occurrence_id, schedule_status,
                        scheduled_date, scheduled_at, schedule_timezone, time_precision,
                        source_effective_date, source_effective_at,
                        source_effective_precision,
                        source_code, source_artifact_id, extractor_contract_version,
                        accepted_by_attempt_id, accepted_at, material_fingerprint
                    ) VALUES (
                        %s, %s, 'SCHEDULED', '2026-09-11', NULL,
                        'America/New_York', 'DATE_ONLY',
                        '2026-08-01', NULL, 'EXACT',
                        'BLS', %s, 'schedule-v1', %s, CURRENT_TIMESTAMP, %s
                    )
                    """,
                    (
                        assertion_id,
                        event_id,
                        artifact_id,
                        promote_attempt,
                        digest("source-effective-invalid"),
                    ),
                )


    def test_attempt_requires_claimed_work_and_current_generation(self) -> None:
        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO ingestion_attempts (
                        attempt_id, work_item_id, execution_scope,
                        data_domain, attempt_number
                    ) VALUES (%s, %s, 'ECONOMIC_COLLECT', 'ECONOMIC', 1)
                    """,
                    (uuid4(), work_id),
                )

        with self.connection() as connection:
            run_id = self.insert_run(connection)
            work_id = self.insert_work(connection, run_id)
            connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED', claim_generation=1,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                 WHERE work_item_id=%s
                """,
                (uuid4(), work_id),
            )
            with self.assertRaises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO ingestion_attempts (
                        attempt_id, work_item_id, execution_scope,
                        data_domain, attempt_number
                    ) VALUES (%s, %s, 'ECONOMIC_COLLECT', 'ECONOMIC', 2)
                    """,
                    (uuid4(), work_id),
                )


if __name__ == "__main__":
    unittest.main()
