import hashlib
import os
import unittest
from pathlib import Path
from uuid import uuid4

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


if __name__ == "__main__":
    unittest.main()
