import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/010_cpi_w1_ingestion_subjects.sql")


class CpiW1IngestionMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 010 must exist")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_six_foundation_tables(self) -> None:
        for table in (
            "data_sources",
            "ingestion_runs",
            "ingestion_work_items",
            "ingestion_attempts",
            "source_artifacts",
            "interpretation_subjects",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_declares_required_state_and_provenance_vocabulary(self) -> None:
        for token in (
            "'LIVE'", "'BACKFILL'", "'REPLAY'",
            "'CREATED'", "'RUNNING'", "'TERMINAL'",
            "'PENDING'", "'CLAIMED'", "'QUARANTINED'", "'DATA_NOT_AVAILABLE'", "'SKIPPED'",
            "claim_generation", "claim_token", "lease_until",
            "source_contract_version", "content_sha256",
            "'RETAINED'", "'NOT_RETAINED'", "'DELETED_BY_POLICY'",
        ):
            self.assertIn(token, self.sql)

    def test_scope_and_domain_are_propagated_with_composite_foreign_keys(self) -> None:
        for fragment in (
            "UNIQUE (run_id, execution_scope, data_domain)",
            "FOREIGN KEY (run_id, execution_scope, data_domain)",
            "UNIQUE (work_item_id, execution_scope, data_domain)",
            "FOREIGN KEY (work_item_id, execution_scope, data_domain)",
            "UNIQUE (attempt_id, execution_scope, data_domain)",
            "FOREIGN KEY (created_by_attempt_id, created_by_execution_scope, data_domain)",
            "FOREIGN KEY (input_artifact_id, data_domain)",
        ):
            self.assertIn(fragment, self.sql)

    def test_run_and_attempt_state_timestamp_invariants_are_structural(self) -> None:
        self.assertIn("ingestion_runs_state_timestamps_valid", self.sql)
        self.assertIn("ingestion_attempts_state_timestamps_valid", self.sql)
        self.assertIn("ingestion_work_items_state_claim_valid", self.sql)
        self.assertIn("cannot terminalize ingestion run with non-terminal work", self.sql)
        self.assertIn("terminal ingestion run cannot receive new work", self.sql)

    def test_artifact_identity_and_retention_are_explicit(self) -> None:
        self.assertIn(
            "UNIQUE (created_by_attempt_id, locator_key, content_sha256)", self.sql
        )
        self.assertIn("source_artifacts_retention_valid", self.sql)
        self.assertIn("storage_generation IS NOT NULL", self.sql)
        self.assertIn("storage_generation IS NULL", self.sql)

    def test_replay_and_scheduled_delivery_have_durable_identity(self) -> None:
        self.assertIn("ingestion_runs_replay_reference_valid", self.sql)
        self.assertIn("ingestion_runs_trigger_idempotency", self.sql)
        self.assertIn(
            "data_domain, execution_scope, job_type, trigger_type, trigger_idempotency_key",
            self.sql,
        )

    def test_interpretation_subject_registry_is_narrowly_typed(self) -> None:
        for subject_type in (
            "SCHEDULE_ASSERTION",
            "EVENT_DISCLOSURE_LINK",
            "DISCLOSURE_ARTIFACT_LINK",
            "DISCLOSURE_MARKER_ASSERTION",
            "OFFICIAL_OBSERVATION_ASSERTION",
        ):
            self.assertIn(subject_type, self.sql)
        self.assertIn("UNIQUE (subject_id, subject_type)", self.sql)

    def test_bls_source_is_migration_owned(self) -> None:
        self.assertIn("VALUES ('BLS', 'U.S. Bureau of Labor Statistics')", self.sql)
        self.assertIn("ON CONFLICT (source_code) DO NOTHING", self.sql)

    def test_keeps_new_ingestion_model_separate_from_legacy_pipeline_telemetry(self) -> None:
        self.assertNotIn("ALTER TABLE pipeline_", self.sql)
        self.assertNotIn("UPDATE pipeline_", self.sql)
        self.assertNotIn("INSERT INTO pipeline_", self.sql)


if __name__ == "__main__":
    unittest.main()
