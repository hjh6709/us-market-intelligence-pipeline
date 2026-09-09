import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/009_event_session_foundations.sql")


class EventSessionFoundationMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 009 must be created")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_normalized_event_and_session_foundations(self) -> None:
        for table in (
            "economic_release_observations",
            "economic_consensus_snapshots",
            "economic_surprises",
            "trading_sessions",
            "economic_event_markers",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_observations_and_consensus_keep_point_in_time_provenance(self) -> None:
        for field in (
            "revision_number INTEGER NOT NULL",
            "observed_at TIMESTAMPTZ NOT NULL",
            "source_url TEXT NOT NULL",
            "payload_sha256 TEXT NOT NULL",
            "contributor_count INTEGER",
        ):
            self.assertIn(field, self.sql)

    def test_surprise_references_exact_inputs_and_algorithm_version(self) -> None:
        for field in (
            "actual_observation_id BIGINT NOT NULL",
            "consensus_snapshot_id BIGINT NOT NULL",
            "algorithm_version TEXT NOT NULL",
            "pipeline_run_id TEXT",
        ):
            self.assertIn(field, self.sql)

    def test_surprise_inputs_must_match_event_and_metric(self) -> None:
        for constraint in (
            "FOREIGN KEY (actual_observation_id, economic_event_id, metric_name)",
            "FOREIGN KEY (consensus_snapshot_id, economic_event_id, metric_name)",
        ):
            self.assertIn(constraint, self.sql)

    def test_point_in_time_facts_reject_update_and_delete(self) -> None:
        self.assertIn("FUNCTION reject_immutable_fact_mutation", self.sql)
        for table in (
            "economic_release_observations",
            "economic_consensus_snapshots",
            "economic_surprises",
        ):
            self.assertIn(f"ON {table}", self.sql)

    def test_sessions_preserve_calendar_lineage_and_early_close(self) -> None:
        for field in (
            "exchange TEXT NOT NULL",
            "session_date DATE NOT NULL",
            "opens_at TIMESTAMPTZ NOT NULL",
            "closes_at TIMESTAMPTZ NOT NULL",
            "is_early_close BOOLEAN NOT NULL",
            "exchange_timezone TEXT NOT NULL",
            "calendar_source TEXT NOT NULL",
            "calendar_version TEXT NOT NULL",
        ):
            self.assertIn(field, self.sql)


if __name__ == "__main__":
    unittest.main()
