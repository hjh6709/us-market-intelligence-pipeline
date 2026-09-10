import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/009_event_session_foundations.sql")


class EventSessionFoundationMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 009 must be created")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_normalized_event_and_session_foundations(self) -> None:
        for table in (
            "canonical_economic_events",
            "economic_event_lifecycle_versions",
            "economic_observation_registry",
            "economic_release_observations",
            "economic_consensus_snapshots",
            "economic_surprises",
            "trading_sessions",
            "economic_event_markers",
            "calendar_snapshots",
            "validation_runs",
            "validation_reconstructed_bars",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_event_lifecycle_supports_upcoming_and_versioned_changes(self) -> None:
        for field in (
            "scheduled_at TIMESTAMPTZ NOT NULL",
            "released_at TIMESTAMPTZ",
            "event_status TEXT NOT NULL",
            "lifecycle_version INTEGER NOT NULL",
            "legacy_economic_event_id TEXT",
        ):
            self.assertIn(field, self.sql)

    def test_observations_and_consensus_use_canonical_identity_and_timestamps(self) -> None:
        for field in (
            "observation_code TEXT NOT NULL",
            "revision_number INTEGER NOT NULL",
            "revision_type TEXT NOT NULL",
            "source_revision_id TEXT",
            "published_at TIMESTAMPTZ NOT NULL",
            "first_observed_at TIMESTAMPTZ NOT NULL",
            "snapshot_at TIMESTAMPTZ NOT NULL",
            "provider_updated_at TIMESTAMPTZ",
            "source_url TEXT NOT NULL",
            "payload_sha256 TEXT NOT NULL",
            "contributor_count INTEGER",
        ):
            self.assertIn(field, self.sql)

        self.assertNotIn("metric_name TEXT NOT NULL", self.sql)

    def test_surprise_references_exact_inputs_unit_and_algorithm_version(self) -> None:
        for field in (
            "actual_observation_id BIGINT NOT NULL",
            "consensus_snapshot_id BIGINT NOT NULL",
            "unit TEXT NOT NULL",
            "algorithm_version TEXT NOT NULL",
            "pipeline_run_id TEXT",
        ):
            self.assertIn(field, self.sql)

    def test_surprise_inputs_must_match_event_code_and_unit(self) -> None:
        for constraint in (
            "FOREIGN KEY (actual_observation_id, economic_event_id, observation_code, unit)",
            "FOREIGN KEY (consensus_snapshot_id, economic_event_id, observation_code, unit)",
        ):
            self.assertIn(constraint, self.sql)

        self.assertIn("FUNCTION enforce_canonical_surprise", self.sql)
        self.assertIn("FUNCTION select_canonical_prerelease_consensus", self.sql)

    def test_observation_write_boundary_defines_idempotency_and_conflict(self) -> None:
        self.assertIn("FUNCTION record_economic_release_observation", self.sql)
        self.assertIn("CONFLICTING_SOURCE_FACT", self.sql)

    def test_point_in_time_facts_reject_update_and_delete(self) -> None:
        self.assertIn("FUNCTION reject_immutable_fact_mutation", self.sql)
        for table in (
            "economic_release_observations",
            "economic_consensus_snapshots",
            "economic_surprises",
        ):
            self.assertIn(f"ON {table}", self.sql)

    def test_sessions_preserve_trusted_market_calendar_generation(self) -> None:
        for field in (
            "market_code TEXT NOT NULL",
            "session_date DATE NOT NULL",
            "opens_at TIMESTAMPTZ NOT NULL",
            "closes_at TIMESTAMPTZ NOT NULL",
            "session_day_type TEXT NOT NULL",
            "exchange_timezone TEXT NOT NULL",
            "calendar_source TEXT NOT NULL",
            "calendar_snapshot_id TEXT NOT NULL",
        ):
            self.assertIn(field, self.sql)

    def test_markers_have_durable_revision_identity_and_current_primary_rule(self) -> None:
        for field in (
            "economic_event_marker_id TEXT PRIMARY KEY",
            "marker_kind TEXT NOT NULL",
            "marker_role TEXT NOT NULL",
            "marker_revision INTEGER NOT NULL",
        ):
            self.assertIn(field, self.sql)
        self.assertIn("VIEW current_economic_event_markers", self.sql)
        self.assertIn("FUNCTION enforce_event_marker_revision_chain", self.sql)
        self.assertIn("different current primary marker", self.sql)

    def test_validation_bars_are_physically_separate_from_research_bars(self) -> None:
        for field in ("workload_id TEXT NOT NULL", "processor_version TEXT NOT NULL", "checkpoint_namespace TEXT NOT NULL"):
            self.assertIn(field, self.sql)
        self.assertIn("validation_run_id TEXT NOT NULL REFERENCES validation_runs", self.sql)
        self.assertIn("FUNCTION record_validation_run", self.sql)
        self.assertIn("FUNCTION record_validation_reconstructed_bar", self.sql)
        self.assertNotIn("ON CONFLICT (\n    validation_run_id", self.sql)
        self.assertIn("CREATE OR REPLACE VIEW market_bar_origin_comparison", self.sql)


if __name__ == "__main__":
    unittest.main()
