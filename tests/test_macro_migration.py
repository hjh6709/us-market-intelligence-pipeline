import unittest
from pathlib import Path


class MacroMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.migration = Path(
            "db/migrations/002_macro_event_analysis.sql"
        ).read_text(encoding="utf-8")

    def test_declares_point_in_time_and_event_study_tables(self) -> None:
        for table in (
            "macro_series",
            "macro_observations",
            "economic_events",
            "macro_event_impacts",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.migration)

    def test_keeps_release_time_vintage_and_analysis_version(self) -> None:
        for field in (
            "released_at TIMESTAMPTZ NOT NULL",
            "realtime_start DATE NOT NULL",
            "realtime_end DATE NOT NULL",
            "vintage_as_of DATE",
            "analysis_version TEXT NOT NULL",
            "coverage_status TEXT NOT NULL",
        ):
            self.assertIn(field, self.migration)

    def test_does_not_require_unverified_consensus_forecast(self) -> None:
        self.assertIn("forecast NUMERIC", self.migration)
        self.assertNotIn("forecast NUMERIC NOT NULL", self.migration)

    def test_matched_baseline_migration_preserves_controls_and_comparison(self) -> None:
        migration = Path("db/migrations/003_matched_baseline.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("macro_event_baseline_impacts", migration)
        self.assertIn("control_offset_weeks", migration)
        self.assertIn("matched_baseline_return_pct", migration)
        self.assertIn("return_vs_matched_baseline_pct", migration)
        self.assertIn("baseline_sample_size", migration)

    def test_pipeline_experiment_migration_tracks_event_context(self) -> None:
        migration = Path("db/migrations/004_pipeline_experiments.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("CREATE TABLE IF NOT EXISTS macro_event_contexts", migration)
        self.assertIn("PRIMARY KEY (economic_event_id, series_id)", migration)

    def test_pipeline_run_migration_tracks_alert_state(self) -> None:
        migration = Path("db/migrations/006_pipeline_runs.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("pipeline_runs", migration)
        self.assertIn("pipeline_work_items", migration)
        self.assertIn("pipeline_run_checks", migration)
        self.assertIn("'NONE','OPEN','RESOLVED'", migration.replace(" ", ""))

    def test_strategy_migration_tracks_cost_and_coverage(self) -> None:
        migration = Path("db/migrations/007_event_strategy_results.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("CREATE TABLE IF NOT EXISTS event_strategy_results", migration)
        self.assertIn("transaction_cost_bps", migration)
        self.assertIn("coverage_status", migration)
        self.assertIn("strategy_version", migration)


if __name__ == "__main__":
    unittest.main()
