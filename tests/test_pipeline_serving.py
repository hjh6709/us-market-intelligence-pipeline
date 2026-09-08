import unittest
from datetime import UTC, datetime

from src.pipeline_serving import PipelineRunRecord, PipelineServingService


class FakePipelineRepository:
    def list_runs(self, **_filters):
        return [
            PipelineRunRecord(
                pipeline_run_id="run-1",
                dag_id="market_context_backfill_pipeline",
                status="SUCCEEDED",
                started_at=datetime(2026, 9, 8, tzinfo=UTC),
                finished_at=datetime(2026, 9, 8, 0, 2, tzinfo=UTC),
                work_item_count=2020,
                failed_item_count=0,
                warning_check_count=1409,
                failed_check_count=0,
                open_alert_count=0,
                observed_coverage_check_count=0,
                input_count=None,
                output_count=None,
            )
        ]

    def get_run(self, _run_id):
        return None


class PipelineServingTest(unittest.TestCase):
    def test_overview_keeps_warning_and_failure_counts_distinct(self):
        result = PipelineServingService(FakePipelineRepository()).overview()

        self.assertEqual(result.latest_run.status, "SUCCEEDED")
        self.assertEqual(result.latest_run.warning_check_count, 1409)
        self.assertEqual(result.latest_run.failed_check_count, 0)
        self.assertEqual(result.latest_run.open_alert_count, 0)
        self.assertIsNone(result.latest_run.input_count)
        self.assertEqual(result.latest_run.duration_seconds, 120)
        self.assertEqual(result.latest_run.quality_contract, "legacy-pre-separation")

    def test_lineage_keeps_research_and_raw_validation_paths_separate(self):
        result = PipelineServingService(FakePipelineRepository()).lineage()

        edge_pairs = {(edge.source, edge.target) for edge in result.edges}
        self.assertIn(("alpaca_bars", "market_bars"), edge_pairs)
        self.assertIn(("archived_sip_trades", "kafka"), edge_pairs)
        self.assertNotIn(("spark", "macro_event_impacts"), edge_pairs)


if __name__ == "__main__":
    unittest.main()
