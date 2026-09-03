import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.pipeline_run_tracking import (
    PipelineCheck,
    PipelineRun,
    PipelineWorkItem,
    canonical_config_hash,
    redact_error_message,
)


class PipelineRunTrackingTest(unittest.TestCase):
    def test_migration_declares_run_work_and_check_business_keys(self) -> None:
        migration = Path("db/migrations/006_pipeline_runs.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("CREATE TABLE IF NOT EXISTS pipeline_runs", migration)
        self.assertIn("config_hash TEXT NOT NULL", migration)
        self.assertIn("CREATE TABLE IF NOT EXISTS pipeline_work_items", migration)
        self.assertIn(
            "PRIMARY KEY (pipeline_run_id, economic_event_id, symbol, stage)",
            migration,
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS pipeline_run_checks", migration)
        self.assertIn("alert_status TEXT NOT NULL", migration)

    def test_config_hash_is_stable_across_mapping_order(self) -> None:
        left = {"symbols": ["SPY", "TLT"], "feed": "sip"}
        right = {"feed": "sip", "symbols": ["SPY", "TLT"]}

        self.assertEqual(canonical_config_hash(left), canonical_config_hash(right))
        self.assertEqual(len(canonical_config_hash(left)), 64)

    def test_redacts_database_credentials_and_api_keys(self) -> None:
        message = (
            "failed postgresql://market:secret@localhost:55432/market "
            "https://example.test/data?api_key=fred-secret&symbol=SPY"
        )

        redacted = redact_error_message(message)

        self.assertNotIn("secret", redacted)
        self.assertNotIn("fred-secret", redacted)
        self.assertIn("[database-url-redacted]", redacted)
        self.assertIn("api_key=[redacted]", redacted)

    def test_records_are_serializable_database_contracts(self) -> None:
        now = datetime(2026, 9, 3, tzinfo=UTC)
        run = PipelineRun(
            pipeline_run_id="run-1",
            dag_id="market_context_backfill_pipeline",
            config={"symbols": ["SPY"]},
            config_hash="a" * 64,
            data_cutoff=now,
            code_version="abc123",
            status="RUNNING",
            started_at=now,
        )
        item = PipelineWorkItem(
            pipeline_run_id="run-1",
            economic_event_id="FOMC|2026-07|2026-07-29T18:00:00Z",
            symbol="SPY",
            stage="MARKET_CONTEXT",
            status="FAILED",
            attempt_count=1,
            error_code="HTTP_503",
            error_message="provider unavailable",
        )
        check = PipelineCheck(
            pipeline_run_id="run-1",
            economic_event_id=item.economic_event_id,
            symbol="SPY",
            stage="MARKET_CONTEXT",
            check_name="provider_request",
            expected_value="HTTP 200",
            actual_value="HTTP 503",
            status="FAIL",
            alert_status="OPEN",
            checked_at=now,
        )

        self.assertEqual(run.status, "RUNNING")
        self.assertEqual(item.error_code, "HTTP_503")
        self.assertEqual(check.alert_status, "OPEN")


if __name__ == "__main__":
    unittest.main()
