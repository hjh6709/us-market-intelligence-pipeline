import argparse
import unittest
from pathlib import Path

from scripts.collect_cpi_w1 import build_parser, validate_args


SOURCE = Path("scripts/collect_cpi_w1.py").read_text(encoding="utf-8")


class CollectCpiW1Test(unittest.TestCase):
    def parse(self, *args: str) -> argparse.Namespace:
        return build_parser().parse_args(args)

    def test_cli_supports_live_backfill_replay_and_dry_run(self) -> None:
        for mode in ("live", "backfill", "replay"):
            args = self.parse("--mode", mode, "--dry-run")
            self.assertEqual(args.mode, mode)
        self.assertTrue(self.parse("--mode", "live", "--dry-run").dry_run)

    def test_dry_run_returns_before_database_connect(self) -> None:
        dry_pos = SOURCE.index("if args.dry_run:")
        connect_pos = SOURCE.index("psycopg.connect")
        self.assertLess(dry_pos, connect_pos)

    def test_replay_requires_artifact_and_extractor_identity(self) -> None:
        with self.assertRaises(SystemExit):
            validate_args(self.parse("--mode", "replay", "--reconcile-promotions"))
        validate_args(
            self.parse(
                "--mode", "replay",
                "--reconcile-promotions",
                "--replay-artifact-id", "00000000-0000-0000-0000-000000000001",
                "--replay-extractor-version", "bls-cpi-release-html-v1",
            )
        )

    def test_replay_arguments_are_used_to_scope_reconciliation(self) -> None:
        self.assertIn("only_artifact_id", SOURCE)
        self.assertIn("only_extractor_version", SOURCE)
        self.assertIn("replay extractor does not match artifact contract kind", SOURCE)

    def test_collector_does_not_import_legacy_cpi_upsert(self) -> None:
        self.assertNotIn("upsert_cpi_data", SOURCE)
        self.assertNotIn("src.cpi_ingestion", SOURCE)

    def test_collection_work_key_is_bound_to_exact_run(self) -> None:
        self.assertIn('f"COLLECT:{locator_key}:{run_id}"', SOURCE)

    def test_collection_and_promotion_are_separate(self) -> None:
        self.assertIn("record_source_artifact", SOURCE)
        self.assertIn("_schedule_promotions", SOURCE)
        self.assertNotIn("promote_release_envelope(", SOURCE)
        self.assertNotIn("promote_observation_bundle(", SOURCE)

    def test_artifact_record_and_work_success_share_database_transaction(self) -> None:
        record_pos = SOURCE.index("db_artifact_id = self.repository.record_source_artifact")
        terminal_pos = SOURCE.index("terminalize_claim_in_transaction", record_pos)
        transaction_pos = SOURCE.rfind("with connection.transaction():", 0, record_pos)
        self.assertLess(transaction_pos, record_pos)
        self.assertLess(record_pos, terminal_pos)
        self.assertIn("if not database_committed:", SOURCE)

    def test_release_gate_blocks_unapproved_promotion_without_blocking_capture(self) -> None:
        record_pos = SOURCE.index("record_source_artifact")
        gate_pos = SOURCE.index("self.release_gate.decision")
        self.assertLess(record_pos, SOURCE.rindex("_schedule_promotions"))
        self.assertIn("BLOCKED_BY_RELEASE_GATE", SOURCE)
        self.assertIn("OFFICIAL_CORPUS_NOT_READY", Path(
            "config/cpi_w1_extractor_release_gate.json"
        ).read_text(encoding="utf-8"))

    def test_release_gate_block_is_durable_audit_evidence(self) -> None:
        self.assertIn("record_promotion_deferred", SOURCE)
        self.assertIn("BLOCKED_BY_RELEASE_GATE", SOURCE)

    def test_missing_source_is_data_not_available_not_canceled(self) -> None:
        self.assertIn('outcome="DATA_NOT_AVAILABLE"', SOURCE)
        self.assertIn('reason_code="SOURCE_NOT_FOUND"', SOURCE)
        self.assertNotIn('outcome="CANCELED"', SOURCE)

    def test_backfill_has_no_customer_side_effect_surface(self) -> None:
        self.assertNotIn("notify", SOURCE.lower())
        self.assertNotIn("webhook", SOURCE.lower())
        self.assertNotIn("trade", SOURCE.lower())

    def test_reconciliation_uses_global_promotion_work_identity(self) -> None:
        self.assertIn("create_work_item", SOURCE)
        self.assertIn("promotion_work_key", SOURCE)
        self.assertIn("finalize_run_if_complete", SOURCE)


if __name__ == "__main__":
    unittest.main()
