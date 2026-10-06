import argparse
import hashlib
import tempfile
import unittest
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from uuid import UUID, uuid4

from scripts.collect_cpi_w1 import (
    CpiW1CollectorOrchestrator,
    build_parser,
    validate_args,
)
from src.cpi_w1_artifacts import FilesystemArtifactStore
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry
from src.cpi_w1_release_gate import CapabilityGateDecision
from src.cpi_w1_source import BlsCpiSourceContract


SOURCE = Path("scripts/collect_cpi_w1.py").read_text(encoding="utf-8")
ROOT = Path(".")


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _Connection:
    @staticmethod
    def transaction():
        return nullcontext()


class _Gate:
    gate_fingerprint = _digest("gate-snapshot")

    def __init__(self, decisions):
        self.decisions = decisions

    def decision(self, capability_id, release_subject_digest=None):
        return self.decisions[capability_id]


class _Repository:
    def __init__(self, targets):
        self.targets = targets
        self.deferred = []
        self.runs = []
        self.works = []
        self.existing = {}
        self.locked = []

    def artifact_promotion_targets(self, connection, *, artifact_id):
        return self.targets

    def record_promotion_deferred(self, connection, **kwargs):
        self.deferred.append(kwargs)
        return uuid4()

    def lock_promotion_scheduling(self, connection, artifact_id):
        self.locked.append(artifact_id)

    def existing_promotion_work_items(self, connection, *, artifact_id, work_identities):
        return {key: self.existing[key] for key in work_identities if key in self.existing}

    def create_run(self, connection, **kwargs):
        self.runs.append(kwargs)
        return uuid4()

    def create_work_item(self, connection, **kwargs):
        work_id = uuid4()
        self.works.append(kwargs)
        self.existing[kwargs["work_key"]] = work_id
        return work_id


class CollectCpiW1Test(unittest.TestCase):
    def orchestrator(self, repository, gate):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return CpiW1CollectorOrchestrator(
            repository=repository,
            source_contract=BlsCpiSourceContract.from_json(
                ROOT / "config/cpi_w1_source_contract.json"
            ),
            capability_registry=PromotionCapabilityRegistry.from_json(
                ROOT / "config/cpi_w1_promotion_capabilities.json"
            ),
            release_gate=gate,
            artifact_store=FilesystemArtifactStore(temporary.name),
        )

    @staticmethod
    def decision(capability, decision):
        return CapabilityGateDecision(
            promotion_capability_id=capability.promotion_capability_id,
            release_subject_digest=capability.release_subject.release_subject_digest,
            evidence_snapshot_digest=_digest(capability.promotion_capability_id),
            gate_policy_version="cpi-w1-gate-v2",
            decision=decision,
            reason_code=("APPROVED" if decision == "ELIGIBLE" else "CORPUS_BLOCKED"),
            review_ref="review:test",
            review_digest=_digest("review"),
            gate_decision_digest=_digest(capability.promotion_capability_id + decision),
        )

    def parse(self, *args: str) -> argparse.Namespace:
        return build_parser().parse_args(args)

    def test_cli_supports_live_backfill_replay_and_dry_run(self) -> None:
        for mode in ("live", "backfill", "replay"):
            arguments = ["--mode", mode, "--dry-run"]
            if mode != "replay":
                arguments += ["--reference-month", "2026-08"]
            args = self.parse(*arguments)
            self.assertEqual(args.mode, mode)
        self.assertTrue(
            self.parse(
                "--mode", "live", "--reference-month", "2026-08", "--dry-run"
            ).dry_run
        )

    def test_live_collection_requires_explicit_reference_month_target(self) -> None:
        with self.assertRaises(SystemExit):
            validate_args(self.parse("--mode", "live", "--dry-run"))
        args = self.parse(
            "--mode", "live",
            "--reference-month", "2026-08",
            "--reference-month", "2026-09",
            "--dry-run",
        )
        validate_args(args)
        self.assertEqual(args.reference_month, ["2026-08", "2026-09"])

    def test_backfill_reconciliation_uses_persisted_targets_only(self) -> None:
        args = self.parse("--mode", "backfill", "--reconcile-promotions")
        validate_args(args)

        with self.assertRaises(SystemExit):
            validate_args(
                self.parse(
                    "--mode", "backfill",
                    "--reconcile-promotions",
                    "--reference-month", "2026-08",
                )
            )

    def test_independent_capabilities_schedule_and_defer_for_same_artifact(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(
            ROOT / "config/cpi_w1_promotion_capabilities.json"
        )
        envelope = registry.require("BLS_CPI_RELEASE_ENVELOPE_HTML")
        core4 = registry.require("BLS_CPI_CORE4_HTML")
        repository = _Repository((date(2026, 8, 1),))
        orchestrator = self.orchestrator(
            repository,
            _Gate(
                {
                    envelope.promotion_capability_id: self.decision(
                        envelope, "ELIGIBLE"
                    ),
                    core4.promotion_capability_id: self.decision(core4, "BLOCKED"),
                }
            ),
        )
        artifact_id = uuid4()

        result = orchestrator._schedule_promotions(
            _Connection(),
            artifact_id=artifact_id,
            artifact_contract_kind="CPI_RELEASE_HTML",
            source_contract_version="bls-cpi-source-v1",
            run_mode="LIVE",
        )

        self.assertEqual(result.status, "PARTIALLY_SCHEDULED")
        self.assertEqual(result.deferred_capability_ids, (core4.promotion_capability_id,))
        self.assertEqual(len(repository.works), 1)
        self.assertEqual(
            repository.works[0]["release_subject"], envelope.release_subject
        )
        self.assertIn("REF:2026-08-01", repository.works[0]["work_key"])
        self.assertEqual(
            repository.deferred[0]["release_subject_digest"],
            core4.release_subject.release_subject_digest,
        )
        self.assertEqual(
            repository.deferred[0]["gate_decision_digest"],
            self.decision(core4, "BLOCKED").gate_decision_digest,
        )

    def test_one_capability_creates_one_work_per_immutable_target(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(
            ROOT / "config/cpi_w1_promotion_capabilities.json"
        )
        envelope = registry.require("BLS_CPI_RELEASE_ENVELOPE_HTML")
        core4 = registry.require("BLS_CPI_CORE4_HTML")
        repository = _Repository((date(2026, 8, 1), date(2026, 9, 1)))
        orchestrator = self.orchestrator(
            repository,
            _Gate(
                {
                    envelope.promotion_capability_id: self.decision(
                        envelope, "ELIGIBLE"
                    ),
                    core4.promotion_capability_id: self.decision(core4, "BLOCKED"),
                }
            ),
        )

        result = orchestrator._schedule_promotions(
            _Connection(),
            artifact_id=uuid4(),
            artifact_contract_kind="CPI_RELEASE_HTML",
            source_contract_version="bls-cpi-source-v1",
            run_mode="BACKFILL",
        )

        self.assertEqual(result.status, "PARTIALLY_SCHEDULED")
        self.assertEqual(len(repository.works), 2)
        self.assertEqual(len(repository.runs), 1)
        self.assertEqual(
            [work["target_reference_month"] for work in repository.works],
            [date(2026, 8, 1), date(2026, 9, 1)],
        )

    def test_missing_target_is_not_misreported_as_all_capabilities_blocked(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(
            ROOT / "config/cpi_w1_promotion_capabilities.json"
        )
        envelope = registry.require("BLS_CPI_RELEASE_ENVELOPE_HTML")
        core4 = registry.require("BLS_CPI_CORE4_HTML")
        repository = _Repository(())
        orchestrator = self.orchestrator(
            repository,
            _Gate(
                {
                    envelope.promotion_capability_id: self.decision(
                        envelope, "ELIGIBLE"
                    ),
                    core4.promotion_capability_id: self.decision(core4, "BLOCKED"),
                }
            ),
        )

        result = orchestrator._schedule_promotions(
            _Connection(),
            artifact_id=uuid4(),
            artifact_contract_kind="CPI_RELEASE_HTML",
            source_contract_version="bls-cpi-source-v1",
            run_mode="BACKFILL",
        )

        self.assertEqual(
            result.status,
            "NO_TARGET_BINDING_WITH_DEFERRED_CAPABILITIES",
        )
        self.assertEqual(result.blocked_reason_code, "ARTIFACT_TARGET_NOT_BOUND")
        self.assertEqual(
            result.deferred_capability_ids,
            (core4.promotion_capability_id,),
        )
        self.assertEqual(repository.runs, [])

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

    def test_schedule_artifacts_have_explicit_promotion_contract(self) -> None:
        self.assertIn("capability_registry.active_for_artifact", SOURCE)
        self.assertNotIn("_EXTRACTOR_BY_ARTIFACT_KIND", SOURCE)
        self.assertNotIn("_FAMILIES_BY_ARTIFACT_KIND", SOURCE)

    def test_collection_and_promotion_are_separate(self) -> None:
        self.assertIn("record_source_artifact", SOURCE)
        self.assertIn("_schedule_promotions", SOURCE)
        self.assertNotIn("promote_release_envelope(", SOURCE)
        self.assertNotIn("promote_observation_bundle(", SOURCE)

    def test_promotion_workload_identity_is_not_the_input_artifact_uuid(self) -> None:
        self.assertNotIn("workload_artifact_digest=str(artifact_id)", SOURCE)
        self.assertIn("_PROMOTE_WORKLOAD_ARTIFACT_DIGEST", SOURCE)

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

    def test_deferred_audit_is_bound_to_gate_snapshot(self) -> None:
        self.assertIn("gate_fingerprint=self.release_gate.gate_fingerprint", SOURCE)

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

    def test_reconciliation_reuses_existing_global_promotion_work(self) -> None:
        self.assertIn("existing_promotion_work_items", SOURCE)
        self.assertIn('"ALREADY_SCHEDULED"', SOURCE)
        self.assertIn("missing = tuple", SOURCE)

    def test_replay_run_records_capture_origin_and_mode(self) -> None:
        self.assertIn('run_mode=("REPLAY" if only_artifact_id is not None else "BACKFILL")', SOURCE)
        self.assertIn("replay_of_run_id=", SOURCE)
        self.assertIn('f"artifact:{run_mode}:{artifact_id}:workset:{workset_digest}"', SOURCE)

    def test_reconciliation_uses_global_promotion_work_identity(self) -> None:
        self.assertIn("create_work_item", SOURCE)
        self.assertIn("promotion_work_key", SOURCE)
        self.assertIn("finalize_run_if_complete", SOURCE)

    def test_reconciliation_is_scoped_to_exact_cpi_w1_contracts(self) -> None:
        self.assertIn("r.job_type='CPI_W1_COLLECT'", SOURCE)
        self.assertIn("r.job_contract_version=%s", SOURCE)
        self.assertIn("job_type IN ('CPI_W1_COLLECT', 'CPI_W1_PROMOTE')", SOURCE)
        self.assertIn("job_contract_version IN (%s, %s)", SOURCE)
        self.assertNotIn("resume_paused_work", SOURCE)


if __name__ == "__main__":
    unittest.main()
