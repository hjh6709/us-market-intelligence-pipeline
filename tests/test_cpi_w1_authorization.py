import unittest
from dataclasses import replace

from src.cpi_w1_authorization import (
    ExecutorProvenanceV1,
    PromotionAuthorizationError,
    PromotionAuthorizationMaterialV1,
)
from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotV1
from src.cpi_w1_release_gate import CapabilityGateDecision


SUBJECT = "1" * 64
EVIDENCE = PromotionEvidenceSnapshotV1(
    release_subject_digest=SUBJECT,
    corpus_snapshot_digest="2" * 64,
    expected_diff_approvals_digest="3" * 64,
    replay_result_digest="4" * 64,
    tested_job_contract_version="cpi-w1-promoter-v1",
    tested_source_revision="5" * 64,
    tested_workload_artifact_digest=None,
    evidence_policy_version="cpi-w1-evidence-v1",
)
EXECUTOR = ExecutorProvenanceV1(
    source_revision="6" * 64,
    workload_artifact_digest="7" * 64,
    job_contract_version="cpi-w1-promoter-v1",
)


def decision(state: str = "ELIGIBLE") -> CapabilityGateDecision:
    return CapabilityGateDecision(
        promotion_capability_id="BLS_CPI_CORE4_HTML",
        release_subject_digest=SUBJECT,
        evidence_snapshot_digest=EVIDENCE.evidence_snapshot_digest,
        gate_policy_version="cpi-w1-gate-v2",
        decision=state,
        reason_code="REVIEWED_ELIGIBLE" if state == "ELIGIBLE" else "BLOCKED",
        review_ref="REVIEW-123",
        review_digest="8" * 64,
        gate_decision_digest="9" * 64,
    )


def historical_material():
    return PromotionAuthorizationMaterialV1(
            release_subject_digest=SUBJECT,
            evidence_snapshot_digest=EVIDENCE.evidence_snapshot_digest,
            gate_decision_digest="9" * 64,
            gate_policy_version="cpi-w1-gate-v2",
            authorization_policy_version="cpi-w1-authorization-v1",
            executor_source_revision=EXECUTOR.source_revision,
            executor_workload_artifact_digest=EXECUTOR.workload_artifact_digest,
            executor_job_contract_version=EXECUTOR.job_contract_version,
            review_ref="AUTH-REVIEW-1",
            review_digest="a" * 64,
        )


class PromotionAuthorizationMaterialV1Test(unittest.TestCase):
    def test_historical_material_retains_deterministic_identity(self) -> None:
        first = historical_material()
        second = historical_material()
        self.assertEqual(first.authorization_material_digest, second.authorization_material_digest)
        self.assertEqual(first.release_subject_digest, SUBJECT)
        self.assertEqual(first.evidence_snapshot_digest, EVIDENCE.evidence_snapshot_digest)
        self.assertEqual(first.gate_decision_digest, "9" * 64)

    def test_blocked_gate_cannot_create_authorization_material(self) -> None:
        with self.assertRaises(PromotionAuthorizationError):
            PromotionAuthorizationMaterialV1.from_review(
                evidence=EVIDENCE,
                gate_decision=decision("BLOCKED"),
                executor=EXECUTOR,
                review_ref="AUTH-REVIEW-1",
                review_digest="a" * 64,
            )

    def test_mismatched_subject_or_evidence_fails_closed(self) -> None:
        mismatched = decision()
        mismatched = CapabilityGateDecision(
            **dict(mismatched.__dict__, release_subject_digest="b" * 64)
        )
        with self.assertRaises(PromotionAuthorizationError):
            PromotionAuthorizationMaterialV1.from_review(
                evidence=EVIDENCE,
                gate_decision=mismatched,
                executor=EXECUTOR,
                review_ref="AUTH-REVIEW-1",
                review_digest="a" * 64,
            )

    def test_executor_identity_is_authorization_material(self) -> None:
        original = historical_material()
        changed = replace(original, executor_source_revision="b" * 64)
        self.assertNotEqual(
            original.authorization_material_digest,
            changed.authorization_material_digest,
        )


if __name__ == "__main__":
    unittest.main()
