from dataclasses import replace
import importlib
import importlib.util
import unittest

from src.cpi_w1_authorization import ExecutorProvenanceV1, PromotionAuthorizationMaterialV1
from src.cpi_w1_evidence_policy import CapabilityEvidencePolicyRegistry, canonical_evidence_bytes
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry
from src.cpi_w1_review_artifact import ReviewArtifactV1
from src.cpi_w1_release_gate import CapabilityGateDecision
from pathlib import Path


def v2_case(api):
    registry = PromotionCapabilityRegistry.from_json("config/cpi_w1_promotion_capabilities.json")
    policy = CapabilityEvidencePolicyRegistry.from_json(Path("config/cpi_w1_capability_evidence_policies.json"), registry).require("BLS_CPI_CORE4_HTML")
    values = {"schema": "cpi-w1-promotion-evidence-snapshot-v2", "evidence_policy_version": "cpi-w1-evidence-v2",
        "promotion_capability_id": policy.promotion_capability_id, "release_subject_digest": policy.release_subject_digest,
        "capability_evidence_policy_digest": policy.policy_digest, "capability_corpus_snapshot_digest": "2" * 64,
        "expected_diff_approvals_digest": "3" * 64, "replay_result_digest": "4" * 64,
        "source_contract_digest": "5" * 64, "tested_source_content_digest": "6" * 64,
        "tested_executor_source_revision": "build-revision-1", "tested_workload_artifact_digest": "7" * 64,
        "tested_job_contract_version": "cpi-w1-promoter-v1"}
    evidence = api.PromotionEvidenceSnapshotV2.from_mapping(values)
    bindings = {key: values[key] for key in ("release_subject_digest", "promotion_capability_id", "capability_evidence_policy_digest",
        "source_contract_digest", "tested_source_content_digest", "tested_executor_source_revision", "tested_workload_artifact_digest", "tested_job_contract_version")}
    bindings.update(evidence_snapshot_digest=evidence.evidence_snapshot_digest, gate_policy_version="cpi-w1-gate-v2")
    review = ReviewArtifactV1("test-review.json", canonical_evidence_bytes({"schema": "cpi-w1-review-artifact-v1", "purpose": "PROMOTION_AUTHORIZATION", "bindings": bindings}))
    executor = ExecutorProvenanceV1("build-revision-1", "7" * 64, "cpi-w1-promoter-v1")
    gate = CapabilityGateDecision(policy.promotion_capability_id, policy.release_subject_digest, evidence.evidence_snapshot_digest,
        "cpi-w1-gate-v2", "ELIGIBLE", "TEST_ONLY", review.review_ref, review.review_digest, "8" * 64)
    return values, evidence, policy, review, executor, gate


class PromotionEvidenceV2Test(unittest.TestCase):
    def api(self):
        self.assertIsNotNone(importlib.util.find_spec("src.cpi_w1_evidence_snapshot_v2"), "missing V2 snapshot")
        return importlib.import_module("src.cpi_w1_evidence_snapshot_v2")

    def test_v2_roundtrip_order_independent_and_rejects_v1_field_confusion(self):
        api = self.api()
        values, evidence, *_ = v2_case(api)
        self.assertEqual(evidence.evidence_snapshot_digest, api.PromotionEvidenceSnapshotV2.from_mapping(dict(reversed(list(values.items())))).evidence_snapshot_digest)
        for change in ({"corpus_snapshot_digest": "9" * 64}, {"schema": "cpi-w1-promotion-evidence-snapshot-v1"}, {"tested_workload_artifact_digest": "bad"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                api.PromotionEvidenceSnapshotV2.from_mapping(dict(values, **change))

    def material(self, **changes):
        api = self.api()
        values, evidence, policy, review, executor, gate = v2_case(api)
        from src.cpi_w1_authorization import PromotionAuthorizationMaterialV2
        args = dict(evidence=evidence, current_policy=policy, review=review, executor=executor,
                    gate_decision=gate, source_contract_digest="5" * 64)
        args.update(changes)
        return PromotionAuthorizationMaterialV2.from_review(**args)

    def test_exact_binding_positive_and_artifact_job_source_mismatch_rejected(self):
        self.assertEqual(self.material().executor_workload_artifact_digest, "7" * 64)
        for executor in (ExecutorProvenanceV1("build-revision-1", "0" * 64, "cpi-w1-promoter-v1"),
                         ExecutorProvenanceV1("build-revision-1", "7" * 64, "cpi-w1-promoter-v2"),
                         ExecutorProvenanceV1("6" * 64, "7" * 64, "cpi-w1-promoter-v1")):
            with self.subTest(executor=executor), self.assertRaises(ValueError):
                self.material(executor=executor)

    def test_null_build_is_readable_but_not_eligible(self):
        api = self.api()
        values, *_ = v2_case(api)
        evidence = api.PromotionEvidenceSnapshotV2.from_mapping(dict(values, tested_workload_artifact_digest=None, tested_executor_source_revision=None))
        self.assertIsNone(evidence.tested_workload_artifact_digest)
        with self.assertRaises(ValueError):
            self.material(evidence=evidence)

    def test_stale_policy_review_contract_and_blocked_gate_rejected(self):
        api = self.api()
        values, evidence, policy, review, executor, gate = v2_case(api)
        for change in ({"source_contract_digest": "0" * 64}, {"gate_decision": replace(gate, decision="BLOCKED")},
                       {"evidence": api.PromotionEvidenceSnapshotV2.from_mapping(dict(values, capability_evidence_policy_digest="0" * 64))}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.material(**change)

    def test_v1_new_authorization_is_retired(self):
        from tests.test_cpi_w1_authorization import EVIDENCE, EXECUTOR, decision
        with self.assertRaisesRegex(ValueError, "LEGACY_AUTHORIZATION_RETIRED"):
            PromotionAuthorizationMaterialV1.from_review(evidence=EVIDENCE, executor=EXECUTOR,
                gate_decision=decision(), review_ref="old", review_digest="a" * 64)


if __name__ == "__main__":
    unittest.main()
