import copy
import importlib
import importlib.util
import json
import hashlib
import tempfile
from pathlib import Path
import unittest

from src.cpi_w1_evidence_policy import CapabilityEvidencePolicyRegistry, CapabilityEvidencePolicyV1, canonical_evidence_bytes
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


class CapabilityEvidenceReadinessTest(unittest.TestCase):
    def setUp(self):
        self.registry = PromotionCapabilityRegistry.from_json("config/cpi_w1_promotion_capabilities.json")
        self.policies = CapabilityEvidencePolicyRegistry.from_json(Path("config/cpi_w1_capability_evidence_policies.json"), self.registry)
        self.manifest = json.loads(Path("tests/fixtures/cpi_w1/corpus.json").read_text())
        self.cap = "BLS_CPI_CORE4_HTML"

    def api(self):
        self.assertIsNotNone(importlib.util.find_spec("src.cpi_w1_evidence_readiness"), "missing capability readiness")
        return importlib.import_module("src.cpi_w1_evidence_readiness")

    def digest(self, manifest):
        return self.api().capability_corpus_snapshot_digest(manifest, self.registry, self.policies.require(self.cap))

    def test_unrelated_capability_mutation_preserves_digest(self):
        raw = copy.deepcopy(self.manifest)
        raw["entries"][0]["capability_expectations"][0]["expected_semantics"] = {"kind": "OTHER"}
        self.assertEqual(self.digest(self.manifest), self.digest(raw))
        raw["entries"].reverse()
        self.assertEqual(self.digest(self.manifest), self.digest(raw))

    def test_relevant_hash_semantics_and_tags_change_digest(self):
        baseline = self.digest(self.manifest)
        for field, value in (("expected_sha256", "a" * 64), ("exceptional_tags", ["ZERO_VALUE"])):
            raw = copy.deepcopy(self.manifest)
            raw["entries"][0][field] = value
            self.assertNotEqual(baseline, self.digest(raw))
        raw = copy.deepcopy(self.manifest)
        raw["entries"][0]["capability_expectations"][1]["expected_semantics"] = {"kind": "CHANGED"}
        self.assertNotEqual(baseline, self.digest(raw))

    def test_missing_active_policy_is_not_ready(self):
        rows = self.api().evaluate_capability_readiness(self.registry, CapabilityEvidencePolicyRegistry(()), self.manifest, [], [], [])
        self.assertEqual(len(rows), 9)
        for row in rows:
            self.assertEqual(row.status, "NOT_READY")
            self.assertIn("POLICY_MISSING", row.blocking_reasons)

    def test_zero_official_and_synthetic_only_never_ready(self):
        raw = copy.deepcopy(self.manifest)
        raw["entries"] = []
        for row in self.api().evaluate_capability_readiness(self.registry, self.policies, raw, [], [], []):
            self.assertEqual(row.status, "NOT_READY")
            self.assertIn("OFFICIAL_EVIDENCE_MISSING", row.blocking_reasons)

    def test_checked_in_all_nine_not_ready_and_unverified_cancellation_not_omitted(self):
        rows = self.api().evaluate_capability_readiness(self.registry, self.policies, self.manifest, [], [], [])
        self.assertEqual(len(rows), 9)
        for row in rows:
            self.assertEqual(row.status, "NOT_READY")
            self.assertEqual(row.materialized_pinned, 0)
        core = next(row for row in rows if row.promotion_capability_id == self.cap)
        self.assertIn("2025-10", core.missing_reference_months)

    def test_duplicate_scoped_results_are_rejected(self):
        row = {"promotion_capability_id": self.cap, "corpus_id": "x", "extractor_contract_version": "bls-cpi-core4-html-v1"}
        with self.assertRaises(ValueError):
            self.api().capability_replay_result_digest([row, row], self.cap)

    def test_replay_and_diff_digests_exclude_other_capabilities(self):
        row = {"promotion_capability_id": self.cap, "corpus_id": "x", "extractor_contract_version": "bls-cpi-core4-html-v1", "semantic_status": "SEMANTIC_UNCHANGED"}
        other = dict(row, promotion_capability_id="BLS_CPI_SCHEDULE_HTML")
        api = self.api()
        self.assertEqual(api.capability_replay_result_digest([row], self.cap), api.capability_replay_result_digest([other, row], self.cap))
        self.assertEqual(api.capability_expected_diff_digest([row], self.cap), api.capability_expected_diff_digest([other, row], self.cap))

    def test_inventory_report_enumerates_all_active_capabilities(self):
        from scripts.replay_cpi_w1_corpus import build_report
        report = build_report(self.manifest, repo_root=Path.cwd())
        self.assertEqual(len(report.get("capability_readiness", [])), 9)
        self.assertTrue(all(row["status"] == "NOT_READY" for row in report["capability_readiness"]))

    def test_replay_for_different_bytes_cannot_satisfy_evidence(self):
        raw = copy.deepcopy(self.manifest)
        entry = raw["entries"][0]
        entry["materialization_status"] = "MATERIALIZED"
        entry["expected_sha256"] = "a" * 64
        entry["capability_expectations"][1]["expected_semantics"] = {"kind": "CORE4"}
        result = {"promotion_capability_id": self.cap, "corpus_id": entry["corpus_id"],
                  "extractor_contract_version": "bls-cpi-core4-html-v1", "artifact_sha256": "b" * 64,
                  "inventory_status": "MATERIALIZED_PINNED", "semantic_status": "SEMANTIC_UNCHANGED"}
        rows = self.api().evaluate_capability_readiness(self.registry, self.policies, raw, [result], [], [])
        self.assertEqual(next(r for r in rows if r.promotion_capability_id == self.cap).replay_passed, 0)

    def cancellation_case(self):
        """Hand-declared synthetic readiness input, never official evidence or a gate."""
        cid = "BLS_CPI_REVISED_RELEASE_DATES"
        raw = copy.deepcopy(self.manifest)
        entry = next(e for e in raw["entries"] if e["corpus_id"] == "cpi:2025-10:explicit-cancellation")
        entry.update(materialization_status="MATERIALIZED", expected_sha256="a" * 64)
        fixture = dict(copy.deepcopy(entry), fixture_id="synthetic:2025-10:cancellation", fixture_kind="SYNTHETIC_CONFORMANCE")
        fixture.pop("corpus_id")
        raw["conformance_fixtures"] = [fixture]
        capability = self.registry.require(cid)
        semantics = hashlib.sha256(b'{"schedule_status":"CANCELED"}').hexdigest()
        result = {"corpus_id": entry["corpus_id"], "promotion_capability_id": cid,
                  "release_subject_digest": capability.release_subject.release_subject_digest,
                  "extractor_contract_version": capability.extractor_contract_version,
                  "artifact_sha256": "a" * 64, "expected_semantics_digest": semantics,
                  "actual_semantics_digest": semantics, "inventory_status": "MATERIALIZED_PINNED",
                  "semantic_status": "SEMANTIC_UNCHANGED"}
        conformance = dict(result, corpus_id=fixture["fixture_id"], inventory_status="SYNTHETIC_CONFORMANCE")
        return cid, raw, result, conformance

    def cancellation_readiness(self, cid, raw, result, conformance, policies=None):
        rows = self.api().evaluate_capability_readiness(self.registry, policies or self.policies,
                                                       raw, [result], [conformance], [])
        return next(row for row in rows if row.promotion_capability_id == cid)

    def test_each_required_conformance_class_must_be_proven(self):
        cid, raw, result, conformance = self.cancellation_case()
        self.assertEqual(self.cancellation_readiness(cid, raw, result, conformance).status, "READY")
        value = self.policies.require(cid).payload()
        value["required_conformance_classes"].append("ADDITIONAL_REQUIRED_CLASS")
        policy = CapabilityEvidencePolicyV1.from_mapping(value, self.registry)
        policies = CapabilityEvidencePolicyRegistry((policy,))
        row = self.cancellation_readiness(cid, raw, result, conformance, policies)
        self.assertEqual(row.status, "NOT_READY")
        self.assertIn("CONFORMANCE_MISSING_OR_FAILED", row.blocking_reasons)

    def test_unlisted_or_stale_conformance_cannot_satisfy_policy(self):
        cid, raw, result, conformance = self.cancellation_case()
        for field, value in (("corpus_id", "unlisted"), ("artifact_sha256", "b" * 64),
                             ("release_subject_digest", "b" * 64), ("expected_semantics_digest", "b" * 64)):
            with self.subTest(field=field):
                row = self.cancellation_readiness(cid, raw, result, dict(conformance, **{field: value}))
                self.assertEqual(row.status, "NOT_READY")
                self.assertEqual(row.conformance_passed, 0)

    def test_stale_expected_semantics_or_subject_cannot_count_as_verified_replay(self):
        cid, raw, result, conformance = self.cancellation_case()
        entry = next(e for e in raw["entries"] if e["corpus_id"] == result["corpus_id"])
        entry["capability_expectations"][0]["expected_semantics"]["schedule_status"] = "SCHEDULED"
        row = self.cancellation_readiness(cid, raw, result, conformance)
        self.assertEqual(row.status, "NOT_READY")
        self.assertEqual(row.replay_passed, 0)
        cid, raw, result, conformance = self.cancellation_case()
        row = self.cancellation_readiness(cid, raw, dict(result, release_subject_digest="b" * 64), conformance)
        self.assertEqual(row.replay_passed, 0)

    def test_cancellation_dependency_requires_exact_pinned_replay_binding(self):
        cid, raw, result, conformance = self.cancellation_case()
        for field, value in (("artifact_sha256", "b" * 64), ("extractor_contract_version", "stale"),
                             ("release_subject_digest", "b" * 64), ("actual_semantics_digest", "b" * 64)):
            with self.subTest(field=field):
                rows = self.api().evaluate_capability_readiness(self.registry, self.policies, raw,
                                                               [dict(result, **{field: value})], [], [])
                core = next(row for row in rows if row.promotion_capability_id == self.cap)
                self.assertIn("2025-10", core.missing_reference_months)

    def test_changed_replay_without_exact_diff_approval_is_not_ready(self):
        cid, raw, result, conformance = self.cancellation_case()
        changed = dict(result, semantic_status="EXPECTED_CHANGED", actual_semantics_digest="b" * 64)
        row = self.cancellation_readiness(cid, raw, changed, conformance)
        self.assertEqual(row.status, "NOT_READY")
        self.assertEqual(row.replay_passed, 0)

    def test_expected_change_requires_matching_canonical_review_bytes(self):
        cid, raw, result, conformance = self.cancellation_case()
        entry = next(e for e in raw["entries"] if e["corpus_id"] == result["corpus_id"])
        entry["capability_expectations"][0]["expected_semantics"]["schedule_status"] = "SCHEDULED"
        changed = dict(result, semantic_status="EXPECTED_CHANGED",
                       expected_semantics_digest=hashlib.sha256(b'{"schedule_status":"SCHEDULED"}').hexdigest())
        bindings = {"release_subject_digest": result["release_subject_digest"], "promotion_capability_id": cid,
                    "artifact_sha256": "a" * 64, "extractor_contract_version": result["extractor_contract_version"],
                    "expected_semantics_sha256": changed["expected_semantics_digest"],
                    "actual_semantics_sha256": result["actual_semantics_digest"]}
        review = canonical_evidence_bytes({"schema": "cpi-w1-review-artifact-v1",
                                          "purpose": "EXPECTED_DIFF", "bindings": bindings})
        approval = dict(bindings, corpus_id=result["corpus_id"], reason_code="TEST_REVIEW",
                        review_ref="review.json", review_digest=hashlib.sha256(review).hexdigest())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "review.json"
            path.write_bytes(review)
            def evaluate():
                return next(row for row in self.api().evaluate_capability_readiness(
                    self.registry, self.policies, raw, [changed], [conformance], [approval], repo_root=root)
                    if row.promotion_capability_id == cid)
            self.assertEqual(evaluate().status, "READY")
            path.write_bytes(review + b"\n")
            self.assertEqual(evaluate().status, "NOT_READY")

    def test_cancellation_dependency_digest_binds_complete_evidence_projection(self):
        baseline = self.digest(self.manifest)
        for field, value in (("review", {"status": "CHANGED"}), ("source_contract_version", "changed-source")):
            raw = copy.deepcopy(self.manifest)
            cancellation = next(e for e in raw["entries"] if e["corpus_id"] == "cpi:2025-10:explicit-cancellation")
            cancellation[field] = value
            with self.subTest(field=field):
                self.assertNotEqual(baseline, self.digest(raw))
        raw = copy.deepcopy(self.manifest)
        cancellation = next(e for e in raw["entries"] if e["corpus_id"] == "cpi:2025-10:explicit-cancellation")
        cancellation["capability_expectations"][0]["replay_required"] = False
        self.assertNotEqual(baseline, self.digest(raw))

    def test_expected_diff_digest_uses_full_actual_transition_identity(self):
        first = {"corpus_id": "x", "promotion_capability_id": self.cap, "artifact_sha256": "a" * 64,
                 "extractor_contract_version": "bls-cpi-core4-html-v1",
                 "expected_semantics_sha256": "b" * 64, "actual_semantics_sha256": "c" * 64}
        second = dict(first, actual_semantics_sha256="d" * 64)
        api = self.api()
        try:
            digest = api.capability_expected_diff_digest([first, second], self.cap)
        except ValueError as error:
            self.fail(f"distinct full transition identities were collapsed: {error}")
        self.assertEqual(digest, api.capability_expected_diff_digest([second, first], self.cap))
        with self.assertRaises(ValueError):
            api.capability_expected_diff_digest([first, dict(first, reason_code="different")], self.cap)


if __name__ == "__main__":
    unittest.main()
