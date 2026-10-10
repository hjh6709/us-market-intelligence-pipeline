import copy
import importlib
import importlib.util
import json
from pathlib import Path
import unittest

from src.cpi_w1_evidence_policy import CapabilityEvidencePolicyRegistry
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


if __name__ == "__main__":
    unittest.main()
