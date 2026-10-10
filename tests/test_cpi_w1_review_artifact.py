import hashlib
import importlib
import importlib.util
from pathlib import Path
import tempfile
import unittest

from src.cpi_w1_evidence_policy import canonical_evidence_bytes


def authorization_bindings():
    return {"release_subject_digest": "1" * 64, "promotion_capability_id": "BLS_CPI_CORE4_HTML",
            "capability_evidence_policy_digest": "2" * 64, "evidence_snapshot_digest": "3" * 64,
            "source_contract_digest": "4" * 64, "tested_source_content_digest": "5" * 64,
            "tested_executor_source_revision": "build-revision-1", "tested_workload_artifact_digest": "6" * 64,
            "tested_job_contract_version": "cpi-w1-promoter-v1", "gate_policy_version": "cpi-w1-gate-v2"}


class ReviewArtifactTest(unittest.TestCase):
    def api(self):
        self.assertIsNotNone(importlib.util.find_spec("src.cpi_w1_review_artifact"), "missing verified review helper")
        return importlib.import_module("src.cpi_w1_review_artifact")

    def test_exact_bytes_and_bindings_and_immutable_payload(self):
        api = self.api()
        bindings = authorization_bindings()
        raw = canonical_evidence_bytes({"schema": "cpi-w1-review-artifact-v1", "purpose": "PROMOTION_AUTHORIZATION", "bindings": bindings})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "review.json").write_bytes(raw)
            review = api.load_review_artifact(root, "review.json", hashlib.sha256(raw).hexdigest())
            review.require_bindings(bindings)
            self.assertEqual(review.raw_bytes, raw)
            with self.assertRaises(ValueError):
                review.require_bindings(dict(bindings, capability_evidence_policy_digest="7" * 64))
            with self.assertRaises(ValueError):
                api.load_review_artifact(root, "review.json", "0" * 64)

    def test_paths_missing_symlink_and_noncanonical_bytes_rejected(self):
        api = self.api()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "review.json").write_bytes(b'{}\n')
            (root / "link.json").symlink_to(root / "review.json")
            for name in ("missing.json", "../review.json", str(root / "review.json"), "link.json", "./review.json", "review.json"):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    api.load_review_artifact(root, name, hashlib.sha256(b'{}\n').hexdigest())

    def test_duplicate_unknown_purpose_null_build_and_extra_binding_rejected(self):
        api = self.api()
        invalid = [b'{"purpose":"A","purpose":"B"}']
        for purpose, bindings in (("UNKNOWN", authorization_bindings()),
                                  ("PROMOTION_AUTHORIZATION", dict(authorization_bindings(), tested_workload_artifact_digest=None)),
                                  ("PROMOTION_AUTHORIZATION", dict(authorization_bindings(), extra="x"))):
            invalid.append(canonical_evidence_bytes({"schema": "cpi-w1-review-artifact-v1", "purpose": purpose, "bindings": bindings}))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for raw in invalid:
                (root / "review.json").write_bytes(raw)
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    api.load_review_artifact(root, "review.json", hashlib.sha256(raw).hexdigest())

    def test_expected_diff_shape_only_review_cannot_approve(self):
        from scripts.replay_cpi_w1_corpus import _approved_semantic_change, _semantic_digest
        approval = {"corpus_id": "test", "artifact_sha256": "a" * 64,
            "promotion_capability_id": "BLS_CPI_CORE4_HTML", "release_subject_digest": "b" * 64,
            "extractor_contract_version": "bls-cpi-core4-html-v1", "expected_semantics_sha256": _semantic_digest({"x": "0"}),
            "actual_semantics_sha256": _semantic_digest({"x": "1"}), "reason_code": "TEST", "review_ref": "missing.json"}
        self.assertFalse(_approved_semantic_change(approvals=[approval], entry={"corpus_id": "test", "expected_sha256": "a" * 64},
            promotion_capability_id="BLS_CPI_CORE4_HTML", release_subject_digest="b" * 64,
            extractor_contract_version="bls-cpi-core4-html-v1", expected={"x": "0"}, actual={"x": "1"}))


if __name__ == "__main__":
    unittest.main()
