import unittest
from pathlib import Path

from scripts.replay_cpi_w1_corpus import (
    build_report,
    corpus_snapshot_digest,
    expected_diff_approvals_digest,
    load_expected_diffs,
    load_manifest,
    replay_result_digest,
)
from src.cpi_w1_evidence_snapshot import (
    PromotionEvidenceSnapshotRegistry,
    PromotionEvidenceSnapshotV1,
    tested_source_revision_digest,
)
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


ROOT = Path(__file__).resolve().parents[1]


VALID = {
    "release_subject_digest": "1" * 64,
    "corpus_snapshot_digest": "2" * 64,
    "expected_diff_approvals_digest": "3" * 64,
    "replay_result_digest": "4" * 64,
    "tested_job_contract_version": "cpi-w1-promoter-v1",
    "tested_source_revision": "b24eb6436494d0081a35b4098d222f0c20fd7ed3",
    "tested_workload_artifact_digest": None,
    "evidence_policy_version": "cpi-w1-evidence-v1",
}


class PromotionEvidenceSnapshotV1Test(unittest.TestCase):
    def test_snapshot_digest_is_canonical_and_metadata_free(self) -> None:
        snapshot = PromotionEvidenceSnapshotV1(**VALID)
        reversed_snapshot = PromotionEvidenceSnapshotV1(
            **dict(reversed(tuple(VALID.items())))
        )
        self.assertEqual(snapshot.evidence_snapshot_digest, reversed_snapshot.evidence_snapshot_digest)
        self.assertEqual(
            snapshot.evidence_snapshot_digest,
            "bc3b65428cc693fd202d3e8753791c3648aad542b17a7da676dc4592b3492598",
        )
        self.assertNotIn("created_at", snapshot.canonical_json)
        self.assertNotIn("gate_decision", snapshot.canonical_json)

    def test_one_technical_evidence_change_changes_identity(self) -> None:
        original = PromotionEvidenceSnapshotV1(**VALID)
        changed = PromotionEvidenceSnapshotV1(
            **dict(VALID, replay_result_digest="5" * 64)
        )
        self.assertNotEqual(
            original.evidence_snapshot_digest,
            changed.evidence_snapshot_digest,
        )

    def test_rejects_noncanonical_digests_and_text(self) -> None:
        with self.assertRaises(ValueError):
            PromotionEvidenceSnapshotV1(**dict(VALID, corpus_snapshot_digest="A" * 64))
        with self.assertRaises(ValueError):
            PromotionEvidenceSnapshotV1(
                **dict(VALID, tested_job_contract_version=" cpi-w1-promoter-v1")
            )
        with self.assertRaises(ValueError):
            PromotionEvidenceSnapshotV1(
                **dict(VALID, tested_workload_artifact_digest="not-a-digest")
            )

    def test_checked_in_snapshots_reproduce_exact_capability_evidence(self) -> None:
        manifest = load_manifest(ROOT / "tests/fixtures/cpi_w1/corpus.json")
        approvals = load_expected_diffs(
            ROOT / "tests/fixtures/cpi_w1/expected-diffs.json"
        )
        report = build_report(
            manifest,
            repo_root=ROOT,
            expected_diff_approvals=approvals,
        )
        capabilities = PromotionCapabilityRegistry.from_json(
            ROOT / "config/cpi_w1_promotion_capabilities.json"
        )
        snapshots = PromotionEvidenceSnapshotRegistry.from_json(
            ROOT / "config/cpi_w1_evidence_snapshots.json"
        )

        self.assertEqual(
            set(snapshots.by_capability),
            {item.promotion_capability_id for item in capabilities.active()},
        )
        for capability in capabilities.active():
            snapshot = snapshots.require(capability.promotion_capability_id)
            result_vector = [
                item
                for item in report["results"] + report["conformance_results"]
                if item["promotion_capability_id"]
                == capability.promotion_capability_id
            ]
            self.assertEqual(
                snapshot.release_subject_digest,
                capability.release_subject.release_subject_digest,
            )
            self.assertEqual(
                snapshot.corpus_snapshot_digest,
                corpus_snapshot_digest(manifest, repo_root=ROOT),
            )
            self.assertEqual(
                snapshot.expected_diff_approvals_digest,
                expected_diff_approvals_digest(approvals),
            )
            self.assertEqual(
                snapshot.replay_result_digest,
                replay_result_digest(result_vector),
            )
            self.assertEqual(
                snapshot.tested_source_revision,
                tested_source_revision_digest(ROOT),
            )


if __name__ == "__main__":
    unittest.main()
