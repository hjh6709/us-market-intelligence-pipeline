import unittest

from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotV1


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


if __name__ == "__main__":
    unittest.main()
