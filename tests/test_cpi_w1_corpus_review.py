import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts.review_cpi_w1_corpus_capture import CorpusReviewError, review_capture
from src.cpi_w1_source import BlsCpiSourceContract


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = BlsCpiSourceContract.from_json(ROOT / "config/cpi_w1_source_contract.json")


class CpiW1CorpusReviewTest(unittest.TestCase):
    def manifest(self):
        return {
            "entries": [
                {
                    "corpus_id": "cpi:2026-08:release-html",
                    "reference_month": "2026-08",
                    "artifact_contract_kind": "CPI_RELEASE_HTML",
                    "source_contract_version": "bls-cpi-source-v1",
                    "official_locator": "https://www.bls.gov/news.release/archives/cpi_09112026.htm",
                    "expected_sha256": None,
                    "local_path": None,
                    "materialization_status": "REMOTE_ONLY",
                    "replay_required": True,
                    "expected_semantics": {"kind": "UNVERIFIED_INVENTORY"},
                    "exceptional_tags": [],
                    "extractor_contract_version": "bls-cpi-release-html-v1",
                }
            ]
        }

    def staged(self, root: Path):
        body = b"<html>official exact bytes</html>\n"
        sha = hashlib.sha256(body).hexdigest()
        data = root / "capture.html"
        meta = root / "capture.json"
        data.write_bytes(body)
        meta.write_text(
            json.dumps(
                {
                    "schema_version": "cpi-w1-corpus-capture-v1",
                    "corpus_id": "cpi:2026-08:release-html",
                    "reference_month": "2026-08",
                    "artifact_contract_kind": "CPI_RELEASE_HTML",
                    "source_contract_version": "bls-cpi-source-v1",
                    "requested_url": "https://www.bls.gov/news.release/archives/cpi_09112026.htm",
                    "final_url": "https://www.bls.gov/news.release/archives/cpi_09112026.htm",
                    "http_status": 200,
                    "content_type": "text/html; charset=utf-8",
                    "content_sha256": sha,
                    "byte_count": len(body),
                    "captured_at": datetime(2026, 9, 22, tzinfo=timezone.utc).isoformat(),
                    "promotion_status": "STAGED_REVIEW_REQUIRED",
                }
            ),
            encoding="utf-8",
        )
        return data, meta, sha

    def test_review_requires_independent_matching_hash_and_preserves_semantic_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, sha = self.staged(root)
            result = review_capture(
                manifest=self.manifest(),
                contract=CONTRACT,
                corpus_id="cpi:2026-08:release-html",
                data_path=data,
                metadata_path=meta,
                reviewer_ref="REVIEW-123",
                verified_sha256=sha,
                approved_root=root / "tests/fixtures/cpi_w1/official",
                repo_root=root,
            )
            candidate = result["candidate_entry"]
            self.assertEqual(candidate["materialization_status"], "MATERIALIZED")
            self.assertEqual(candidate["expected_sha256"], sha)
            self.assertEqual(
                candidate["expected_semantics"],
                {"kind": "UNVERIFIED_INVENTORY"},
            )
            self.assertEqual(result["status"], "REVIEWED_BYTES_ONLY")

    def test_review_rejects_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, _sha = self.staged(root)
            with self.assertRaises(CorpusReviewError):
                review_capture(
                    manifest=self.manifest(),
                    contract=CONTRACT,
                    corpus_id="cpi:2026-08:release-html",
                    data_path=data,
                    metadata_path=meta,
                    reviewer_ref="REVIEW-123",
                    verified_sha256="0" * 64,
                    approved_root=root / "tests/fixtures/cpi_w1/official",
                repo_root=root,
                )

    def test_review_rejects_manifest_sidecar_identity_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, sha = self.staged(root)
            payload = json.loads(meta.read_text(encoding="utf-8"))
            payload["reference_month"] = "2026-07"
            meta.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(CorpusReviewError):
                review_capture(
                    manifest=self.manifest(),
                    contract=CONTRACT,
                    corpus_id="cpi:2026-08:release-html",
                    data_path=data,
                    metadata_path=meta,
                    reviewer_ref="REVIEW-123",
                    verified_sha256=sha,
                    approved_root=root / "tests/fixtures/cpi_w1/official",
                repo_root=root,
                )

    def test_review_rejects_non_allowlisted_final_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, sha = self.staged(root)
            payload = json.loads(meta.read_text(encoding="utf-8"))
            payload["final_url"] = "https://example.com/cpi"
            meta.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(Exception):
                review_capture(
                    manifest=self.manifest(),
                    contract=CONTRACT,
                    corpus_id="cpi:2026-08:release-html",
                    data_path=data,
                    metadata_path=meta,
                    reviewer_ref="REVIEW-123",
                    verified_sha256=sha,
                    approved_root=root / "tests/fixtures/cpi_w1/official",
                repo_root=root,
                )

    def test_review_rejects_approved_root_outside_official_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, sha = self.staged(root)
            with self.assertRaises(CorpusReviewError):
                review_capture(
                    manifest=self.manifest(),
                    contract=CONTRACT,
                    corpus_id="cpi:2026-08:release-html",
                    data_path=data,
                    metadata_path=meta,
                    reviewer_ref="REVIEW-123",
                    verified_sha256=sha,
                    approved_root=root / "outside",
                    repo_root=root,
                )

    def test_review_does_not_mutate_manifest(self) -> None:
        manifest = self.manifest()
        original = json.dumps(manifest, sort_keys=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, meta, sha = self.staged(root)
            review_capture(
                manifest=manifest,
                contract=CONTRACT,
                corpus_id="cpi:2026-08:release-html",
                data_path=data,
                metadata_path=meta,
                reviewer_ref="REVIEW-123",
                verified_sha256=sha,
                approved_root=root / "tests/fixtures/cpi_w1/official",
                repo_root=root,
            )
        self.assertEqual(json.dumps(manifest, sort_keys=True), original)


if __name__ == "__main__":
    unittest.main()
