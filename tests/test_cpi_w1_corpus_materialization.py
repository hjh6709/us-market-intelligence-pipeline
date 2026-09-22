import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts.materialize_cpi_w1_corpus import (
    CorpusMaterializationError,
    locator_for_entry,
    materialize_entry,
)
from src.cpi_w1_source import (
    BlsCpiSourceContract,
    CapturedResponse,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = BlsCpiSourceContract.from_json(
    ROOT / "config/cpi_w1_source_contract.json"
)


class FakeClient:
    def __init__(self, response: CapturedResponse) -> None:
        self.response = response
        self.seen = []

    def fetch(self, locator):
        self.seen.append(locator)
        return self.response


class CpiW1CorpusMaterializationTest(unittest.TestCase):
    def entry(self):
        return {
            "corpus_id": "cpi:2026-08:release-html",
            "reference_month": "2026-08",
            "artifact_contract_kind": "CPI_RELEASE_HTML",
            "source_contract_version": "bls-cpi-source-v1",
            "official_locator": (
                "https://www.bls.gov/news.release/archives/cpi_09112026.htm"
            ),
            "materialization_status": "REMOTE_ONLY",
            "replay_required": True,
        }

    def response(self):
        return CapturedResponse(
            locator_key="cpi:2026-08:release-html",
            requested_url=(
                "https://www.bls.gov/news.release/archives/cpi_09112026.htm"
            ),
            final_url=(
                "https://www.bls.gov/news.release/archives/cpi_09112026.htm"
            ),
            status_code=200,
            headers={"content-type": "text/html; charset=utf-8"},
            body=b"<html>official exact bytes</html>\n",
            captured_at=datetime(2026, 9, 22, 0, 0, tzinfo=timezone.utc),
        )

    def test_archive_release_uses_versioned_source_policy(self) -> None:
        locator = locator_for_entry(CONTRACT, self.entry())
        self.assertEqual(locator.surface_role, "RELEASE_EVIDENCE")
        self.assertEqual(locator.max_bytes, 8 * 1024 * 1024)
        self.assertFalse(locator.capture_chronology_allowed)

    def test_materialization_stages_bytes_and_sidecar_without_mutating_manifest(self) -> None:
        entry = self.entry()
        original = json.dumps(entry, sort_keys=True)
        fake = FakeClient(self.response())
        with tempfile.TemporaryDirectory() as temp:
            result = materialize_entry(
                entry=entry,
                contract=CONTRACT,
                client=fake,
                staging_dir=Path(temp),
            )
            data_path = Path(result["data_path"])
            metadata_path = Path(result["metadata_path"])
            self.assertEqual(data_path.read_bytes(), self.response().body)
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata["promotion_status"], "STAGED_REVIEW_REQUIRED")
            self.assertEqual(metadata["byte_count"], len(self.response().body))
            self.assertEqual(
                metadata["requested_url"],
                self.entry()["official_locator"],
            )
            self.assertEqual(json.dumps(entry, sort_keys=True), original)

    def test_existing_output_requires_explicit_overwrite(self) -> None:
        fake = FakeClient(self.response())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            materialize_entry(
                entry=self.entry(),
                contract=CONTRACT,
                client=fake,
                staging_dir=root,
            )
            with self.assertRaises(CorpusMaterializationError):
                materialize_entry(
                    entry=self.entry(),
                    contract=CONTRACT,
                    client=fake,
                    staging_dir=root,
                )

    def test_non_bls_locator_is_rejected_before_fetch(self) -> None:
        entry = self.entry()
        entry["official_locator"] = "https://example.com/cpi"
        with self.assertRaises(Exception):
            locator_for_entry(CONTRACT, entry)

    def test_materialized_entry_cannot_be_silently_restaged(self) -> None:
        entry = self.entry()
        entry["materialization_status"] = "MATERIALIZED"
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(CorpusMaterializationError):
                materialize_entry(
                    entry=entry,
                    contract=CONTRACT,
                    client=FakeClient(self.response()),
                    staging_dir=Path(temp),
                )

    def test_script_does_not_write_database_or_manifest(self) -> None:
        source = (
            ROOT / "scripts/materialize_cpi_w1_corpus.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("psycopg", source)
        self.assertNotIn("DATABASE_URL", source)
        self.assertNotIn("write_text(\n        json.dumps(payload", source)
        self.assertIn("STAGED_REVIEW_REQUIRED", source)


if __name__ == "__main__":
    unittest.main()
