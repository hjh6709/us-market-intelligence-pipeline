import hashlib
import json
import unittest
from pathlib import Path

from scripts.replay_cpi_w1_corpus import (
    build_report,
    load_manifest,
    validate_conformance_fixtures,
    validate_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests/fixtures/cpi_w1/corpus.json"


class CpiW1CorpusTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load_manifest(MANIFEST_PATH)

    def test_baseline_inventory_is_contiguous_and_unique(self) -> None:
        validate_manifest(self.manifest, ROOT)
        self.assertEqual(len(self.manifest["entries"]), 56)
        self.assertEqual(
            self.manifest["baseline"],
            {
                "start_reference_month": "2022-01",
                "end_reference_month": "2026-08",
            },
        )

    def test_official_inventory_does_not_claim_synthetic_bytes(self) -> None:
        self.assertTrue(
            all(
                entry["materialization_status"] == "REMOTE_ONLY"
                for entry in self.manifest["entries"]
            )
        )
        self.assertTrue(
            all(
                entry["local_path"] is None
                and entry["expected_sha256"] is None
                for entry in self.manifest["entries"]
            )
        )

    def test_synthetic_conformance_fixtures_are_separately_sha_pinned(self) -> None:
        validate_conformance_fixtures(self.manifest, ROOT)
        fixtures = self.manifest["conformance_fixtures"]
        self.assertEqual(len(fixtures), 2)
        for fixture in fixtures:
            data = (ROOT / fixture["local_path"]).read_bytes()
            self.assertEqual(
                hashlib.sha256(data).hexdigest(),
                fixture["expected_sha256"],
            )

    def test_required_exception_tags_exist_on_exact_reference_months(self) -> None:
        by_month = {
            entry["reference_month"]: entry
            for entry in self.manifest["entries"]
        }
        self.assertIn(
            "ZERO_HEADLINE_MOM",
            by_month["2023-10"]["exceptional_tags"],
        )
        self.assertIn(
            "NEGATIVE_HEADLINE_MOM",
            by_month["2024-06"]["exceptional_tags"],
        )
        self.assertIn(
            "JANUARY_SEASONAL_REVISION",
            by_month["2025-01"]["exceptional_tags"],
        )
        self.assertIn(
            "EXPLICIT_CANCELLATION",
            by_month["2025-10"]["exceptional_tags"],
        )
        self.assertIn(
            "EXPLICIT_UNAVAILABLE",
            by_month["2025-11"]["exceptional_tags"],
        )
        self.assertIn(
            "POST_SHUTDOWN_RECOVERY",
            by_month["2025-11"]["exceptional_tags"],
        )
        self.assertIn(
            "CURRENT_BASELINE_RELEASE",
            by_month["2026-08"]["exceptional_tags"],
        )

    def test_october_2025_cancellation_uses_exception_surface_not_missing_release(self) -> None:
        entry = next(
            item
            for item in self.manifest["entries"]
            if item["reference_month"] == "2025-10"
        )
        self.assertEqual(
            entry["artifact_contract_kind"],
            "BLS_REVISED_RELEASE_DATES_HTML",
        )
        self.assertEqual(
            entry["official_locator"],
            "https://www.bls.gov/bls/2025-lapse-revised-release-dates.htm",
        )
        self.assertEqual(
            entry["expected_semantics"]["schedule_status"],
            "CANCELED",
        )
        self.assertEqual(entry["materialization_status"], "REMOTE_ONLY")

    def test_synthetic_release_html_replays_without_counting_as_official_corpus(self) -> None:
        report = build_report(self.manifest, repo_root=ROOT)
        self.assertEqual(len(report["conformance_results"]), 2)
        self.assertEqual(
            {item["semantic_status"] for item in report["conformance_results"]},
            {"SEMANTIC_UNCHANGED"},
        )
        self.assertEqual(report["counts"]["materialized_pinned"], 0)

    def test_release_gate_fails_closed_until_full_baseline_is_materialized(self) -> None:
        report = build_report(self.manifest, repo_root=ROOT)
        self.assertFalse(report["release_gate_ready"])
        self.assertEqual(report["counts"]["entries"], 56)
        self.assertEqual(report["counts"]["materialized_pinned"], 0)
        self.assertEqual(report["counts"]["remote_only"], 56)
        self.assertEqual(report["counts"]["synthetic_conformance"], 2)

    def test_replay_tool_has_no_database_dependency(self) -> None:
        source = (
            ROOT / "scripts/replay_cpi_w1_corpus.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("psycopg", source)
        self.assertNotIn("DATABASE_URL", source)
        self.assertNotIn("source_artifacts", source)


if __name__ == "__main__":
    unittest.main()
