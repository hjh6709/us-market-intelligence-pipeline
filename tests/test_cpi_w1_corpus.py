import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.replay_cpi_w1_corpus import (
    CorpusValidationError,
    _approved_semantic_change,
    _semantic_digest,
    build_report,
    corpus_snapshot_digest,
    expected_diff_approvals_digest,
    load_expected_diffs,
    load_manifest,
    replay_result_digest,
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

    def test_release_html_inventory_has_independent_capability_expectations(self) -> None:
        release_entries = [
            entry
            for entry in self.manifest["entries"]
            if entry["artifact_contract_kind"] == "CPI_RELEASE_HTML"
        ]
        self.assertTrue(release_entries)
        for entry in release_entries:
            self.assertEqual(
                {
                    expectation["promotion_capability_id"]
                    for expectation in entry["capability_expectations"]
                },
                {
                    "BLS_CPI_RELEASE_ENVELOPE_HTML",
                    "BLS_CPI_CORE4_HTML",
                },
            )
            self.assertNotIn("extractor_contract_version", entry)
            self.assertNotIn("expected_semantics", entry)
            self.assertNotIn("replay_required", entry)

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
            entry["capability_expectations"][0]["expected_semantics"][
                "schedule_status"
            ],
            "CANCELED",
        )
        self.assertEqual(entry["materialization_status"], "REMOTE_ONLY")

    def test_synthetic_release_html_replays_without_counting_as_official_corpus(self) -> None:
        report = build_report(self.manifest, repo_root=ROOT)
        self.assertEqual(len(report["conformance_results"]), 4)
        self.assertEqual(
            {item["semantic_status"] for item in report["conformance_results"]},
            {"SEMANTIC_UNCHANGED"},
        )
        self.assertEqual(
            {
                item["promotion_capability_id"]
                for item in report["conformance_results"]
            },
            {
                "BLS_CPI_RELEASE_ENVELOPE_HTML",
                "BLS_CPI_CORE4_HTML",
            },
        )
        self.assertEqual(report["counts"]["materialized_pinned"], 0)

    def test_one_artifact_records_independent_capability_outcomes(self) -> None:
        manifest = json.loads(json.dumps(self.manifest))
        fixture = manifest["conformance_fixtures"][1]
        core4 = next(
            item
            for item in fixture["capability_expectations"]
            if item["promotion_capability_id"] == "BLS_CPI_CORE4_HTML"
        )
        core4["expected_semantics"]["values"]["CPI_HEADLINE_MOM"] = "9.9"

        report = build_report(manifest, repo_root=ROOT)
        outcomes = {
            item["promotion_capability_id"]: item["semantic_status"]
            for item in report["conformance_results"]
            if item["corpus_id"] == fixture["fixture_id"]
        }
        self.assertEqual(
            outcomes,
            {
                "BLS_CPI_RELEASE_ENVELOPE_HTML": "SEMANTIC_UNCHANGED",
                "BLS_CPI_CORE4_HTML": "UNEXPECTED_CHANGED",
            },
        )

    def test_evidence_digests_are_semantic_and_order_independent(self) -> None:
        manifest = json.loads(json.dumps(self.manifest))
        original = corpus_snapshot_digest(manifest, repo_root=ROOT)
        manifest["entries"].reverse()
        for entry in manifest["entries"]:
            entry["capability_expectations"].reverse()
            entry["local_path"] = None
        self.assertEqual(original, corpus_snapshot_digest(manifest, repo_root=ROOT))

        approvals = [
            {
                "corpus_id": "cpi:test",
                "artifact_sha256": "1" * 64,
                "promotion_capability_id": "BLS_CPI_CORE4_HTML",
                "release_subject_digest": "2" * 64,
                "extractor_contract_version": "bls-cpi-core4-html-v1",
                "expected_semantics_sha256": "3" * 64,
                "actual_semantics_sha256": "4" * 64,
                "reason_code": "EXPECTED_CHANGE",
                "review_ref": "REVIEW-1",
            }
        ]
        self.assertEqual(
            expected_diff_approvals_digest(approvals),
            expected_diff_approvals_digest(list(reversed(approvals))),
        )

        report = build_report(self.manifest, repo_root=ROOT)
        self.assertEqual(
            replay_result_digest(report["results"]),
            replay_result_digest(list(reversed(report["results"]))),
        )
        for result in report["conformance_results"]:
            self.assertIn("expected_semantics_digest", result)
            self.assertIn("actual_semantics_digest", result)

    def test_evidence_requirements_fail_until_full_baseline_is_materialized(self) -> None:
        report = build_report(self.manifest, repo_root=ROOT)
        self.assertFalse(report["evidence_requirements_satisfied"])
        self.assertEqual(report["counts"]["entries"], 56)
        self.assertEqual(report["counts"]["materialized_pinned"], 0)
        self.assertEqual(report["counts"]["remote_only"], 56)
        self.assertEqual(report["counts"]["synthetic_conformance"], 4)

    def test_evidence_requirements_include_conformance_and_official_corpus(self) -> None:
        report = build_report(self.manifest, repo_root=ROOT)
        self.assertTrue(report["conformance_ready"])
        self.assertFalse(report["official_corpus_ready"])
        self.assertFalse(report["evidence_requirements_satisfied"])

    def test_expected_change_approval_is_bound_to_exact_artifact_extractor_and_semantics(self) -> None:
        entry = {
            "corpus_id": "cpi:test",
            "expected_sha256": "a" * 64,
        }
        release_subject_digest = "c" * 64
        expected = {"CPI_HEADLINE_MOM": "0.3"}
        actual = {"CPI_HEADLINE_MOM": "0.4"}
        approval = {
            "corpus_id": "cpi:test",
            "artifact_sha256": "a" * 64,
            "promotion_capability_id": "BLS_CPI_CORE4_HTML",
            "release_subject_digest": release_subject_digest,
            "extractor_contract_version": "extractor-v2",
            "expected_semantics_sha256": _semantic_digest(expected),
            "actual_semantics_sha256": _semantic_digest(actual),
            "reason_code": "INTENTIONAL_PARSER_CHANGE",
            "review_ref": "REVIEW-123",
        }
        self.assertTrue(
            _approved_semantic_change(
                approvals=[approval],
                entry=entry,
                promotion_capability_id="BLS_CPI_CORE4_HTML",
                release_subject_digest=release_subject_digest,
                extractor_contract_version="extractor-v2",
                expected=expected,
                actual=actual,
            )
        )
        changed_artifact = dict(entry)
        changed_artifact["expected_sha256"] = "b" * 64
        self.assertFalse(
            _approved_semantic_change(
                approvals=[approval],
                entry=changed_artifact,
                promotion_capability_id="BLS_CPI_CORE4_HTML",
                release_subject_digest=release_subject_digest,
                extractor_contract_version="extractor-v2",
                expected=expected,
                actual=actual,
            )
        )
        self.assertFalse(
            _approved_semantic_change(
                approvals=[approval],
                entry=entry,
                promotion_capability_id="BLS_CPI_CORE4_HTML",
                release_subject_digest=release_subject_digest,
                extractor_contract_version="extractor-v2",
                expected=expected,
                actual={"CPI_HEADLINE_MOM": "0.5"},
            )
        )
        self.assertFalse(
            _approved_semantic_change(
                approvals=[approval],
                entry=entry,
                promotion_capability_id="BLS_CPI_RELEASE_ENVELOPE_HTML",
                release_subject_digest="d" * 64,
                extractor_contract_version="extractor-v2",
                expected=expected,
                actual=actual,
            )
        )

    def test_checked_in_expected_diff_file_starts_empty(self) -> None:
        approvals = load_expected_diffs(
            ROOT / "tests/fixtures/cpi_w1/expected-diffs.json"
        )
        self.assertEqual(approvals, [])

    def test_materialized_official_path_cannot_escape_repository_corpus(self) -> None:
        manifest = json.loads(json.dumps(self.manifest))
        entry = manifest["entries"][0]
        entry["materialization_status"] = "MATERIALIZED"
        entry["expected_sha256"] = "0" * 64
        entry["local_path"] = "../../etc/passwd"
        with self.assertRaises(Exception):
            validate_manifest(manifest, ROOT)

    def test_replay_tool_has_no_database_dependency(self) -> None:
        source = (
            ROOT / "scripts/replay_cpi_w1_corpus.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("psycopg", source)
        self.assertNotIn("DATABASE_URL", source)
        self.assertNotIn("source_artifacts", source)


class CpiW1MaterializedV2ReplayTest(unittest.TestCase):
    """Temporary synthetic bytes exercise the real materialized path, not official evidence."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.manifest = load_manifest(MANIFEST_PATH)
        registry_path = Path("config/cpi_w1_promotion_capabilities.json")
        (self.root / registry_path).parent.mkdir(parents=True)
        (self.root / registry_path).write_bytes((ROOT / registry_path).read_bytes())
        for fixture in self.manifest["conformance_fixtures"]:
            path = self.root / fixture["local_path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((ROOT / fixture["local_path"]).read_bytes())
        fixture = self.manifest["conformance_fixtures"][1]
        self.entry = next(e for e in self.manifest["entries"] if e["reference_month"] == "2026-08")
        relative = "tests/fixtures/cpi_w1/official/unit-test-only.html"
        path = self.root / relative
        path.parent.mkdir(parents=True)
        path.write_bytes((ROOT / fixture["local_path"]).read_bytes())
        self.entry.update(
            materialization_status="MATERIALIZED", local_path=relative,
            expected_sha256=fixture["expected_sha256"],
            capability_expectations=json.loads(json.dumps(fixture["capability_expectations"])),
        )

    def results(self, approvals=None):
        report = build_report(self.manifest, repo_root=self.root, expected_diff_approvals=approvals)
        return [r for r in report["results"] if r["corpus_id"] == self.entry["corpus_id"]]

    def test_materialized_v2_replays_both_registry_scoped_extractors(self) -> None:
        validate_manifest(self.manifest, self.root)
        results = self.results()
        self.assertEqual(len(results), 2)
        self.assertEqual({r["semantic_status"] for r in results}, {"SEMANTIC_UNCHANGED"})
        self.assertEqual(
            {r["promotion_capability_id"]: r["extractor_contract_version"] for r in results},
            {"BLS_CPI_RELEASE_ENVELOPE_HTML": "bls-cpi-release-envelope-html-v1",
             "BLS_CPI_CORE4_HTML": "bls-cpi-core4-html-v1"},
        )
        self.assertEqual(len({r["release_subject_digest"] for r in results}), 2)
        self.assertTrue(all(r["artifact_sha256"] == self.entry["expected_sha256"] for r in results))

    def test_artifact_wide_extractor_remains_forbidden(self) -> None:
        self.entry["extractor_contract_version"] = "bls-cpi-core4-html-v1"
        with self.assertRaisesRegex(CorpusValidationError, "legacy artifact-wide"):
            self.results()

    def test_unknown_capability_fails_closed(self) -> None:
        self.entry["capability_expectations"][0]["promotion_capability_id"] = "UNKNOWN"
        with self.assertRaisesRegex(CorpusValidationError, "unknown promotion capability"):
            self.results()

    def test_artifact_capability_mismatch_fails_closed(self) -> None:
        self.entry["capability_expectations"][0]["promotion_capability_id"] = "BLS_CPI_SCHEDULE_HTML"
        with self.assertRaisesRegex(CorpusValidationError, "artifact contract mismatch"):
            self.results()

    def test_materialized_replay_digest_ignores_expectation_order(self) -> None:
        first = self.results()
        self.entry["capability_expectations"].reverse()
        second = self.results()
        self.assertEqual(replay_result_digest(first), replay_result_digest(second))
        self.assertEqual({r["semantic_status"] for r in second}, {"SEMANTIC_UNCHANGED"})

    def test_unimplemented_registry_extractor_version_cannot_run_v1_parser(self) -> None:
        path = self.root / "config/cpi_w1_promotion_capabilities.json"
        registry = json.loads(path.read_text())
        for capability in registry["capabilities"]:
            if capability["promotion_capability_id"] == "BLS_CPI_CORE4_HTML":
                capability["extractor_contract_version"] = "bls-cpi-core4-html-v2"
        path.write_text(json.dumps(registry))
        result = next(r for r in self.results() if r["promotion_capability_id"] == "BLS_CPI_CORE4_HTML")
        self.assertEqual(result["extractor_contract_version"], "bls-cpi-core4-html-v2")
        self.assertEqual(result["inventory_status"], "BLOCKED_NO_EXTRACTOR")
        self.assertEqual(result["semantic_status"], "NOT_RUN")

    def test_materialized_expected_diff_requires_every_exact_scope_field(self) -> None:
        core = self.entry["capability_expectations"][1]
        core["expected_semantics"]["values"]["CPI_HEADLINE_MOM"] = "9.9"
        before = {"CPI_HEADLINE_MOM": "9.9", "CPI_HEADLINE_YOY": "3.4", "CPI_CORE_MOM": "0.3", "CPI_CORE_YOY": "2.4"}
        after = dict(before, CPI_HEADLINE_MOM="0.4")
        approval = {
            "corpus_id": self.entry["corpus_id"], "artifact_sha256": self.entry["expected_sha256"],
            "promotion_capability_id": "BLS_CPI_CORE4_HTML",
            "release_subject_digest": "9be6522e1b2db41e0745c687799f44446b7f375a578b057b94a09c218a28dfe7",
            "extractor_contract_version": "bls-cpi-core4-html-v1",
            "expected_semantics_sha256": _semantic_digest(before),
            "actual_semantics_sha256": _semantic_digest(after),
            "reason_code": "UNIT_TEST_ONLY", "review_ref": "UNIT_TEST_ONLY_NOT_OFFICIAL_APPROVAL",
        }
        outcomes = {r["promotion_capability_id"]: r["semantic_status"] for r in self.results([approval])}
        self.assertEqual(outcomes, {"BLS_CPI_CORE4_HTML": "EXPECTED_CHANGED", "BLS_CPI_RELEASE_ENVELOPE_HTML": "SEMANTIC_UNCHANGED"})
        for field in ("corpus_id", "artifact_sha256", "promotion_capability_id", "release_subject_digest", "extractor_contract_version", "expected_semantics_sha256", "actual_semantics_sha256"):
            with self.subTest(field=field):
                changed = dict(approval, **{field: "different"})
                result = next(r for r in self.results([changed]) if r["promotion_capability_id"] == "BLS_CPI_CORE4_HTML")
                self.assertEqual(result["semantic_status"], "UNEXPECTED_CHANGED")


if __name__ == "__main__":
    unittest.main()
