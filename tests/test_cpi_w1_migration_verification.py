import os
import unittest
from pathlib import Path

from src.cpi_w1_migration_verification import (
    IMMUTABLE_BASELINE_MANIFEST,
    compare_fresh_and_upgrade,
    load_hash_manifest,
    verify_immutable_baseline,
)


class CpiW1MigrationManifestTest(unittest.TestCase):
    def test_foundation_manifest_freezes_exact_001_through_013(self):
        from src import cpi_w1_migration_verification as verification
        self.assertTrue(hasattr(verification, 'load_foundation_hash_manifest'))
        entries = verification.load_foundation_hash_manifest()
        self.assertEqual(len(entries), 13)
        self.assertEqual([int(entry.path.name[:3]) for entry in entries], list(range(1, 14)))
        self.assertEqual(verify_immutable_baseline(entries), ())
    def test_manifest_freezes_exact_001_through_009_bytes(self) -> None:
        entries = load_hash_manifest(IMMUTABLE_BASELINE_MANIFEST)
        self.assertEqual(
            [entry.path.name for entry in entries],
            [
                "001_market_bars.sql",
                "002_macro_event_analysis.sql",
                "003_matched_baseline.sql",
                "004_pipeline_experiments.sql",
                "005_derived_bar_coverage.sql",
                "006_pipeline_runs.sql",
                "007_event_strategy_results.sql",
                "008_paper_order_intents.sql",
                "009_event_session_foundations.sql",
            ],
        )
        self.assertEqual(verify_immutable_baseline(entries), ())

    def test_manifest_rejects_modified_baseline_bytes(self) -> None:
        entries = load_hash_manifest(IMMUTABLE_BASELINE_MANIFEST)
        first = entries[0]
        modified = first.with_digest("0" * 64)
        failures = verify_immutable_baseline((modified, *entries[1:]))
        self.assertEqual(len(failures), 1)
        self.assertIn(first.path.as_posix(), failures[0])

    def test_ci_runs_isolated_fresh_upgrade_verification(self) -> None:
        workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("tests.test_cpi_w1_migration_verification", workflow)


@unittest.skipUnless(
    os.environ.get("RUN_POSTGRES_INTEGRATION") == "1",
    "set RUN_POSTGRES_INTEGRATION=1 to test isolated migration databases",
)
class CpiW1MigrationEquivalencePostgresTest(unittest.TestCase):
    def test_foundation_upgrade_preserves_historical_v1_and_matches_fresh_014(self):
        from src import cpi_w1_migration_verification as verification
        self.assertTrue(hasattr(verification,'compare_foundation_upgrade'))
        result=verification.compare_foundation_upgrade(os.environ.get('DATABASE_URL','postgresql://market:market@localhost:55435/market'))
        self.assertEqual(result.differences,())
        self.assertTrue(result.historical_fixture_preserved)
        self.assertTrue(result.legacy_fixture_preserved)
        self.assertEqual(result.migration_names[-1],'014_cpi_w1_release_evidence_v2.sql')
    def test_fresh_and_legacy_upgrade_have_equivalent_target_schema(self) -> None:
        result = compare_fresh_and_upgrade(
            os.environ.get(
                "DATABASE_URL",
                "postgresql://market:market@localhost:55432/market",
            )
        )
        self.assertEqual(result.differences, ())
        self.assertTrue(result.legacy_fixture_preserved)
        self.assertEqual(result.migration_names[-1], "014_cpi_w1_release_evidence_v2.sql")


if __name__ == "__main__":
    unittest.main()
