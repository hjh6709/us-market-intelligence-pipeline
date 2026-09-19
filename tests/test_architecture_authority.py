import unittest
from pathlib import Path


class ArchitectureAuthorityTest(unittest.TestCase):
    def test_authority_index_routes_current_target_and_legacy_contracts(self) -> None:
        authority = Path("docs/architecture/AUTHORITY.md")
        self.assertTrue(authority.exists())
        text = authority.read_text(encoding="utf-8")
        self.assertIn("CPI W1 Data & Governance", text)
        self.assertIn("2026-09-19-cpi-w1-data-governance-design.md", text)
        self.assertIn("CURRENT_IMPLEMENTATION", text)
        self.assertIn("HISTORICAL_SUPERSEDED", text)

    def test_old_target_specs_are_visibly_superseded(self) -> None:
        for path in (
            "docs/superpowers/specs/2026-09-10-canonical-docs-event-data-foundation-design.md",
            "docs/superpowers/specs/2026-09-10-operational-pipelines-storage-design.md",
            "docs/superpowers/specs/2026-09-10-serving-deployment-product-design.md",
        ):
            text = Path(path).read_text(encoding="utf-8")
            self.assertIn("HISTORICAL_SUPERSEDED", text)


if __name__ == "__main__":
    unittest.main()
