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

    def test_cpi_w1_target_route_requires_target_canonical_spec_status(self) -> None:
        authority = Path("docs/architecture/AUTHORITY.md").read_text(encoding="utf-8")
        self.assertIn(
            "| CPI W1 Data & Governance target | `TARGET_CANONICAL` |",
            authority,
        )

        spec = Path(
            "docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md"
        ).read_text(encoding="utf-8")
        first_nonempty = next(line.strip() for line in spec.splitlines() if line.strip())
        self.assertEqual(first_nonempty, "STATUS: TARGET_CANONICAL")

    def test_current_implementation_truth_is_checkout_scoped(self) -> None:
        authority = Path("docs/architecture/AUTHORITY.md").read_text(encoding="utf-8")
        self.assertIn("code + migrations + tests at the commit being inspected", authority)
        self.assertNotIn("at the deployed commit", authority)

        spec = Path(
            "docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md"
        ).read_text(encoding="utf-8")
        self.assertNotIn("at the deployed commit", spec)
        self.assertIn("at the commit being inspected", spec)

        docs_hub = Path("docs/README.md").read_text(encoding="utf-8")
        self.assertIn("executable code + migrations + tests", docs_hub)
        self.assertIn("descriptive snapshot", docs_hub)
        self.assertIn("STATUS_LEDGER", docs_hub)

    def test_configuration_cannot_create_target_authority(self) -> None:
        text = Path("docs/configuration/README.md").read_text(encoding="utf-8")
        self.assertIn("authority index", text)
        self.assertIn("TARGET_CANONICAL", text)
        self.assertIn("cannot promote", text)
        self.assertNotIn("Canonical contracts describe required target semantics", text)

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
