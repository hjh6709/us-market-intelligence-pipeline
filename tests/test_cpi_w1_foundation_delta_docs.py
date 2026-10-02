import re
import unittest
from pathlib import Path


SPEC = Path(
    "docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md"
)
PLAN = Path(
    "docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md"
)
LEDGER = Path("docs/engineering/current-vs-target.md")


class CpiW1FoundationDeltaDocsTest(unittest.TestCase):
    def test_spec_freezes_capability_scoped_release_subject(self) -> None:
        text = SPEC.read_text(encoding="utf-8")
        self.assertIn("cpi-w1-release-subject-v1", text)
        for field in (
            "source_code",
            "artifact_contract_kind",
            "source_contract_version",
            "promotion_capability_id",
            "extractor_contract_version",
            "promotion_family",
        ):
            self.assertIn(f"`{field}`", text)
        self.assertIn("Capability != extractor != promotion family", text)
        self.assertIn("reference month is excluded", text)

    def test_spec_freezes_authorization_pause_and_lock_contracts(self) -> None:
        text = SPEC.read_text(encoding="utf-8")
        self.assertIn("multiple simultaneously APPROVED", text)
        self.assertIn("PENDING -> PAUSED", text)
        self.assertIn("CLAIMED -> PAUSED", text)
        self.assertIn(
            "DOMAIN -> RELEASE_SUBJECT -> WORK -> EVENT",
            text,
        )
        self.assertIn("before the first event-scoped mutation", text)
        self.assertIn("domain recovery mutation journal", text)
        self.assertIn("immutable domain recovery snapshot", text)

    def test_plan_orders_all_sixteen_accepted_phases(self) -> None:
        text = PLAN.read_text(encoding="utf-8")
        positions = []
        for phase in range(1, 17):
            marker = f"### Accepted Delta Phase {phase}:"
            self.assertEqual(text.count(marker), 1, marker)
            positions.append(text.index(marker))
        self.assertEqual(positions, sorted(positions))
        self.assertIn("Migration verification harness", text)
        self.assertIn("Final evidence package", text)

    def test_ledger_separates_keep_amend_add_and_completion_truth(self) -> None:
        text = LEDGER.read_text(encoding="utf-8")
        for heading in ("### KEEP", "### AMEND", "### ADD"):
            self.assertIn(heading, text)
        self.assertIn("ACCEPTANCE_INCOMPLETE", text)
        self.assertIn("b24eb6436494d0081a35b4098d222f0c20fd7ed3", text)
        self.assertNotRegex(
            text,
            re.compile(r"CPI W1 accepted delta[^\n]*FOUNDATION_VERIFIED", re.I),
        )


if __name__ == "__main__":
    unittest.main()
