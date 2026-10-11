"""Approved canonical contract reconciliation (Task 1)."""
from pathlib import Path
import unittest


class CpiW1ReleaseEvidenceDocsTest(unittest.TestCase):
    def test_section_31_2_owns_extractor_per_capability_evaluation(self):
        text = Path("docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md").read_text()
        section = text.split("### 31.2 Parser differential replay", 1)[1].split("### 31.3", 1)[0]
        self.assertNotIn("Extractor contract version is entry-scoped", section)
        self.assertIn("artifact × promotion-capability evaluation", section)
        self.assertIn("promotion capability", section)


if __name__ == "__main__":
    unittest.main()
