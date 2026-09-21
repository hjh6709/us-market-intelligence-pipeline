import unittest
from datetime import date
from pathlib import Path

from src.cpi_w1_promoter import canonical_release_disclosure_key


SOURCE = Path("src/cpi_w1_promoter.py").read_text(encoding="utf-8")


class CpiW1PromoterTest(unittest.TestCase):
    def test_release_disclosure_key_is_platform_owned_and_deterministic(self) -> None:
        self.assertEqual(
            canonical_release_disclosure_key(date(2026, 8, 1)),
            "BLS:CPI:2026-08-01:DATA_RELEASE",
        )

    def test_promotion_public_interfaces_do_not_accept_accepted_at(self) -> None:
        self.assertNotIn("accepted_at:", SOURCE)
        self.assertNotIn("accepted_at=", SOURCE)

    def test_envelope_and_observation_promotions_are_separate(self) -> None:
        envelope_start = SOURCE.index("def promote_release_envelope")
        observation_start = SOURCE.index("def promote_observation_bundle")
        envelope_body = SOURCE[envelope_start:observation_start]
        self.assertNotIn("official_observation_assertions", envelope_body)
        self.assertIn("official_observation_assertions", SOURCE[observation_start:])

    def test_core4_requires_event_release_not_supplemental_relation(self) -> None:
        observation_start = SOURCE.index("def _current_valid_observation_topology")
        body = SOURCE[observation_start:]
        self.assertIn("l.relation_kind = 'EVENT_RELEASE'", body)
        self.assertNotIn("l.relation_kind = 'SUPPLEMENTAL_DISCLOSURE'", body)

    def test_observation_bundle_is_exactly_four_semantics(self) -> None:
        for code in (
            "CPI_HEADLINE_MOM",
            "CPI_HEADLINE_YOY",
            "CPI_CORE_MOM",
            "CPI_CORE_YOY",
        ):
            self.assertIn(code, SOURCE)
        self.assertIn("len(codes) != 4", SOURCE)


if __name__ == "__main__":
    unittest.main()
