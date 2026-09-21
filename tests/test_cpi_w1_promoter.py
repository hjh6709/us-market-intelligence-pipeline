import unittest
from datetime import date
from pathlib import Path

from src.cpi_w1_contracts import PromotionFamily
from src.cpi_w1_promoter import canonical_release_disclosure_key, promotion_work_key


SOURCE = Path("src/cpi_w1_promoter.py").read_text(encoding="utf-8")


class CpiW1PromoterTest(unittest.TestCase):
    def test_claim_prefix_filter_does_not_use_sql_like(self) -> None:
        repository_source = Path("src/cpi_w1_repository.py").read_text(encoding="utf-8")
        self.assertIn("LEFT(w.work_key, CHAR_LENGTH(%s)) = %s", repository_source)
        self.assertNotIn("w.work_key LIKE %s", repository_source)

    def test_release_disclosure_key_is_platform_owned_and_deterministic(self) -> None:
        self.assertEqual(
            canonical_release_disclosure_key(date(2026, 8, 1)),
            "BLS:CPI:2026-08-01:DATA_RELEASE",
        )

    def test_promotion_work_key_binds_family_artifact_and_extractor(self) -> None:
        from uuid import UUID

        artifact_id = UUID("00000000-0000-0000-0000-000000000123")
        self.assertEqual(
            promotion_work_key(
                PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
                artifact_id,
                "bls-cpi-release-html-v1",
            ),
            (
                "CPI_RELEASE_ENVELOPE_PROMOTE:"
                "00000000-0000-0000-0000-000000000123:"
                "bls-cpi-release-html-v1"
            ),
        )

    def test_promotion_public_interfaces_do_not_accept_accepted_at(self) -> None:
        self.assertNotIn("accepted_at:", SOURCE)
        self.assertNotIn("accepted_at=", SOURCE)

    def test_existing_natural_key_is_checked_before_subject_creation(self) -> None:
        link_select = SOURCE.index("SELECT disclosure_link_id")
        link_subject = SOURCE.index('"EVENT_DISCLOSURE_LINK"')
        self.assertLess(link_select, link_subject)

    def test_corroborating_representation_has_separate_promotion_family(self) -> None:
        self.assertIn(
            "CPI_CORROBORATING_REPRESENTATION_PROMOTE",
            SOURCE,
        )
        start = SOURCE.index("def promote_corroborating_representation")
        observation = SOURCE.index("def promote_observation_bundle")
        body = SOURCE[start:observation]
        self.assertIn("'CORROBORATING_REPRESENTATION'", body)
        self.assertNotIn("official_observation_assertions", body)

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

    def test_promotion_result_uses_verified_not_inserted_count(self) -> None:
        self.assertIn("verified_observation_count", SOURCE)
        self.assertNotIn("inserted_observation_count", SOURCE)

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
