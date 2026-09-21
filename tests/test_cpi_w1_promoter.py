import ast
import unittest
from datetime import date
from pathlib import Path

from src.cpi_w1_contracts import PromotionFamily
from src.cpi_w1_promoter import canonical_release_disclosure_key, promotion_work_key


SOURCE = Path("src/cpi_w1_promoter.py").read_text(encoding="utf-8")


def method_source(name: str) -> str:
    module = ast.parse(SOURCE)
    for node in ast.walk(module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            segment = ast.get_source_segment(SOURCE, node)
            if segment is None:
                raise AssertionError(f"source segment unavailable for {name}")
            return segment
    raise AssertionError(f"method not found: {name}")


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

    def test_promoter_binds_candidate_to_artifact_hash_and_approved_extractor(self) -> None:
        self.assertIn("parsed candidate does not match input artifact content hash", SOURCE)
        self.assertIn("extractor contract version is not approved for artifact kind", SOURCE)
        self.assertIn("_ALLOWED_EXTRACTORS_BY_ARTIFACT", SOURCE)

    def test_promotion_public_interfaces_do_not_accept_accepted_at(self) -> None:
        self.assertNotIn("accepted_at:", SOURCE)
        self.assertNotIn("accepted_at=", SOURCE)

    def test_existing_natural_key_is_checked_before_subject_creation(self) -> None:
        link_select = SOURCE.index("SELECT disclosure_link_id")
        link_subject = SOURCE.index('"EVENT_DISCLOSURE_LINK"')
        self.assertLess(link_select, link_subject)

    def test_event_fence_precedes_secondary_topology_validation(self) -> None:
        corroborating = SOURCE.index("def promote_corroborating_representation")
        observation = SOURCE.index("def _current_valid_observation_topology")
        corroborating_body = SOURCE[corroborating:observation]
        self.assertLess(
            corroborating_body.index("lock_cpi_event"),
            corroborating_body.index("SELECT e.event_occurrence_id"),
        )

        observation_promote = SOURCE.index("def promote_observation_bundle")
        observation_body = SOURCE[observation_promote:]
        self.assertLess(
            observation_body.index("lock_cpi_event"),
            observation_body.index("_current_valid_observation_topology"),
        )

    def test_canonical_promotion_takes_event_fence(self) -> None:
        self.assertGreaterEqual(SOURCE.count("lock_cpi_event(connection,"), 3)

    def test_correction_promotions_are_split_and_subset_scoped(self) -> None:
        self.assertIn("def promote_correction_notice", SOURCE)
        self.assertIn("def promote_correction_observations", SOURCE)
        notice_body = method_source("promote_correction_notice")
        self.assertIn("'CORRECTION_NOTICE'", notice_body)
        self.assertNotIn("official_observation_assertions", notice_body)
        observation_body = method_source("promote_correction_observations")
        self.assertIn(
            "correction bundle must be a non-empty unique Core-4 subset",
            observation_body,
        )
        self.assertIn("_current_valid_correction_topology", observation_body)

    def test_corroborating_representation_has_separate_promotion_family(self) -> None:
        self.assertIn(
            "CPI_CORROBORATING_REPRESENTATION_PROMOTE",
            SOURCE,
        )
        body = method_source("promote_corroborating_representation")
        self.assertIn("'CORROBORATING_REPRESENTATION'", body)
        self.assertNotIn("official_observation_assertions", body)

    def test_envelope_and_observation_promotions_are_separate(self) -> None:
        envelope_body = method_source("promote_release_envelope")
        observation_body = method_source("promote_observation_bundle")
        self.assertNotIn("official_observation_assertions", envelope_body)
        self.assertIn("official_observation_assertions", observation_body)

    def test_core4_requires_event_release_not_supplemental_relation(self) -> None:
        observation_start = SOURCE.index("def _current_valid_observation_topology")
        body = SOURCE[observation_start:]
        self.assertIn("l.relation_kind = 'EVENT_RELEASE'", body)
        self.assertNotIn("l.relation_kind = 'SUPPLEMENTAL_DISCLOSURE'", body)

    def test_promotion_result_uses_verified_not_inserted_count(self) -> None:
        self.assertIn("verified_observation_count", SOURCE)
        self.assertNotIn("inserted_observation_count", SOURCE)

    def test_observation_retry_checks_immutable_source_provenance(self) -> None:
        self.assertIn(
            "same observation parse identity changed immutable source provenance",
            SOURCE,
        )

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
