import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/012_cpi_w1_observations.sql")


class CpiW1ObservationMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 012 must exist")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_definition_and_assertion_tables(self) -> None:
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS observation_definitions",
            self.sql,
        )
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS official_observation_assertions",
            self.sql,
        )

    def test_seeds_exact_core_four_observation_codes(self) -> None:
        expected = {
            "CPI_HEADLINE_MOM",
            "CPI_HEADLINE_YOY",
            "CPI_CORE_MOM",
            "CPI_CORE_YOY",
        }
        for code in expected:
            self.assertIn(f"'{code}'", self.sql)
        self.assertIn("canonical_unit = 'PERCENT'", self.sql)

    def test_stored_states_are_value_or_explicit_unavailable_only(self) -> None:
        self.assertIn(
            "assertion_state IN ('VALUE', 'EXPLICIT_UNAVAILABLE')",
            self.sql,
        )
        assertion_table = self.sql.split(
            "CREATE TABLE IF NOT EXISTS official_observation_assertions", 1
        )[1]
        self.assertNotIn("'UNRESOLVED'", assertion_table)
        self.assertNotIn("'CONFLICT'", assertion_table)

    def test_canonical_value_uses_exact_numeric_storage(self) -> None:
        self.assertIn("normalized_value NUMERIC", self.sql)
        self.assertNotIn("DOUBLE PRECISION", self.sql)
        self.assertNotIn("normalized_value REAL", self.sql)

    def test_value_and_unavailable_invariants_are_database_enforced(self) -> None:
        self.assertIn(
            "assertion_state = 'VALUE' AND normalized_value IS NOT NULL",
            self.sql,
        )
        self.assertIn(
            "assertion_state = 'EXPLICIT_UNAVAILABLE'",
            self.sql,
        )
        self.assertIn("normalized_value IS NULL", self.sql)
        self.assertIn("NULLIF(BTRIM(source_reason_text), '') IS NOT NULL", self.sql)

    def test_observation_reuses_row_uuid_as_governance_subject(self) -> None:
        self.assertIn(
            "FOREIGN KEY (assertion_id, subject_type)",
            self.sql,
        )
        self.assertIn("OFFICIAL_OBSERVATION_ASSERTION", self.sql)

    def test_event_type_and_observation_definition_are_bound_together(self) -> None:
        self.assertIn(
            "FOREIGN KEY (event_occurrence_id, event_type)",
            self.sql,
        )
        self.assertIn(
            "FOREIGN KEY (observation_code, event_type)",
            self.sql,
        )

    def test_assertion_pins_exact_event_disclosure_and_artifact_edges(self) -> None:
        self.assertIn(
            "disclosure_link_id, event_occurrence_id, disclosure_id",
            self.sql,
        )
        self.assertIn(
            "disclosure_artifact_link_id, disclosure_id, source_artifact_id",
            self.sql,
        )
        self.assertIn(
            "disclosure_artifact_link_id, disclosure_id, artifact_id",
            self.sql,
        )

    def test_assertion_carries_explicit_knowledge_and_material_identity(self) -> None:
        self.assertIn("accepted_at TIMESTAMPTZ NOT NULL", self.sql)
        self.assertIn("material_fingerprint TEXT NOT NULL", self.sql)
        self.assertIn("extractor_contract_version TEXT NOT NULL", self.sql)

    def test_observation_evidence_is_append_only(self) -> None:
        self.assertIn("official_observation_assertions_immutable", self.sql)
        self.assertIn(
            "reject_cpi_w1_immutable_evidence_mutation",
            self.sql,
        )


if __name__ == "__main__":
    unittest.main()
