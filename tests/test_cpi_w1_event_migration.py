import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/011_cpi_w1_event_disclosure.sql")


class CpiW1EventMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 011 must exist")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_event_schedule_disclosure_and_marker_tables(self) -> None:
        for table in (
            "core_event_occurrences",
            "event_schedule_assertions",
            "event_disclosures",
            "event_disclosure_links",
            "event_disclosure_artifacts",
            "disclosure_marker_assertions",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_cpi_event_identity_is_reference_month_not_release_time(self) -> None:
        self.assertIn("UNIQUE (event_type, reference_month)", self.sql)
        self.assertIn("EXTRACT(DAY FROM reference_month) = 1", self.sql)
        self.assertNotIn("UNIQUE (event_type, reference_month, marker_at)", self.sql)

    def test_schedule_vocabulary_and_precision_are_exact(self) -> None:
        for value in ("'SCHEDULED'", "'DATE_PENDING'", "'CANCELED'"):
            self.assertIn(value, self.sql)
        self.assertNotIn("'RESCHEDULED'", self.sql)
        self.assertIn("time_precision IN ('EXACT', 'DATE_ONLY')", self.sql)
        self.assertIn("scheduled_at IS NULL", self.sql)

    def test_disclosure_relation_vocabularies_are_narrow(self) -> None:
        for value in (
            "'EVENT_RELEASE'",
            "'SUPPLEMENTAL_DISCLOSURE'",
            "'RELEASE_REPRESENTATION'",
            "'CORROBORATING_REPRESENTATION'",
            "'CORRECTION_NOTICE'",
        ):
            self.assertIn(value, self.sql)
        self.assertNotIn("'PRIMARY'", self.sql)

    def test_governable_evidence_reuses_row_uuid_as_subject_id(self) -> None:
        for field, subject_type in (
            ("schedule_assertion_id", "SCHEDULE_ASSERTION"),
            ("disclosure_link_id", "EVENT_DISCLOSURE_LINK"),
            ("disclosure_artifact_link_id", "DISCLOSURE_ARTIFACT_LINK"),
            ("marker_assertion_id", "DISCLOSURE_MARKER_ASSERTION"),
        ):
            self.assertIn(f"FOREIGN KEY ({field}, subject_type)", self.sql)
            self.assertIn(subject_type, self.sql)

    def test_links_publish_pit_knowledge_time_and_reference_identity(self) -> None:
        self.assertGreaterEqual(self.sql.count("accepted_at TIMESTAMPTZ NOT NULL"), 4)
        self.assertIn(
            "UNIQUE (disclosure_link_id, event_occurrence_id, disclosure_id, relation_kind)",
            self.sql,
        )
        self.assertIn(
            "UNIQUE (disclosure_artifact_link_id, disclosure_id, artifact_id, relation_kind)",
            self.sql,
        )

    def test_marker_pins_exact_disclosure_artifact_provenance(self) -> None:
        self.assertIn(
            "disclosure_artifact_link_id, disclosure_id, source_artifact_id",
            self.sql,
        )
        self.assertIn(
            "disclosure_artifact_link_id, disclosure_id, artifact_id",
            self.sql,
        )
        self.assertIn("disclosure and artifact source mismatch", self.sql)

    def test_promoter_lineage_function_has_valid_dollar_delimiter(self) -> None:
        self.assertIn("AS $promoter$", self.sql)
        self.assertIn("$promoter$;", self.sql)
        self.assertNotIn("\nAS $\nDECLARE", self.sql)

    def test_promoter_lineage_is_db_enforced(self) -> None:
        self.assertIn("enforce_cpi_w1_promoter_attempt", self.sql)
        self.assertIn(
            "canonical CPI evidence requires ECONOMIC_PROMOTE attempt lineage",
            self.sql,
        )

    def test_evidence_is_append_only(self) -> None:
        self.assertIn("reject_cpi_w1_immutable_evidence_mutation", self.sql)
        for table in (
            "core_event_occurrences",
            "event_schedule_assertions",
            "event_disclosures",
            "event_disclosure_links",
            "event_disclosure_artifacts",
            "disclosure_marker_assertions",
        ):
            self.assertIn(f"ON {table}", self.sql)


if __name__ == "__main__":
    unittest.main()
