import unittest
from pathlib import Path


MIGRATION = Path("db/migrations/013_cpi_w1_governance_serving.sql")


class CpiW1GovernanceMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MIGRATION.exists(), "migration 013 must exist")
        self.sql = MIGRATION.read_text(encoding="utf-8")

    def test_declares_governance_and_serving_tables(self) -> None:
        for table in (
            "interpretation_requests",
            "interpretation_approvals",
            "interpretation_decisions",
            "business_audit_events",
            "economic_serving_control_decisions",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_two_person_and_one_vote_constraints_are_structural(self) -> None:
        self.assertIn("CHECK (approver_subject <> proposer_subject)", self.sql)
        self.assertIn("UNIQUE (request_id, approver_subject)", self.sql)
        self.assertIn("approver_subject = BTRIM(approver_subject)", self.sql)
        self.assertIn("proposer_subject = BTRIM(proposer_subject)", self.sql)

    def test_policy_v1_owns_exact_24_hour_expiry(self) -> None:
        self.assertIn("cpi-governance-v1", self.sql)
        self.assertIn("NEW.expires_at := NEW.requested_at + interval '24 hours'", self.sql)

    def test_decision_version_is_unique_per_subject(self) -> None:
        self.assertIn("UNIQUE (subject_id, decision_version)", self.sql)

    def test_control_scope_and_version_are_structural(self) -> None:
        self.assertIn(
            "scope_kind = 'CPI_DOMAIN' AND event_occurrence_id IS NULL",
            self.sql,
        )
        self.assertIn(
            "scope_kind = 'EVENT_OCCURRENCE' AND event_occurrence_id IS NOT NULL",
            self.sql,
        )
        self.assertIn("economic_serving_control_domain_version", self.sql)
        self.assertIn("economic_serving_control_event_version", self.sql)

    def test_activation_rejects_orphan_and_future_dated_subject_requests(self) -> None:
        self.assertIn("interpretation subject has no matching typed evidence", self.sql)
        self.assertIn("interpretation request is not active yet", self.sql)

    def test_reason_codes_are_nonblank_canonical_tokens(self) -> None:
        self.assertGreaterEqual(self.sql.count("reason_code = BTRIM(reason_code)"), 2)

    def test_atomic_activation_functions_exist(self) -> None:
        self.assertIn("FUNCTION apply_interpretation_decision", self.sql)
        self.assertIn("FUNCTION apply_economic_serving_control", self.sql)
        self.assertIn("pg_advisory_xact_lock", self.sql)

    def test_spec_policy_is_at_least_one_approve_and_no_reject(self) -> None:
        self.assertIn("approve_count < 1 OR reject_count <> 0", self.sql)
        self.assertNotIn("approve_count <> 1", self.sql)

    def test_business_audit_is_written_inside_both_activation_functions(self) -> None:
        self.assertGreaterEqual(
            self.sql.count("INSERT INTO business_audit_events"),
            2,
        )
        self.assertIn("INTERPRETATION_DECISION_APPLIED", self.sql)
        self.assertIn("ECONOMIC_SERVING_CONTROL_APPLIED", self.sql)

    def test_append_only_history_has_mutation_guards(self) -> None:
        for table in (
            "interpretation_requests",
            "interpretation_approvals",
            "interpretation_decisions",
            "business_audit_events",
            "economic_serving_control_decisions",
        ):
            self.assertIn(f"ON {table}", self.sql)
        self.assertIn("reject_cpi_w1_immutable_evidence_mutation", self.sql)

    def test_actor_identity_and_fingerprint_shape_are_structural(self) -> None:
        self.assertIn("activation actor must be canonical", self.sql)
        self.assertIn("serving-control actor must be canonical", self.sql)
        self.assertIn(
            "verified_knowledge_fingerprint ~ '^[0-9a-f]{64}$'",
            self.sql,
        )
        self.assertIn(
            "event re-enable requires lowercase SHA-256 knowledge fingerprint",
            self.sql,
        )

    def test_reenable_guards_are_not_optional(self) -> None:
        self.assertIn(
            "event re-enable requires lowercase SHA-256 knowledge fingerprint",
            self.sql,
        )
        self.assertIn("domain re-enable requires verification reference", self.sql)


if __name__ == "__main__":
    unittest.main()
