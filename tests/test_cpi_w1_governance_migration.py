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

    def test_immutable_evidence_snapshot_is_acyclic_and_digest_bound(self) -> None:
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS promotion_release_evidence_snapshots",
            self.sql,
        )
        for field in (
            "release_subject_digest",
            "corpus_snapshot_digest",
            "expected_diff_approvals_digest",
            "replay_result_digest",
            "tested_job_contract_version",
            "tested_source_revision",
            "tested_workload_artifact_digest",
            "evidence_policy_version",
            "evidence_snapshot_digest",
        ):
            self.assertIn(field, self.sql)
        table = self.sql[
            self.sql.index("CREATE TABLE IF NOT EXISTS promotion_release_evidence_snapshots"):
            self.sql.index(";", self.sql.index("CREATE TABLE IF NOT EXISTS promotion_release_evidence_snapshots"))
        ]
        self.assertNotIn("gate_decision", table)
        self.assertIn("promotion_release_evidence_snapshots_immutable", self.sql)

    def test_release_authorization_material_grant_and_control_are_separate(self) -> None:
        for table in (
            "promotion_release_authorization_materials",
            "promotion_release_authorizations",
            "promotion_release_control_decisions",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)
        self.assertIn("authorization_material_digest", self.sql)
        self.assertIn("authorization_id UUID PRIMARY KEY", self.sql)
        self.assertIn("state IN ('APPROVED', 'REVOKED')", self.sql)
        self.assertIn("FUNCTION apply_promotion_release_control", self.sql)
        self.assertIn("FUNCTION resolve_promotion_release_authorization", self.sql)

    def test_release_authorization_binds_exact_subject_evidence_gate_and_executor(self) -> None:
        for field in (
            "release_subject_digest",
            "evidence_snapshot_id",
            "evidence_snapshot_digest",
            "gate_decision_digest",
            "authorization_policy_version",
            "executor_source_revision",
            "executor_workload_artifact_digest",
            "executor_job_contract_version",
            "review_ref",
            "review_digest",
        ):
            self.assertIn(field, self.sql)
        self.assertIn(
            "FOREIGN KEY (evidence_snapshot_id, release_subject_digest, evidence_snapshot_digest)",
            self.sql,
        )
        self.assertIn("ambiguous active promotion authorization", self.sql)

    def test_attempt_authorization_guard_matches_subject_executor_and_active_control(self) -> None:
        self.assertIn("enforce_cpi_attempt_release_authorization", self.sql)
        self.assertIn("promotion attempt requires exact release authorization", self.sql)
        self.assertIn("attempt release authorization subject mismatch", self.sql)
        self.assertIn("attempt executor provenance does not match authorization", self.sql)
        self.assertIn("attempt release authorization is not approved", self.sql)
        self.assertIn("FOREIGN KEY (release_authorization_id)", self.sql)

    def test_pause_boundaries_distinguish_admission_and_owned_work(self) -> None:
        self.assertIn("pause_pending_cpi_ingestion_work", self.sql)
        self.assertIn("p_attempt_reason_code TEXT", self.sql)
        self.assertIn("p_work_reason_code TEXT", self.sql)
        self.assertIn("'source_state', 'PENDING'", self.sql)
        self.assertIn("'attempt_reason_code', p_attempt_reason_code", self.sql)
        self.assertIn("'work_reason_code', p_work_reason_code", self.sql)

    def test_revoked_release_authorization_cannot_be_reactivated(self) -> None:
        self.assertIn("revoked promotion authorization cannot be reactivated", self.sql)
        self.assertIn("promotion release control expected version mismatch", self.sql)
        self.assertIn("promotion_release_authorization_materials_immutable", self.sql)
        self.assertIn("promotion_release_authorizations_immutable", self.sql)
        self.assertIn("promotion_release_control_decisions_immutable", self.sql)

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

    def test_interpretation_and_event_control_share_event_fence(self) -> None:
        self.assertIn("FUNCTION lock_cpi_governance_subject_events", self.sql)
        self.assertIn(
            "PERFORM lock_cpi_governance_subject_events(req.subject_id)",
            self.sql,
        )
        self.assertIn("'CPI_EVENT:' || affected_event::TEXT", self.sql)
        self.assertIn("'CPI_EVENT:' || p_event_occurrence_id::TEXT", self.sql)

    def test_pause_resume_is_audited_and_guarded(self) -> None:
        self.assertIn("FUNCTION pause_cpi_ingestion_work", self.sql)
        self.assertIn("FUNCTION resume_cpi_ingestion_work", self.sql)
        self.assertIn("INGESTION_WORK_PAUSED", self.sql)
        self.assertIn("INGESTION_WORK_RESUMED", self.sql)
        self.assertIn("work_item_id UUID REFERENCES ingestion_work_items", self.sql)
        self.assertIn("pause requires current claim ownership", self.sql)
        self.assertIn("resume requires canonical case reference", self.sql)

    def test_deferred_promotion_identity_includes_gate_snapshot(self) -> None:
        self.assertIn("event_payload ->> 'gate_fingerprint'", self.sql)
        self.assertIn("p_gate_fingerprint TEXT", self.sql)

    def test_promotion_deferred_audit_is_artifact_and_extractor_idempotent(self) -> None:
        self.assertIn("CPI_PROMOTION_DEFERRED", self.sql)
        self.assertIn("source_artifact_id UUID REFERENCES source_artifacts", self.sql)
        self.assertIn("business_audit_cpi_promotion_deferred_identity", self.sql)
        self.assertIn("FUNCTION record_cpi_promotion_deferred", self.sql)

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
