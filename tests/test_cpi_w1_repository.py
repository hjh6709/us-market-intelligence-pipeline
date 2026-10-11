import unittest
from pathlib import Path
from uuid import UUID

from src.cpi_w1_repository import (
    MAX_RETRY_DELAY_SECONDS,
    Claim,
    CpiW1Repository,
    _validate_reason,
)


SOURCE = Path("src/cpi_w1_repository.py").read_text(encoding="utf-8")


class CpiW1RepositoryTest(unittest.TestCase):
    def test_repository_owns_run_and_work_creation_boundaries(self) -> None:
        self.assertIn("def create_run(", SOURCE)
        self.assertIn("def create_work_item(", SOURCE)

    def test_repository_owns_immutable_artifact_target_boundaries(self) -> None:
        self.assertIn("def record_artifact_promotion_targets(", SOURCE)
        self.assertIn("def artifact_promotion_targets(", SOURCE)
        self.assertIn("cpi_artifact_promotion_targets", SOURCE)
        self.assertIn("ON CONFLICT DO NOTHING", SOURCE)
        self.assertIn(
            "same ingestion run idempotency key changed immutable metadata",
            SOURCE,
        )
        self.assertIn(
            "same ingestion work identity changed immutable metadata",
            SOURCE,
        )

    def test_promotion_work_uses_structured_release_subject_not_work_key(self) -> None:
        create_pos = SOURCE.index("def create_work_item(")
        create_body = SOURCE[create_pos:SOURCE.index("def existing_promotion", create_pos)]
        promotion_lookup = create_body[
            create_body.index('if execution_scope == "ECONOMIC_PROMOTE":', 200):
            create_body.index("else:", create_body.index("SELECT work_item_id"))
        ]
        self.assertIn("release_subject: ReleaseSubjectV1 | None", create_body)
        self.assertIn("release_subject_digest", create_body)
        self.assertIn("promotion_capability_id", create_body)
        self.assertIn("extractor_contract_version", create_body)
        self.assertIn("input_artifact_id=%s", create_body)
        self.assertIn("release_subject_digest=%s", create_body)
        self.assertNotIn("AND work_key=%s", promotion_lookup)

    def test_repository_persists_immutable_evidence_snapshot_by_semantic_digest(self) -> None:
        self.assertIn("def create_promotion_evidence_snapshot(", SOURCE)
        self.assertIn("promotion_release_evidence_snapshots", SOURCE)
        self.assertIn("evidence_snapshot_digest", SOURCE)
        self.assertIn("same evidence snapshot digest changed immutable material", SOURCE)

    def test_repository_owns_release_authorization_boundaries(self) -> None:
        for method in (
            "def create_promotion_authorization_material(",
            "def create_promotion_authorization(",
            "def apply_promotion_release_control(",
            "def resolve_promotion_release_authorization(",
        ):
            self.assertIn(method, SOURCE)
        self.assertIn("V2 authorization material changed immutable binding", SOURCE)
        self.assertIn("LEGACY_AUTHORIZATION_RETIRED", SOURCE)
        self.assertIn("SELECT apply_promotion_release_control", SOURCE)
        self.assertIn("SELECT resolve_promotion_release_authorization", SOURCE)

    def test_claim_sql_uses_skip_locked_and_due_or_expired_work(self) -> None:
        self.assertIn("FOR UPDATE OF w SKIP LOCKED", SOURCE)
        self.assertIn("w.next_claim_at <= CURRENT_TIMESTAMP", SOURCE)
        self.assertIn("w.lease_until <= clock_timestamp()", SOURCE)

    def test_claim_binds_exact_authorization_and_executor_to_attempt(self) -> None:
        claim_pos = SOURCE.index("def claim_work_item(")
        claim_body = SOURCE[claim_pos:SOURCE.index("def assert_current_claim", claim_pos)]
        self.assertIn("executor: ExecutorProvenanceV1", claim_body)
        self.assertIn("resolve_promotion_release_authorization", claim_body)
        for field in (
            "release_authorization_id",
            "executor_source_revision",
            "executor_workload_artifact_digest",
            "executor_job_contract_version",
        ):
            self.assertIn(field, claim_body)
        self.assertNotIn("str(input_artifact_id)", claim_body)

    def test_claim_reclaim_and_heartbeat_recheck_runtime_authorization(self) -> None:
        claim_pos = SOURCE.index("def claim_work_item(")
        claim_body = SOURCE[claim_pos:SOURCE.index("def assert_current_claim", claim_pos)]
        renew_pos = SOURCE.index("def renew_claim(")
        renew_body = SOURCE[renew_pos:SOURCE.index("def terminalize_claim", renew_pos)]
        self.assertIn("pause_pending_cpi_ingestion_work", claim_body)
        self.assertIn("pause_reclaimed_cpi_ingestion_work", claim_body)
        self.assertIn("RELEASE_AUTHORIZATION_UNAVAILABLE", claim_body)
        self.assertIn("LEASE_EXPIRED_RECLAIM", claim_body)
        self.assertIn("assert_current_promotion_authorization", renew_body)
        self.assertNotIn("resolve_promotion_release_authorization", renew_body)
        self.assertIn("RELEASE_AUTHORIZATION_REVOKED", renew_body)

    def test_final_commit_exposes_global_fences_and_exact_authorization_check(self) -> None:
        self.assertIn("def lock_cpi_domain_shared", SOURCE)
        self.assertIn("def lock_cpi_release_subject", SOURCE)
        self.assertIn("def assert_current_promotion_authorization", SOURCE)
        self.assertIn("pg_advisory_xact_lock_shared", SOURCE)
        authorization_pos = SOURCE.index("def assert_current_promotion_authorization")
        authorization_body = SOURCE[
            authorization_pos:SOURCE.index("def renew_claim", authorization_pos)
        ]
        self.assertIn("promotion_release_control_decisions", authorization_body)
        self.assertIn('row[5] != "APPROVED"', authorization_body)
        self.assertIn("promotion_release_authorization_materials", authorization_body)
        control_pos = SOURCE.index("def apply_promotion_release_control")
        control_body = SOURCE[
            control_pos:SOURCE.index(
                "def resolve_promotion_release_authorization",
                control_pos,
            )
        ]
        self.assertLess(
            control_body.index("lock_cpi_domain_shared"),
            control_body.index("lock_cpi_release_subject"),
        )
        self.assertLess(
            control_body.index("lock_cpi_release_subject"),
            control_body.index("SELECT apply_promotion_release_control"),
        )

    def test_pause_resume_uses_guarded_database_boundary(self) -> None:
        self.assertIn("SELECT pause_pending_cpi_ingestion_work", SOURCE)
        self.assertIn("SELECT pause_cpi_ingestion_work", SOURCE)
        self.assertIn("SELECT resume_cpi_ingestion_work", SOURCE)
        self.assertIn("attempt_reason_code", SOURCE)
        self.assertIn("work_reason_code", SOURCE)
        self.assertNotIn("SET state='PAUSED'", SOURCE)

    def test_artifact_recording_accepts_caller_owned_identity(self) -> None:
        self.assertIn("artifact_id: UUID | None = None", SOURCE)
        self.assertIn("artifact_id = artifact_id or uuid4()", SOURCE)
        self.assertIn("changed artifact identity", SOURCE)

    def test_claim_can_filter_by_work_family_prefix(self) -> None:
        self.assertIn("CLAIM_SELECT_PREFIX_SQL", SOURCE)
        self.assertIn("LEFT(w.work_key, CHAR_LENGTH(%s)) = %s", SOURCE)
        self.assertNotIn("w.work_key LIKE %s", SOURCE)
        self.assertIn("work_key_prefix", SOURCE)

    def test_unfiltered_claim_does_not_bind_nullable_prefix_parameters(self) -> None:
        self.assertIn("(execution_scope,)", SOURCE)
        self.assertIn("if work_key_prefix is None:", SOURCE)

    def test_current_claim_fencing_uses_generation_token_lease_and_attempt(self) -> None:
        for fragment in (
            "w.claim_generation = %s",
            "w.claim_token = %s",
            "w.lease_until > clock_timestamp()",
            "a.state = 'RUNNING'",
            "a.attempt_number = w.claim_generation",
        ):
            self.assertIn(fragment, SOURCE)

    def test_claim_starts_parent_run_with_database_clock(self) -> None:
        self.assertIn("state='RUNNING'", SOURCE)
        self.assertIn("started_at=COALESCE(started_at, CURRENT_TIMESTAMP)", SOURCE)

    def test_reclaim_closes_expired_attempt_before_new_attempt(self) -> None:
        self.assertIn("LEASE_EXPIRED_RECLAIM", SOURCE)
        close_pos = SOURCE.index("LEASE_EXPIRED_RECLAIM")
        new_attempt_pos = SOURCE.index("INSERT INTO ingestion_attempts", close_pos)
        self.assertLess(close_pos, new_attempt_pos)

    def test_retry_delay_is_policy_bounded_and_database_clock_owned(self) -> None:
        retry_pos = SOURCE.index("def retry_claim")
        retry_body = SOURCE[retry_pos:]
        self.assertIn("retry_after_seconds", retry_body)
        self.assertIn("MAX_RETRY_DELAY_SECONDS", retry_body)
        self.assertIn(
            "next_claim_at=CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')",
            retry_body,
        )
        self.assertEqual(MAX_RETRY_DELAY_SECONDS, 86400)

    def test_artifact_retry_compares_all_immutable_metadata(self) -> None:
        for fragment in (
            "artifact_contract_kind",
            "source_contract_version",
            "retrieval_url",
            "content_type",
            "captured_at",
            "content_state",
            "storage_uri",
            "storage_generation",
            "same collector attempt/locator retry changed immutable artifact metadata",
        ):
            self.assertIn(fragment, SOURCE)

    def test_retry_closes_attempt_before_returning_work_to_pending(self) -> None:
        retry_pos = SOURCE.index("def retry_claim")
        retry_body = SOURCE[retry_pos:]
        self.assertLess(
            retry_body.index("UPDATE ingestion_attempts"),
            retry_body.index("UPDATE ingestion_work_items"),
        )
        self.assertIn("state='PENDING'", retry_body)
        self.assertIn("reason_code=NULL", retry_body)

    def test_renew_claim_preserves_generation_and_token(self) -> None:
        self.assertIn("def renew_claim(", SOURCE)
        self.assertIn("w.claim_generation=%s", SOURCE)
        self.assertIn("w.claim_token=%s", SOURCE)
        self.assertIn("w.lease_until > clock_timestamp()", SOURCE)
        self.assertIn("a.state='RUNNING'", SOURCE)
        self.assertIn("GREATEST(", SOURCE)

    def test_terminalization_orders_attempt_before_work(self) -> None:
        attempt_pos = SOURCE.index("UPDATE ingestion_attempts a")
        work_pos = SOURCE.index("UPDATE ingestion_work_items", attempt_pos)
        self.assertLess(attempt_pos, work_pos)

    def test_failed_terminalization_requires_explicit_abandon_boundary(self) -> None:
        self.assertIn(
            "terminal FAILED is irreversible; use abandon_claim explicitly",
            SOURCE,
        )
        self.assertIn("def abandon_claim(", SOURCE)
        self.assertIn("irreversible_failure=True", SOURCE)

    def test_reason_contract(self) -> None:
        _validate_reason("SUCCEEDED", None)
        _validate_reason("FAILED", "SOURCE_MISMATCH")
        with self.assertRaises(ValueError):
            _validate_reason("FAILED", None)
        with self.assertRaises(ValueError):
            _validate_reason("FAILED", "lowercase")
        with self.assertRaises(ValueError):
            _validate_reason("SUCCEEDED", "SHOULD_NOT_EXIST")

    def test_claim_is_immutable_value_object(self) -> None:
        claim = Claim(
            work_item_id=UUID(int=1),
            run_id=UUID(int=2),
            attempt_id=UUID(int=3),
            execution_scope="ECONOMIC_PROMOTE",
            claim_generation=1,
            claim_token=UUID(int=4),
            input_artifact_id=UUID(int=5),
            work_key="CPI_RELEASE_ENVELOPE_PROMOTE:test",
            release_authorization_id=UUID(int=6),
            release_control_decision_id=UUID(int=7),
            executor_source_revision="deadbeef",
            executor_workload_artifact_digest="7" * 64,
            executor_job_contract_version="cpi-w1-promoter-v1",
        )
        with self.assertRaises(Exception):
            claim.claim_generation = 2  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
