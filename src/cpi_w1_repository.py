"""PostgreSQL repository primitives for CPI W1 fenced work ownership."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from src.cpi_w1_authorization import (
    ExecutorProvenanceV1,
    PromotionAuthorizationMaterialV1,
)
from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotV1
from src.cpi_w1_release_subject import ReleaseSubjectV1


_REASON_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
MAX_RETRY_DELAY_SECONDS = 24 * 60 * 60


class StaleClaimError(RuntimeError):
    """The caller no longer owns the current work generation."""


class RepositoryInvariantError(RuntimeError):
    """Durable repository state contradicts the W1 contract."""


@dataclass(frozen=True)
class Claim:
    work_item_id: UUID
    run_id: UUID
    attempt_id: UUID
    execution_scope: str
    claim_generation: int
    claim_token: UUID
    input_artifact_id: UUID | None
    work_key: str
    release_authorization_id: UUID | None
    release_control_decision_id: UUID | None
    executor_source_revision: str
    executor_workload_artifact_digest: str
    executor_job_contract_version: str
    promotion_capability_id: str | None = None
    extractor_contract_version: str | None = None
    release_subject_digest: str | None = None


CLAIM_SELECT_SQL = """
SELECT
    w.work_item_id,
    w.run_id,
    w.input_artifact_id,
    w.claim_generation,
    w.state,
    w.claim_token,
    w.work_key,
    w.promotion_capability_id,
    w.extractor_contract_version,
    w.release_subject_digest
  FROM ingestion_work_items w
  JOIN ingestion_runs r
    ON r.run_id = w.run_id
   AND r.execution_scope = w.execution_scope
   AND r.data_domain = w.data_domain
 WHERE w.execution_scope = %s
   AND w.data_domain = 'ECONOMIC'
   AND r.state <> 'TERMINAL'
   AND (
        (
            w.state = 'PENDING'
            AND (w.next_claim_at IS NULL OR w.next_claim_at <= CURRENT_TIMESTAMP)
        )
        OR
        (
            w.state = 'CLAIMED'
            AND w.lease_until <= CURRENT_TIMESTAMP
        )
   )
 ORDER BY COALESCE(w.next_claim_at, w.created_at), w.created_at, w.work_item_id
 FOR UPDATE OF w SKIP LOCKED
 LIMIT 1
"""

CLAIM_SELECT_PREFIX_SQL = CLAIM_SELECT_SQL.replace(
    "   AND (\n        (\n            w.state = 'PENDING'",
    "   AND LEFT(w.work_key, CHAR_LENGTH(%s)) = %s\n"
    "   AND (\n        (\n            w.state = 'PENDING'",
)

CURRENT_CLAIM_SQL = """
SELECT
    w.run_id,
    w.input_artifact_id,
    w.work_key,
    w.promotion_capability_id,
    w.extractor_contract_version,
    w.release_subject_digest,
    a.state,
    a.attempt_number,
    a.release_authorization_id,
    a.release_control_decision_id,
    a.executor_source_revision,
    a.executor_workload_artifact_digest,
    a.executor_job_contract_version
  FROM ingestion_work_items w
  JOIN ingestion_attempts a
    ON a.attempt_id = %s
   AND a.work_item_id = w.work_item_id
   AND a.execution_scope = w.execution_scope
   AND a.data_domain = w.data_domain
 WHERE w.work_item_id = %s
   AND w.execution_scope = %s
   AND w.data_domain = 'ECONOMIC'
   AND w.state = 'CLAIMED'
   AND w.claim_generation = %s
   AND w.claim_token = %s
   AND w.lease_until > CURRENT_TIMESTAMP
   AND a.state = 'RUNNING'
   AND a.attempt_number = w.claim_generation
 FOR UPDATE OF w
"""


def _validate_reason(outcome: str, reason_code: str | None) -> None:
    if outcome == "SUCCEEDED":
        if reason_code is not None:
            raise ValueError("SUCCEEDED must not carry reason_code")
        return
    if outcome not in {"FAILED", "QUARANTINED", "DATA_NOT_AVAILABLE"}:
        raise ValueError(f"unsupported executed work outcome: {outcome}")
    if reason_code is None or _REASON_RE.fullmatch(reason_code) is None:
        raise ValueError("abnormal outcome requires canonical uppercase reason_code")


class CpiW1Repository:
    def create_promotion_evidence_snapshot(
        self,
        connection: Any,
        *,
        snapshot: PromotionEvidenceSnapshotV1,
        created_by_subject: str,
    ) -> UUID:
        if (
            not created_by_subject
            or created_by_subject != created_by_subject.strip()
        ):
            raise ValueError("created_by_subject must be canonical and non-empty")
        candidate_id = uuid4()
        expected = (
            snapshot.release_subject_digest,
            snapshot.corpus_snapshot_digest,
            snapshot.expected_diff_approvals_digest,
            snapshot.replay_result_digest,
            snapshot.tested_job_contract_version,
            snapshot.tested_source_revision,
            snapshot.tested_workload_artifact_digest,
            snapshot.evidence_policy_version,
        )
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO promotion_release_evidence_snapshots (
                    evidence_snapshot_id, schema_version,
                    release_subject_digest, corpus_snapshot_digest,
                    expected_diff_approvals_digest, replay_result_digest,
                    tested_job_contract_version, tested_source_revision,
                    tested_workload_artifact_digest, evidence_policy_version,
                    evidence_snapshot_digest, created_by_subject
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (evidence_snapshot_digest) DO NOTHING
                """,
                (
                    candidate_id,
                    snapshot.SCHEMA,
                    *expected,
                    snapshot.evidence_snapshot_digest,
                    created_by_subject,
                ),
            )
            row = connection.execute(
                """
                SELECT evidence_snapshot_id, release_subject_digest,
                       corpus_snapshot_digest, expected_diff_approvals_digest,
                       replay_result_digest, tested_job_contract_version,
                       tested_source_revision, tested_workload_artifact_digest,
                       evidence_policy_version
                  FROM promotion_release_evidence_snapshots
                 WHERE evidence_snapshot_digest=%s
                """,
                (snapshot.evidence_snapshot_digest,),
            ).fetchone()
            if row is None:
                raise RepositoryInvariantError("evidence snapshot did not converge")
            if tuple(row[1:]) != expected:
                raise RepositoryInvariantError(
                    "same evidence snapshot digest changed immutable material"
                )
            return row[0]

    def create_promotion_authorization_material(
        self,
        connection: Any,
        *,
        evidence_snapshot_id: UUID,
        material: PromotionAuthorizationMaterialV1,
        created_by_subject: str,
    ) -> UUID:
        if not created_by_subject or created_by_subject != created_by_subject.strip():
            raise ValueError("created_by_subject must be canonical and non-empty")
        candidate_id = uuid4()
        expected = (
            material.release_subject_digest,
            evidence_snapshot_id,
            material.evidence_snapshot_digest,
            material.gate_decision_digest,
            material.gate_policy_version,
            material.authorization_policy_version,
            material.executor_source_revision,
            material.executor_workload_artifact_digest,
            material.executor_job_contract_version,
            material.review_ref,
            material.review_digest,
        )
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO promotion_release_authorization_materials (
                    authorization_material_id, release_subject_digest,
                    evidence_snapshot_id, evidence_snapshot_digest,
                    gate_decision_digest, gate_policy_version,
                    authorization_policy_version, executor_source_revision,
                    executor_workload_artifact_digest,
                    executor_job_contract_version, review_ref, review_digest,
                    authorization_material_digest, created_by_subject
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (authorization_material_digest) DO NOTHING
                """,
                (
                    candidate_id,
                    *expected,
                    material.authorization_material_digest,
                    created_by_subject,
                ),
            )
            row = connection.execute(
                """
                SELECT authorization_material_id, release_subject_digest,
                       evidence_snapshot_id, evidence_snapshot_digest,
                       gate_decision_digest, gate_policy_version,
                       authorization_policy_version, executor_source_revision,
                       executor_workload_artifact_digest,
                       executor_job_contract_version, review_ref, review_digest
                  FROM promotion_release_authorization_materials
                 WHERE authorization_material_digest=%s
                """,
                (material.authorization_material_digest,),
            ).fetchone()
            if row is None:
                raise RepositoryInvariantError(
                    "authorization material did not converge"
                )
            if tuple(row[1:]) != expected:
                raise RepositoryInvariantError(
                    "same authorization material digest changed immutable material"
                )
            return row[0]

    def create_promotion_authorization(
        self,
        connection: Any,
        *,
        authorization_material_id: UUID,
        release_subject_digest: str,
        grant_reason_code: str,
        created_by_subject: str,
    ) -> UUID:
        if re.fullmatch(r"[0-9a-f]{64}", release_subject_digest) is None:
            raise ValueError("release_subject_digest must be lowercase SHA-256")
        if _REASON_RE.fullmatch(grant_reason_code) is None:
            raise ValueError("grant_reason_code must be canonical uppercase token")
        if not created_by_subject or created_by_subject != created_by_subject.strip():
            raise ValueError("created_by_subject must be canonical and non-empty")
        authorization_id = uuid4()
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO promotion_release_authorizations (
                    authorization_id, authorization_material_id,
                    release_subject_digest, grant_reason_code, created_by_subject
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    authorization_id,
                    authorization_material_id,
                    release_subject_digest,
                    grant_reason_code,
                    created_by_subject,
                ),
            )
        return authorization_id

    def apply_promotion_release_control(
        self,
        connection: Any,
        *,
        authorization_id: UUID,
        expected_control_version: int,
        state: str,
        reason_code: str,
        actor_subject: str,
        review_ref: str,
        review_digest: str,
    ) -> UUID:
        with connection.transaction():
            subject_row = connection.execute(
                """
                SELECT release_subject_digest
                  FROM promotion_release_authorizations
                 WHERE authorization_id=%s
                """,
                (authorization_id,),
            ).fetchone()
            if subject_row is None:
                raise RepositoryInvariantError(
                    "promotion authorization does not exist"
                )
            self.lock_cpi_domain_shared(connection)
            self.lock_cpi_release_subject(connection, subject_row[0])
            row = connection.execute(
                """
                SELECT apply_promotion_release_control(
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    authorization_id,
                    expected_control_version,
                    state,
                    reason_code,
                    actor_subject,
                    review_ref,
                    review_digest,
                ),
            ).fetchone()
            if row is None:
                raise RepositoryInvariantError("release control returned no identity")
            return row[0]

    def resolve_promotion_release_authorization(
        self,
        connection: Any,
        *,
        release_subject_digest: str,
        executor: ExecutorProvenanceV1,
    ) -> UUID | None:
        row = connection.execute(
            """
            SELECT resolve_promotion_release_authorization(%s, %s, %s, %s)
            """,
            (
                release_subject_digest,
                executor.source_revision,
                executor.workload_artifact_digest,
                executor.job_contract_version,
            ),
        ).fetchone()
        if row is None:
            raise RepositoryInvariantError("authorization resolver returned no row")
        return row[0]

    def create_run(
        self,
        connection: Any,
        *,
        execution_scope: str,
        job_type: str,
        trigger_type: str,
        run_mode: str,
        source_revision: str,
        workload_artifact_digest: str,
        job_contract_version: str,
        config_fingerprint: str,
        trigger_idempotency_key: str | None = None,
        scheduled_for: datetime | None = None,
        replay_of_run_id: UUID | None = None,
    ) -> UUID:
        if execution_scope not in {"ECONOMIC_COLLECT", "ECONOMIC_PROMOTE"}:
            raise ValueError("unsupported execution_scope")
        if run_mode not in {"LIVE", "BACKFILL", "REPLAY"}:
            raise ValueError("unsupported run_mode")
        for label, value in (
            ("job_type", job_type),
            ("trigger_type", trigger_type),
            ("source_revision", source_revision),
            ("workload_artifact_digest", workload_artifact_digest),
            ("job_contract_version", job_contract_version),
            ("config_fingerprint", config_fingerprint),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{label} must be canonical and non-empty")
        if trigger_idempotency_key is not None and (
            not trigger_idempotency_key
            or trigger_idempotency_key != trigger_idempotency_key.strip()
        ):
            raise ValueError("trigger_idempotency_key must be canonical when supplied")
        if scheduled_for is not None and (
            scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None
        ):
            raise ValueError("scheduled_for must be timezone-aware")
        if (run_mode == "REPLAY") != (replay_of_run_id is not None):
            raise ValueError("REPLAY requires replay_of_run_id and other modes forbid it")

        candidate_id = uuid4()
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, execution_scope, data_domain, job_type, trigger_type,
                    run_mode, trigger_idempotency_key, scheduled_for,
                    replay_of_run_id, source_revision, workload_artifact_digest,
                    job_contract_version, config_fingerprint
                ) VALUES (
                    %s, %s, 'ECONOMIC', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
                """,
                (
                    candidate_id,
                    execution_scope,
                    job_type,
                    trigger_type,
                    run_mode,
                    trigger_idempotency_key,
                    scheduled_for,
                    replay_of_run_id,
                    source_revision,
                    workload_artifact_digest,
                    job_contract_version,
                    config_fingerprint,
                ),
            )
            if trigger_idempotency_key is None:
                row = connection.execute(
                    """
                    SELECT run_id, execution_scope, job_type, trigger_type,
                           run_mode, trigger_idempotency_key, scheduled_for,
                           replay_of_run_id, source_revision,
                           workload_artifact_digest, job_contract_version,
                           config_fingerprint
                      FROM ingestion_runs
                     WHERE run_id=%s
                    """,
                    (candidate_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT run_id, execution_scope, job_type, trigger_type,
                           run_mode, trigger_idempotency_key, scheduled_for,
                           replay_of_run_id, source_revision,
                           workload_artifact_digest, job_contract_version,
                           config_fingerprint
                      FROM ingestion_runs
                     WHERE data_domain='ECONOMIC'
                       AND execution_scope=%s
                       AND job_type=%s
                       AND trigger_type=%s
                       AND trigger_idempotency_key=%s
                    """,
                    (
                        execution_scope,
                        job_type,
                        trigger_type,
                        trigger_idempotency_key,
                    ),
                ).fetchone()
            if row is None:
                raise RepositoryInvariantError("ingestion run identity did not converge")
            expected = (
                execution_scope,
                job_type,
                trigger_type,
                run_mode,
                trigger_idempotency_key,
                scheduled_for,
                replay_of_run_id,
                source_revision,
                workload_artifact_digest,
                job_contract_version,
                config_fingerprint,
            )
            if tuple(row[1:]) != expected:
                raise RepositoryInvariantError(
                    "same ingestion run idempotency key changed immutable metadata"
                )
            return row[0]

    def create_work_item(
        self,
        connection: Any,
        *,
        run_id: UUID,
        execution_scope: str,
        work_key: str,
        input_artifact_id: UUID | None = None,
        release_subject: ReleaseSubjectV1 | None = None,
    ) -> UUID:
        if execution_scope not in {"ECONOMIC_COLLECT", "ECONOMIC_PROMOTE"}:
            raise ValueError("unsupported execution_scope")
        if not work_key or work_key != work_key.strip():
            raise ValueError("work_key must be canonical and non-empty")
        if execution_scope == "ECONOMIC_PROMOTE":
            if input_artifact_id is None or release_subject is None:
                raise ValueError(
                    "promotion work requires input_artifact_id and release_subject"
                )
        elif input_artifact_id is not None or release_subject is not None:
            raise ValueError("collection work must not carry promotion identity")

        promotion_capability_id = (
            release_subject.promotion_capability_id if release_subject else None
        )
        extractor_contract_version = (
            release_subject.extractor_contract_version if release_subject else None
        )
        release_subject_digest = (
            release_subject.release_subject_digest if release_subject else None
        )

        candidate_id = uuid4()
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO ingestion_work_items (
                    work_item_id, run_id, execution_scope, data_domain,
                    work_key, input_artifact_id, promotion_capability_id,
                    extractor_contract_version, release_subject_digest
                ) VALUES (%s, %s, %s, 'ECONOMIC', %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    candidate_id,
                    run_id,
                    execution_scope,
                    work_key,
                    input_artifact_id,
                    promotion_capability_id,
                    extractor_contract_version,
                    release_subject_digest,
                ),
            )
            if execution_scope == "ECONOMIC_PROMOTE":
                row = connection.execute(
                    """
                    SELECT work_item_id, run_id, execution_scope,
                           input_artifact_id, work_key,
                           promotion_capability_id, extractor_contract_version,
                           release_subject_digest
                      FROM ingestion_work_items
                     WHERE execution_scope='ECONOMIC_PROMOTE'
                       AND input_artifact_id=%s
                       AND release_subject_digest=%s
                    """,
                    (input_artifact_id, release_subject_digest),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT work_item_id, run_id, execution_scope,
                           input_artifact_id, work_key,
                           promotion_capability_id, extractor_contract_version,
                           release_subject_digest
                      FROM ingestion_work_items
                     WHERE run_id=%s AND work_key=%s
                    """,
                    (run_id, work_key),
                ).fetchone()
            if row is None:
                raise RepositoryInvariantError("ingestion work identity did not converge")
            expected = (
                execution_scope,
                input_artifact_id,
                work_key,
                promotion_capability_id,
                extractor_contract_version,
                release_subject_digest,
            )
            if tuple(row[2:]) != expected:
                raise RepositoryInvariantError(
                    "same ingestion work identity changed immutable metadata"
                )
            return row[0]

    def existing_promotion_work_items(
        self,
        connection: Any,
        *,
        artifact_id: UUID,
        work_keys: tuple[str, ...],
    ) -> dict[str, UUID]:
        if not work_keys:
            return {}
        rows = connection.execute(
            """
            SELECT work_key, work_item_id
              FROM ingestion_work_items
             WHERE execution_scope='ECONOMIC_PROMOTE'
               AND input_artifact_id=%s
               AND work_key = ANY(%s::text[])
            """,
            (artifact_id, list(work_keys)),
        ).fetchall()
        result = {row[0]: row[1] for row in rows}
        if len(result) != len(rows):
            raise RepositoryInvariantError(
                "promotion work identity resolved to duplicate work keys"
            )
        return result

    @staticmethod
    def lock_cpi_domain_shared(connection: Any) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s, 0))",
            ("CPI_DOMAIN",),
        )

    @staticmethod
    def lock_cpi_domain_exclusive(connection: Any) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("CPI_DOMAIN",),
        )

    @staticmethod
    def lock_cpi_release_subject(
        connection: Any,
        release_subject_digest: str,
    ) -> None:
        if (
            not isinstance(release_subject_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", release_subject_digest) is None
        ):
            raise ValueError("release_subject_digest must be lowercase SHA-256")
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"CPI_RELEASE_SUBJECT:{release_subject_digest}",),
        )

    @staticmethod
    def lock_cpi_event(connection: Any, event_occurrence_id: UUID) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"CPI_EVENT:{event_occurrence_id}",),
        )

    @staticmethod
    def lock_promotion_scheduling(connection: Any, artifact_id: UUID) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"CPI_PROMOTION_SCHEDULE:{artifact_id}",),
        )

    def record_promotion_deferred(
        self,
        connection: Any,
        *,
        artifact_id: UUID,
        promotion_capability_id: str,
        release_subject_digest: str,
        extractor_contract_version: str,
        evidence_snapshot_digest: str,
        gate_decision_digest: str,
        reason_code: str,
        review_ref: str,
        gate_fingerprint: str,
    ) -> UUID:
        if _REASON_RE.fullmatch(promotion_capability_id) is None:
            raise ValueError("promotion_capability_id must be canonical")
        if re.fullmatch(r"[0-9a-f]{64}", release_subject_digest) is None:
            raise ValueError("release_subject_digest must be lowercase SHA-256")
        if _REASON_RE.fullmatch(reason_code) is None:
            raise ValueError("promotion deferred reason must be canonical")
        if not extractor_contract_version or extractor_contract_version != extractor_contract_version.strip():
            raise ValueError("extractor_contract_version must be canonical")
        if not review_ref or review_ref != review_ref.strip():
            raise ValueError("review_ref must be canonical")
        if re.fullmatch(r"[0-9a-f]{64}", evidence_snapshot_digest) is None:
            raise ValueError("evidence_snapshot_digest must be lowercase SHA-256")
        if re.fullmatch(r"[0-9a-f]{64}", gate_decision_digest) is None:
            raise ValueError("gate_decision_digest must be lowercase SHA-256")
        if re.fullmatch(r"[0-9a-f]{64}", gate_fingerprint) is None:
            raise ValueError("gate_fingerprint must be lowercase SHA-256")
        with connection.transaction():
            row = connection.execute(
                """
                SELECT record_cpi_promotion_deferred(
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    artifact_id,
                    promotion_capability_id,
                    release_subject_digest,
                    extractor_contract_version,
                    evidence_snapshot_digest,
                    gate_decision_digest,
                    reason_code,
                    review_ref,
                    gate_fingerprint,
                ),
            ).fetchone()
            if row is None:
                raise RepositoryInvariantError("promotion deferred audit returned no id")
            return row[0]

    def claim_work_item(
        self,
        connection: Any,
        *,
        execution_scope: str,
        executor: ExecutorProvenanceV1,
        lease_seconds: int = 300,
        work_key_prefix: str | None = None,
    ) -> Claim | None:
        if execution_scope not in {"ECONOMIC_COLLECT", "ECONOMIC_PROMOTE"}:
            raise ValueError("unsupported execution_scope")
        if lease_seconds < 1 or lease_seconds > 3600:
            raise ValueError("lease_seconds must be in [1, 3600]")

        with connection.transaction():
            if work_key_prefix is None:
                row = connection.execute(
                    CLAIM_SELECT_SQL,
                    (execution_scope,),
                ).fetchone()
            else:
                row = connection.execute(
                    CLAIM_SELECT_PREFIX_SQL,
                    (execution_scope, work_key_prefix, work_key_prefix),
                ).fetchone()
            if row is None:
                return None

            (
                work_item_id,
                run_id,
                input_artifact_id,
                generation,
                previous_state,
                previous_claim_token,
                work_key,
                promotion_capability_id,
                extractor_contract_version,
                release_subject_digest,
            ) = row
            previous_generation = int(generation)
            release_authorization_id: UUID | None = None
            release_control_decision_id: UUID | None = None

            expired_attempt_id: UUID | None = None
            if previous_state == "CLAIMED":
                closed = connection.execute(
                    """
                    UPDATE ingestion_attempts
                       SET state='TERMINAL',
                           outcome='FAILED',
                           reason_code='LEASE_EXPIRED_RECLAIM',
                           finished_at=CURRENT_TIMESTAMP
                     WHERE work_item_id=%s
                       AND attempt_number=%s
                       AND state='RUNNING'
                     RETURNING attempt_id
                    """,
                    (work_item_id, previous_generation),
                ).fetchone()
                if closed is None:
                    raise RepositoryInvariantError(
                        "expired claimed work has no running prior attempt"
                    )
                expired_attempt_id = closed[0]

            if execution_scope == "ECONOMIC_PROMOTE":
                if release_subject_digest is None:
                    raise RepositoryInvariantError(
                        "promotion work has no structured release subject"
                    )
                release_authorization_id = self.resolve_promotion_release_authorization(
                    connection,
                    release_subject_digest=release_subject_digest,
                    executor=executor,
                )
                if release_authorization_id is None:
                    if previous_state == "PENDING":
                        connection.execute(
                            """
                            SELECT pause_pending_cpi_ingestion_work(
                                %s, 'RELEASE_AUTHORIZATION_UNAVAILABLE',
                                'system:cpi-claim-admission', NULL
                            )
                            """,
                            (work_item_id,),
                        )
                    else:
                        connection.execute(
                            """
                            SELECT pause_reclaimed_cpi_ingestion_work(
                                %s, %s, %s, %s,
                                'RELEASE_AUTHORIZATION_UNAVAILABLE',
                                'system:cpi-claim-admission', NULL
                            )
                            """,
                            (
                                work_item_id,
                                expired_attempt_id,
                                previous_generation,
                                previous_claim_token,
                            ),
                        )
                    return None
                control_row = connection.execute(
                    """
                    SELECT control_decision_id
                      FROM promotion_release_control_decisions
                     WHERE authorization_id=%s
                     ORDER BY control_version DESC
                     LIMIT 1
                    """,
                    (release_authorization_id,),
                ).fetchone()
                if control_row is None:
                    raise RepositoryInvariantError(
                        "promotion authorization has no effective control decision"
                    )
                release_control_decision_id = control_row[0]
            generation = previous_generation + 1
            claim_token = uuid4()
            attempt_id = uuid4()

            connection.execute(
                """
                UPDATE ingestion_runs
                   SET state='RUNNING',
                       started_at=COALESCE(started_at, CURRENT_TIMESTAMP)
                 WHERE run_id=%s
                   AND state='CREATED'
                """,
                (run_id,),
            )

            updated = connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='CLAIMED',
                       claim_generation=%s,
                       claim_token=%s,
                       lease_until=CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'),
                       next_claim_at=NULL
                 WHERE work_item_id=%s
                 RETURNING work_item_id
                """,
                (generation, claim_token, lease_seconds, work_item_id),
            ).fetchone()
            if updated is None:
                raise StaleClaimError("claim disappeared while locked")

            connection.execute(
                """
                INSERT INTO ingestion_attempts (
                    attempt_id, work_item_id, execution_scope, data_domain,
                    attempt_number, release_authorization_id,
                    release_control_decision_id, executor_source_revision,
                    executor_workload_artifact_digest,
                    executor_job_contract_version
                ) VALUES (%s, %s, %s, 'ECONOMIC', %s, %s, %s, %s, %s, %s)
                """,
                (
                    attempt_id,
                    work_item_id,
                    execution_scope,
                    generation,
                    release_authorization_id,
                    release_control_decision_id,
                    executor.source_revision,
                    executor.workload_artifact_digest,
                    executor.job_contract_version,
                ),
            )

            return Claim(
                work_item_id=work_item_id,
                run_id=run_id,
                attempt_id=attempt_id,
                execution_scope=execution_scope,
                claim_generation=generation,
                claim_token=claim_token,
                input_artifact_id=input_artifact_id,
                work_key=work_key,
                release_authorization_id=release_authorization_id,
                release_control_decision_id=release_control_decision_id,
                executor_source_revision=executor.source_revision,
                executor_workload_artifact_digest=executor.workload_artifact_digest,
                executor_job_contract_version=executor.job_contract_version,
                promotion_capability_id=promotion_capability_id,
                extractor_contract_version=extractor_contract_version,
                release_subject_digest=release_subject_digest,
            )

    def assert_current_claim(self, connection: Any, claim: Claim) -> tuple[UUID, UUID | None]:
        row = connection.execute(
            CURRENT_CLAIM_SQL,
            (
                claim.attempt_id,
                claim.work_item_id,
                claim.execution_scope,
                claim.claim_generation,
                claim.claim_token,
            ),
        ).fetchone()
        if row is None:
            raise StaleClaimError("claim is stale, expired, or no longer running")
        (
            run_id,
            input_artifact_id,
            work_key,
            promotion_capability_id,
            extractor_contract_version,
            release_subject_digest,
            attempt_state,
            attempt_number,
            release_authorization_id,
            release_control_decision_id,
            executor_source_revision,
            executor_workload_artifact_digest,
            executor_job_contract_version,
        ) = row
        if run_id != claim.run_id:
            raise RepositoryInvariantError("claim run identity changed")
        if work_key != claim.work_key:
            raise RepositoryInvariantError("claim work identity changed")
        if (
            promotion_capability_id != claim.promotion_capability_id
            or extractor_contract_version != claim.extractor_contract_version
            or release_subject_digest != claim.release_subject_digest
        ):
            raise RepositoryInvariantError("claim release subject identity changed")
        if attempt_state != "RUNNING" or int(attempt_number) != claim.claim_generation:
            raise RepositoryInvariantError("attempt ownership does not match claim")
        if (
            release_authorization_id != claim.release_authorization_id
            or release_control_decision_id != claim.release_control_decision_id
            or executor_source_revision != claim.executor_source_revision
            or executor_workload_artifact_digest
            != claim.executor_workload_artifact_digest
            or executor_job_contract_version != claim.executor_job_contract_version
        ):
            raise RepositoryInvariantError(
                "attempt authorization or executor provenance changed unexpectedly"
            )
        return run_id, input_artifact_id

    def assert_current_promotion_authorization(
        self,
        connection: Any,
        claim: Claim,
    ) -> None:
        if claim.execution_scope != "ECONOMIC_PROMOTE":
            raise RepositoryInvariantError(
                "final promotion authorization requires ECONOMIC_PROMOTE"
            )
        if (
            claim.release_authorization_id is None
            or claim.release_subject_digest is None
            or claim.promotion_capability_id is None
            or claim.extractor_contract_version is None
        ):
            raise RepositoryInvariantError(
                "promotion claim is missing exact authorization identity"
            )
        row = connection.execute(
            """
            SELECT a.release_subject_digest,
                   m.executor_source_revision,
                   m.executor_workload_artifact_digest,
                   m.executor_job_contract_version,
                   c.control_decision_id,
                   c.state
              FROM promotion_release_authorizations a
              JOIN promotion_release_authorization_materials m
                ON m.authorization_material_id=a.authorization_material_id
              JOIN LATERAL (
                    SELECT control_decision_id, state
                      FROM promotion_release_control_decisions
                     WHERE authorization_id=a.authorization_id
                     ORDER BY control_version DESC
                     LIMIT 1
              ) c ON TRUE
             WHERE a.authorization_id=%s
            """,
            (claim.release_authorization_id,),
        ).fetchone()
        expected = (
            claim.release_subject_digest,
            claim.executor_source_revision,
            claim.executor_workload_artifact_digest,
            claim.executor_job_contract_version,
        )
        if row is None or tuple(row[:4]) != expected:
            raise RepositoryInvariantError(
                "final promotion authorization identity does not match claim"
            )
        if row[5] != "APPROVED":
            raise RepositoryInvariantError(
                "promotion authorization is not currently approved"
            )
        if row[4] != claim.release_control_decision_id:
            raise RepositoryInvariantError(
                "promotion authorization control changed after claim"
            )

    def renew_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        lease_seconds: int = 300,
    ) -> datetime:
        if lease_seconds < 1 or lease_seconds > 3600:
            raise ValueError("lease_seconds must be in [1, 3600]")

        authorization_lost = False
        row = None
        with connection.transaction():
            if claim.execution_scope == "ECONOMIC_PROMOTE":
                self.assert_current_claim(connection, claim)
                if claim.release_subject_digest is None:
                    raise RepositoryInvariantError(
                        "promotion claim has no release subject"
                    )
                active_authorization_id = (
                    self.resolve_promotion_release_authorization(
                        connection,
                        release_subject_digest=claim.release_subject_digest,
                        executor=ExecutorProvenanceV1(
                            source_revision=claim.executor_source_revision,
                            workload_artifact_digest=(
                                claim.executor_workload_artifact_digest
                            ),
                            job_contract_version=claim.executor_job_contract_version,
                        ),
                    )
                )
                if active_authorization_id != claim.release_authorization_id:
                    connection.execute(
                        """
                        SELECT pause_cpi_ingestion_work(
                            %s, %s, %s, %s,
                            'RELEASE_AUTHORIZATION_REVOKED',
                            'RELEASE_AUTHORIZATION_REVOKED',
                            'system:cpi-heartbeat', NULL
                        )
                        """,
                        (
                            claim.work_item_id,
                            claim.attempt_id,
                            claim.claim_generation,
                            claim.claim_token,
                        ),
                    )
                    authorization_lost = True

            if not authorization_lost:
                row = connection.execute(
                    """
                    UPDATE ingestion_work_items w
                       SET lease_until = GREATEST(
                               w.lease_until,
                               CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')
                           )
                      FROM ingestion_attempts a
                     WHERE w.work_item_id=%s
                       AND w.execution_scope=%s
                       AND w.data_domain='ECONOMIC'
                       AND w.state='CLAIMED'
                       AND w.claim_generation=%s
                       AND w.claim_token=%s
                       AND w.lease_until > CURRENT_TIMESTAMP
                       AND a.attempt_id=%s
                       AND a.work_item_id=w.work_item_id
                       AND a.execution_scope=w.execution_scope
                       AND a.data_domain=w.data_domain
                       AND a.state='RUNNING'
                       AND a.attempt_number=w.claim_generation
                     RETURNING w.lease_until, w.claim_token, w.claim_generation
                    """,
                    (
                        lease_seconds,
                        claim.work_item_id,
                        claim.execution_scope,
                        claim.claim_generation,
                        claim.claim_token,
                        claim.attempt_id,
                    ),
                ).fetchone()
        if authorization_lost:
            raise RepositoryInvariantError(
                "promotion claim authorization is no longer active"
            )
        if row is None:
            raise StaleClaimError("claim is stale or expired and cannot be renewed")
        lease_until, claim_token, claim_generation = row
        if claim_token != claim.claim_token:
            raise RepositoryInvariantError("lease renewal changed claim token")
        if int(claim_generation) != claim.claim_generation:
            raise RepositoryInvariantError("lease renewal changed claim generation")
        return lease_until

    def terminalize_claim_in_transaction(
        self,
        connection: Any,
        claim: Claim,
        *,
        outcome: str,
        reason_code: str | None = None,
        irreversible_failure: bool = False,
    ) -> None:
        _validate_reason(outcome, reason_code)
        if outcome == "FAILED" and not irreversible_failure:
            raise ValueError(
                "terminal FAILED is irreversible; use abandon_claim explicitly"
            )
        self.assert_current_claim(connection, claim)

        attempt = connection.execute(
            """
            UPDATE ingestion_attempts a
               SET state='TERMINAL',
                   outcome=%s,
                   reason_code=%s,
                   finished_at=CURRENT_TIMESTAMP
              FROM ingestion_work_items w
             WHERE a.attempt_id=%s
               AND a.work_item_id=w.work_item_id
               AND w.work_item_id=%s
               AND w.execution_scope=%s
               AND w.state='CLAIMED'
               AND w.claim_generation=%s
               AND w.claim_token=%s
               AND w.lease_until > CURRENT_TIMESTAMP
               AND a.state='RUNNING'
               AND a.attempt_number=w.claim_generation
             RETURNING a.attempt_id
            """,
            (
                outcome,
                reason_code,
                claim.attempt_id,
                claim.work_item_id,
                claim.execution_scope,
                claim.claim_generation,
                claim.claim_token,
            ),
        ).fetchone()
        if attempt is None:
            raise StaleClaimError("claim lost before attempt terminalization")

        work = connection.execute(
            """
            UPDATE ingestion_work_items
               SET state='TERMINAL',
                   outcome=%s,
                   reason_code=%s,
                   claim_token=NULL,
                   lease_until=NULL,
                   next_claim_at=NULL
             WHERE work_item_id=%s
               AND execution_scope=%s
               AND state='CLAIMED'
               AND claim_generation=%s
               AND claim_token=%s
             RETURNING work_item_id
            """,
            (
                outcome,
                reason_code,
                claim.work_item_id,
                claim.execution_scope,
                claim.claim_generation,
                claim.claim_token,
            ),
        ).fetchone()
        if work is None:
            raise StaleClaimError("claim lost before work terminalization")

    def pause_pending_work(
        self,
        connection: Any,
        *,
        work_item_id: UUID,
        reason_code: str,
        actor_subject: str,
        case_ref: str | None = None,
    ) -> None:
        if _REASON_RE.fullmatch(reason_code) is None:
            raise ValueError("pause reason_code must be canonical uppercase token")
        if not actor_subject or actor_subject != actor_subject.strip():
            raise ValueError("pause actor_subject must be canonical")
        with connection.transaction():
            connection.execute(
                "SELECT pause_pending_cpi_ingestion_work(%s, %s, %s, %s)",
                (work_item_id, reason_code, actor_subject, case_ref),
            )

    def pause_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        attempt_reason_code: str,
        work_reason_code: str,
        actor_subject: str,
        case_ref: str | None = None,
    ) -> None:
        if _REASON_RE.fullmatch(attempt_reason_code) is None:
            raise ValueError(
                "pause attempt_reason_code must be canonical uppercase token"
            )
        if _REASON_RE.fullmatch(work_reason_code) is None:
            raise ValueError("pause work_reason_code must be canonical uppercase token")
        if not actor_subject or actor_subject != actor_subject.strip():
            raise ValueError("pause actor_subject must be canonical")
        with connection.transaction():
            self.assert_current_claim(connection, claim)
            connection.execute(
                """
                SELECT pause_cpi_ingestion_work(
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    claim.work_item_id,
                    claim.attempt_id,
                    claim.claim_generation,
                    claim.claim_token,
                    attempt_reason_code,
                    work_reason_code,
                    actor_subject,
                    case_ref,
                ),
            )

    def resume_paused_work(
        self,
        connection: Any,
        *,
        work_item_id: UUID,
        actor_subject: str,
        case_ref: str,
    ) -> None:
        if not actor_subject or actor_subject != actor_subject.strip():
            raise ValueError("resume actor_subject must be canonical")
        if not case_ref or case_ref != case_ref.strip():
            raise ValueError("resume case_ref must be canonical")
        with connection.transaction():
            connection.execute(
                "SELECT resume_cpi_ingestion_work(%s, %s, %s)",
                (work_item_id, actor_subject, case_ref),
            )

    def retry_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        reason_code: str,
        retry_after_seconds: int,
    ) -> None:
        if _REASON_RE.fullmatch(reason_code) is None:
            raise ValueError("retry reason_code must be canonical uppercase token")
        if (
            not isinstance(retry_after_seconds, int)
            or isinstance(retry_after_seconds, bool)
            or retry_after_seconds < 1
            or retry_after_seconds > MAX_RETRY_DELAY_SECONDS
        ):
            raise ValueError(
                f"retry_after_seconds must be in [1, {MAX_RETRY_DELAY_SECONDS}]"
            )
        with connection.transaction():
            self.assert_current_claim(connection, claim)
            attempt = connection.execute(
                """
                UPDATE ingestion_attempts a
                   SET state='TERMINAL',
                       outcome='FAILED',
                       reason_code=%s,
                       finished_at=CURRENT_TIMESTAMP
                  FROM ingestion_work_items w
                 WHERE a.attempt_id=%s
                   AND a.work_item_id=w.work_item_id
                   AND w.work_item_id=%s
                   AND w.execution_scope=%s
                   AND w.state='CLAIMED'
                   AND w.claim_generation=%s
                   AND w.claim_token=%s
                   AND w.lease_until > CURRENT_TIMESTAMP
                   AND a.state='RUNNING'
                   AND a.attempt_number=w.claim_generation
                 RETURNING a.attempt_id
                """,
                (
                    reason_code,
                    claim.attempt_id,
                    claim.work_item_id,
                    claim.execution_scope,
                    claim.claim_generation,
                    claim.claim_token,
                ),
            ).fetchone()
            if attempt is None:
                raise StaleClaimError("claim lost before retry attempt terminalization")

            work = connection.execute(
                """
                UPDATE ingestion_work_items
                   SET state='PENDING',
                       outcome=NULL,
                       reason_code=NULL,
                       claim_token=NULL,
                       lease_until=NULL,
                       next_claim_at=CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')
                 WHERE work_item_id=%s
                   AND execution_scope=%s
                   AND state='CLAIMED'
                   AND claim_generation=%s
                   AND claim_token=%s
                 RETURNING work_item_id
                """,
                (
                    retry_after_seconds,
                    claim.work_item_id,
                    claim.execution_scope,
                    claim.claim_generation,
                    claim.claim_token,
                ),
            ).fetchone()
            if work is None:
                raise StaleClaimError("claim lost before retry work reset")

    def finalize_run_if_complete(
        self,
        connection: Any,
        run_id: UUID,
    ) -> bool:
        with connection.transaction():
            row = connection.execute(
                "SELECT finalize_ingestion_run_if_complete(%s)",
                (run_id,),
            ).fetchone()
            if row is None:
                raise RepositoryInvariantError("run finalizer returned no result")
            return bool(row[0])

    def terminalize_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        outcome: str,
        reason_code: str | None = None,
    ) -> None:
        with connection.transaction():
            self.terminalize_claim_in_transaction(
                connection,
                claim,
                outcome=outcome,
                reason_code=reason_code,
            )

    def abandon_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        reason_code: str,
    ) -> None:
        if _REASON_RE.fullmatch(reason_code) is None:
            raise ValueError("abandon reason_code must be canonical uppercase token")
        with connection.transaction():
            self.terminalize_claim_in_transaction(
                connection,
                claim,
                outcome="FAILED",
                reason_code=reason_code,
                irreversible_failure=True,
            )

    def record_source_artifact(
        self,
        connection: Any,
        claim: Claim,
        *,
        source_code: str,
        artifact_contract_kind: str,
        source_contract_version: str,
        locator_key: str,
        content_sha256: str,
        content_type: str,
        captured_at: datetime,
        retrieval_url: str | None = None,
        storage_uri: str | None = None,
        storage_generation: str | None = None,
        artifact_id: UUID | None = None,
    ) -> UUID:
        if claim.execution_scope != "ECONOMIC_COLLECT":
            raise ValueError("source artifacts require ECONOMIC_COLLECT ownership")
        if re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
            raise ValueError("content_sha256 must be lowercase SHA-256")
        if captured_at.tzinfo is None or captured_at.utcoffset() is None:
            raise ValueError("captured_at must be timezone-aware")
        if (storage_uri is None) != (storage_generation is None):
            raise ValueError("storage_uri and storage_generation must appear together")
        content_state = "RETAINED" if storage_uri is not None else "NOT_RETAINED"

        with connection.transaction():
            self.assert_current_claim(connection, claim)

            existing = connection.execute(
                """
                SELECT artifact_id, source_code, artifact_contract_kind,
                       source_contract_version, retrieval_url, content_sha256,
                       content_type, captured_at, content_state,
                       storage_uri, storage_generation
                  FROM source_artifacts
                 WHERE created_by_attempt_id=%s
                   AND locator_key=%s
                 FOR UPDATE
                """,
                (claim.attempt_id, locator_key),
            ).fetchall()
            if existing:
                if len(existing) != 1:
                    raise RepositoryInvariantError(
                        "one collector attempt/locator resolved to multiple artifacts"
                    )
                row = existing[0]
                expected = (
                    source_code,
                    artifact_contract_kind,
                    source_contract_version,
                    retrieval_url,
                    content_sha256,
                    content_type,
                    captured_at,
                    content_state,
                    storage_uri,
                    storage_generation,
                )
                if tuple(row[1:]) != expected:
                    raise RepositoryInvariantError(
                        "same collector attempt/locator retry changed immutable artifact metadata"
                    )
                if artifact_id is not None and row[0] != artifact_id:
                    raise RepositoryInvariantError(
                        "same collector attempt/locator retry changed artifact identity"
                    )
                return row[0]

            artifact_id = artifact_id or uuid4()
            connection.execute(
                """
                INSERT INTO source_artifacts (
                    artifact_id, data_domain, source_code, artifact_contract_kind,
                    source_contract_version, locator_key, retrieval_url,
                    content_sha256, content_type, captured_at,
                    created_by_attempt_id, created_by_execution_scope,
                    content_state, storage_uri, storage_generation
                ) VALUES (
                    %s, 'ECONOMIC', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, 'ECONOMIC_COLLECT',
                    %s, %s, %s
                )
                """,
                (
                    artifact_id,
                    source_code,
                    artifact_contract_kind,
                    source_contract_version,
                    locator_key,
                    retrieval_url,
                    content_sha256,
                    content_type,
                    captured_at,
                    claim.attempt_id,
                    content_state,
                    storage_uri,
                    storage_generation,
                ),
            )
            return artifact_id

    def record_artifact_promotion_targets(
        self,
        connection: Any,
        claim: Claim,
        *,
        artifact_id: UUID,
        reference_months: tuple[date, ...],
    ) -> tuple[date, ...]:
        if claim.execution_scope != "ECONOMIC_COLLECT":
            raise ValueError("artifact targets require ECONOMIC_COLLECT ownership")
        if not reference_months:
            raise ValueError("artifact promotion targets must not be empty")
        if any(
            not isinstance(month, date)
            or isinstance(month, datetime)
            or month.day != 1
            for month in reference_months
        ):
            raise ValueError("reference months must be month-start dates")
        canonical = tuple(sorted(set(reference_months)))
        if len(canonical) != len(reference_months):
            raise ValueError("artifact promotion targets must be unique")

        with connection.transaction():
            self.assert_current_claim(connection, claim)
            artifact = connection.execute(
                """
                SELECT created_by_attempt_id
                  FROM source_artifacts
                 WHERE artifact_id=%s
                   AND data_domain='ECONOMIC'
                 FOR SHARE
                """,
                (artifact_id,),
            ).fetchone()
            if artifact is None:
                raise KeyError(f"CPI source artifact not found: {artifact_id}")
            if artifact[0] != claim.attempt_id:
                raise RepositoryInvariantError(
                    "artifact target must be bound by its collector attempt"
                )
            for reference_month in canonical:
                connection.execute(
                    """
                    INSERT INTO cpi_artifact_promotion_targets (
                        source_artifact_id, data_domain, reference_month,
                        bound_by_attempt_id, bound_by_execution_scope
                    ) VALUES (%s, 'ECONOMIC', %s, %s, 'ECONOMIC_COLLECT')
                    ON CONFLICT (source_artifact_id, reference_month) DO NOTHING
                    """,
                    (artifact_id, reference_month, claim.attempt_id),
                )
            persisted = self.artifact_promotion_targets(
                connection,
                artifact_id=artifact_id,
            )
            if persisted != canonical:
                raise RepositoryInvariantError(
                    "artifact promotion target identity did not converge"
                )
            return persisted

    @staticmethod
    def artifact_promotion_targets(
        connection: Any,
        *,
        artifact_id: UUID,
    ) -> tuple[date, ...]:
        rows = connection.execute(
            """
            SELECT reference_month
              FROM cpi_artifact_promotion_targets
             WHERE source_artifact_id=%s
             ORDER BY reference_month
            """,
            (artifact_id,),
        ).fetchall()
        return tuple(row[0] for row in rows)
