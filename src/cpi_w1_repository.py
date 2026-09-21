"""PostgreSQL repository primitives for CPI W1 fenced work ownership."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4


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


CLAIM_SELECT_SQL = """
SELECT
    w.work_item_id,
    w.run_id,
    w.input_artifact_id,
    w.claim_generation,
    w.state,
    w.work_key
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
    a.state,
    a.attempt_number
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
    def claim_work_item(
        self,
        connection: Any,
        *,
        execution_scope: str,
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

            work_item_id, run_id, input_artifact_id, generation, previous_state, work_key = row
            previous_generation = int(generation)
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
                    attempt_number
                ) VALUES (%s, %s, %s, 'ECONOMIC', %s)
                """,
                (attempt_id, work_item_id, execution_scope, generation),
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
        run_id, input_artifact_id, work_key, attempt_state, attempt_number = row
        if run_id != claim.run_id:
            raise RepositoryInvariantError("claim run identity changed")
        if work_key != claim.work_key:
            raise RepositoryInvariantError("claim work identity changed")
        if attempt_state != "RUNNING" or int(attempt_number) != claim.claim_generation:
            raise RepositoryInvariantError("attempt ownership does not match claim")
        return run_id, input_artifact_id

    def renew_claim(
        self,
        connection: Any,
        claim: Claim,
        *,
        lease_seconds: int = 300,
    ) -> datetime:
        if lease_seconds < 1 or lease_seconds > 3600:
            raise ValueError("lease_seconds must be in [1, 3600]")

        with connection.transaction():
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
    ) -> None:
        _validate_reason(outcome, reason_code)
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
                return row[0]

            artifact_id = uuid4()
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
