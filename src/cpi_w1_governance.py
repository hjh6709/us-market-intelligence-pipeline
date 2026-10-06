"""Application boundary for CPI W1 interpretation and serving governance."""

from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from psycopg.pq import TransactionStatus

from src.cpi_w1_contracts import ReleaseProjectionState, ServingControlState
from src.cpi_w1_repository import CpiW1Repository
from src.cpi_w1_selector import (
    CpiW1Selector,
    ObservationResolutionState,
)


_POLICY_VERSION = "cpi-governance-v1"
_RECOVERY_POLICY_VERSION = "cpi-domain-recovery-v1"
_REASON_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class GovernanceIdentityError(ValueError):
    """Authenticated workforce identity is missing or unsuitable."""


class GovernanceSafetyError(RuntimeError):
    """A governance action is unsafe for the current knowledge state."""


@dataclass(frozen=True)
class WorkforcePrincipal:
    """Stable IdP identity supplied by the trusted authentication layer."""

    issuer: str
    subject_id: str

    def __post_init__(self) -> None:
        for label, value in (("issuer", self.issuer), ("subject_id", self.subject_id)):
            if not value or value != value.strip():
                raise GovernanceIdentityError(f"{label} must be canonical and non-empty")
            if any(char.isspace() for char in value):
                raise GovernanceIdentityError(f"{label} must not contain whitespace")
            if "@" in value:
                raise GovernanceIdentityError(
                    f"{label} must be an immutable IdP identifier, not an email alias"
                )

    @staticmethod
    def _encode(value: str) -> str:
        return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")

    @property
    def database_subject(self) -> str:
        return f"idp:{self._encode(self.issuer)}:{self._encode(self.subject_id)}"


@contextmanager
def _owned_governance_transaction(connection: Any):
    info = getattr(connection, "info", None)
    transaction_status = getattr(info, "transaction_status", None)
    if transaction_status is not None and transaction_status != TransactionStatus.IDLE:
        raise RuntimeError(
            "CPI governance requires an idle connection so it owns the mutation transaction"
        )
    with connection.transaction():
        yield


def _reason_code(value: str) -> str:
    if _REASON_RE.fullmatch(value) is None:
        raise ValueError("reason_code must be a canonical uppercase token")
    return value


def _case_ref(value: str | None, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ValueError("case_ref is required")
        return None
    if not value or value != value.strip():
        raise ValueError("case_ref must be canonical and non-empty")
    return value


def _current_interpretation_version(connection: Any, subject_id: UUID) -> int:
    row = connection.execute(
        """
        SELECT decision_version
          FROM interpretation_decisions
         WHERE subject_id=%s
         ORDER BY decision_version DESC
         LIMIT 1
        """,
        (subject_id,),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _current_control_version(
    connection: Any,
    *,
    scope_kind: str,
    event_occurrence_id: UUID | None,
) -> int:
    if scope_kind == "CPI_DOMAIN":
        row = connection.execute(
            """
            SELECT control_version
              FROM economic_serving_control_decisions
             WHERE scope_kind='CPI_DOMAIN'
             ORDER BY control_version DESC
             LIMIT 1
            """
        ).fetchone()
    else:
        row = connection.execute(
            """
            SELECT control_version
              FROM economic_serving_control_decisions
             WHERE scope_kind='EVENT_OCCURRENCE'
               AND event_occurrence_id=%s
             ORDER BY control_version DESC
             LIMIT 1
            """,
            (event_occurrence_id,),
        ).fetchone()
    return int(row[0]) if row is not None else 0


def _semantic_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CpiW1Governance:
    def __init__(
        self,
        selector: CpiW1Selector | None = None,
        repository: CpiW1Repository | None = None,
    ) -> None:
        self.selector = selector or CpiW1Selector()
        self.repository = repository or CpiW1Repository()

    def create_interpretation_request(
        self,
        connection: Any,
        *,
        principal: WorkforcePrincipal,
        subject_id: UUID,
        requested_state: str,
        reason_code: str,
        case_ref: str | None = None,
    ) -> UUID:
        if requested_state not in {"VALID", "INVALID"}:
            raise ValueError("requested_state must be VALID or INVALID")
        _reason_code(reason_code)
        case_ref = _case_ref(case_ref)

        with _owned_governance_transaction(connection):
            expected_version = _current_interpretation_version(connection, subject_id)
            request_id = uuid4()
            connection.execute(
                """
                INSERT INTO interpretation_requests (
                    request_id, subject_id, requested_state,
                    expected_decision_version, reason_code, case_ref,
                    governance_policy_version, proposer_subject
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    request_id,
                    subject_id,
                    requested_state,
                    expected_version,
                    reason_code,
                    case_ref,
                    _POLICY_VERSION,
                    principal.database_subject,
                ),
            )
            return request_id

    def record_interpretation_approval(
        self,
        connection: Any,
        *,
        principal: WorkforcePrincipal,
        request_id: UUID,
        decision: str,
    ) -> UUID:
        if decision not in {"APPROVE", "REJECT"}:
            raise ValueError("decision must be APPROVE or REJECT")

        with _owned_governance_transaction(connection):
            row = connection.execute(
                """
                SELECT proposer_subject
                  FROM interpretation_requests
                 WHERE request_id=%s
                """,
                (request_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"interpretation request not found: {request_id}")
            proposer_subject = row[0]
            if proposer_subject == principal.database_subject:
                raise GovernanceIdentityError("proposer cannot approve their own request")

            approval_id = uuid4()
            connection.execute(
                """
                INSERT INTO interpretation_approvals (
                    approval_id, request_id, proposer_subject,
                    approver_subject, approval_decision
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    approval_id,
                    request_id,
                    proposer_subject,
                    principal.database_subject,
                    decision,
                ),
            )
            return approval_id

    def activate_interpretation_request(
        self,
        connection: Any,
        *,
        principal: WorkforcePrincipal,
        request_id: UUID,
    ) -> UUID:
        with _owned_governance_transaction(connection):
            row = connection.execute(
                "SELECT apply_interpretation_decision(%s, %s)",
                (request_id, principal.database_subject),
            ).fetchone()
            if row is None:
                raise GovernanceSafetyError("interpretation activation returned no decision")
            return row[0]

    def create_domain_recovery_snapshot(
        self,
        connection: Any,
        *,
        principal: WorkforcePrincipal,
        case_ref: str,
        verification_ref: str,
    ) -> UUID:
        case_ref = _case_ref(case_ref, required=True)
        if not verification_ref or verification_ref != verification_ref.strip():
            raise ValueError("verification_ref must be canonical and non-empty")

        with _owned_governance_transaction(connection):
            self.repository.lock_cpi_domain_exclusive(connection)
            event_ids = tuple(
                row[0]
                for row in connection.execute(
                    """
                    SELECT event_occurrence_id
                      FROM core_event_occurrences
                     WHERE event_type='CPI'
                     ORDER BY reference_month, event_occurrence_id
                    """
                ).fetchall()
            )
            event_controls = {
                row[0]: row[1]
                for row in connection.execute(
                    """
                    SELECT DISTINCT ON (event_occurrence_id)
                           event_occurrence_id, state
                      FROM economic_serving_control_decisions
                     WHERE scope_kind='EVENT_OCCURRENCE'
                     ORDER BY event_occurrence_id, control_version DESC
                    """
                ).fetchall()
            }

            canonical_state = []
            withheld_event_count = 0
            conflicting_event_count = 0
            unresolved_event_count = 0
            unsafe_uncontained_event_count = 0
            unresolved_states = {
                ObservationResolutionState.UNRESOLVED,
                ObservationResolutionState.CONFLICT,
            }
            for event_id in event_ids:
                knowledge = self.selector._select_current_event_in_caller_transaction(
                    connection,
                    event_id,
                )
                is_conflicting = (
                    knowledge.release_state is ReleaseProjectionState.CONFLICT
                    or any(
                        item.state is ObservationResolutionState.CONFLICT
                        for item in knowledge.observations
                    )
                )
                is_unresolved = (
                    knowledge.release_state is ReleaseProjectionState.UNRESOLVED
                    or any(
                        item.state is ObservationResolutionState.UNRESOLVED
                        for item in knowledge.observations
                    )
                )
                is_withheld = event_controls.get(event_id) == "WITHHELD"
                withheld_event_count += int(is_withheld)
                conflicting_event_count += int(is_conflicting)
                unresolved_event_count += int(is_unresolved)
                unsafe_uncontained_event_count += int(
                    (is_conflicting or is_unresolved) and not is_withheld
                )
                canonical_state.append(
                    {
                        "event_occurrence_id": str(event_id),
                        "knowledge_fingerprint": knowledge.knowledge_fingerprint,
                        "release_state": knowledge.release_state.value,
                        "observation_states": [
                            {
                                "observation_code": item.observation_code,
                                "state": item.state.value,
                            }
                            for item in knowledge.observations
                            if item.state in unresolved_states
                        ],
                    }
                )

            interpretation_state = [
                {
                    "subject_id": str(row[0]),
                    "decision_version": int(row[1]),
                    "decision_state": row[2],
                }
                for row in connection.execute(
                    """
                    SELECT DISTINCT ON (subject_id)
                           subject_id, decision_version, decision_state
                      FROM interpretation_decisions
                     ORDER BY subject_id, decision_version DESC
                    """
                ).fetchall()
            ]
            serving_state = [
                {
                    "scope_kind": row[0],
                    "event_occurrence_id": str(row[1]) if row[1] else None,
                    "control_version": int(row[2]),
                    "state": row[3],
                }
                for row in connection.execute(
                    """
                    SELECT DISTINCT ON (scope_kind, event_occurrence_id)
                           scope_kind, event_occurrence_id,
                           control_version, state
                      FROM economic_serving_control_decisions
                     ORDER BY scope_kind, event_occurrence_id,
                              control_version DESC
                    """
                ).fetchall()
            ]
            committed_change_watermark = int(
                connection.execute(
                    "SELECT current_recovery_watermark()"
                ).fetchone()[0]
            )
            canonical_knowledge_digest = _semantic_digest(canonical_state)
            interpretation_state_digest = _semantic_digest(interpretation_state)
            serving_control_digest = _semantic_digest(serving_state)
            snapshot_material = {
                "recovery_policy_version": _RECOVERY_POLICY_VERSION,
                "committed_change_watermark": committed_change_watermark,
                "canonical_knowledge_digest": canonical_knowledge_digest,
                "interpretation_state_digest": interpretation_state_digest,
                "serving_control_digest": serving_control_digest,
                "event_count": len(event_ids),
                "withheld_event_count": withheld_event_count,
                "conflicting_event_count": conflicting_event_count,
                "unresolved_event_count": unresolved_event_count,
                "unsafe_uncontained_event_count": unsafe_uncontained_event_count,
                "generated_by_subject": principal.database_subject,
                "case_ref": case_ref,
                "verification_ref": verification_ref,
            }
            recovery_snapshot_digest = _semantic_digest(snapshot_material)
            recovery_snapshot_id = uuid4()
            connection.execute(
                """
                INSERT INTO cpi_domain_recovery_snapshots (
                    recovery_snapshot_id, recovery_policy_version,
                    committed_change_watermark, canonical_knowledge_digest,
                    interpretation_state_digest, serving_control_digest,
                    event_count, withheld_event_count,
                    conflicting_event_count, unresolved_event_count,
                    unsafe_uncontained_event_count, generated_by_subject,
                    case_ref, verification_ref, recovery_snapshot_digest
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    recovery_snapshot_id,
                    _RECOVERY_POLICY_VERSION,
                    committed_change_watermark,
                    canonical_knowledge_digest,
                    interpretation_state_digest,
                    serving_control_digest,
                    len(event_ids),
                    withheld_event_count,
                    conflicting_event_count,
                    unresolved_event_count,
                    unsafe_uncontained_event_count,
                    principal.database_subject,
                    case_ref,
                    verification_ref,
                    recovery_snapshot_digest,
                ),
            )
            return recovery_snapshot_id

    def apply_serving_control(
        self,
        connection: Any,
        *,
        principal: WorkforcePrincipal,
        scope_kind: str,
        state: ServingControlState | str,
        reason_code: str,
        event_occurrence_id: UUID | None = None,
        case_ref: str | None = None,
        operator_verified_knowledge_fingerprint: str | None = None,
        verification_ref: str | None = None,
        recovery_snapshot_id: UUID | None = None,
    ) -> UUID:
        if not isinstance(state, ServingControlState):
            state = ServingControlState(state)
        if scope_kind not in {"CPI_DOMAIN", "EVENT_OCCURRENCE"}:
            raise ValueError("invalid serving-control scope_kind")
        if (scope_kind == "CPI_DOMAIN") != (event_occurrence_id is None):
            raise ValueError("serving-control scope/event mismatch")
        _reason_code(reason_code)
        case_ref = _case_ref(case_ref)

        verified_fingerprint = None
        if state is ServingControlState.ENABLED:
            case_ref = _case_ref(case_ref, required=True)
            if scope_kind == "EVENT_OCCURRENCE":
                if operator_verified_knowledge_fingerprint is None or (
                    _SHA256_RE.fullmatch(operator_verified_knowledge_fingerprint) is None
                ):
                    raise ValueError(
                        "event re-enable requires operator-verified lowercase SHA-256"
                    )
            else:
                if verification_ref is None or not verification_ref.strip():
                    raise ValueError("domain re-enable requires verification_ref")
                if recovery_snapshot_id is None:
                    raise ValueError("domain re-enable requires recovery_snapshot_id")

        with _owned_governance_transaction(connection):
            if (
                state is ServingControlState.ENABLED
                and scope_kind == "EVENT_OCCURRENCE"
            ):
                self.repository.lock_cpi_domain_shared(connection)
                self.repository.lock_cpi_event(connection, event_occurrence_id)
                knowledge = self.selector._select_current_event_in_caller_transaction(
                    connection,
                    event_occurrence_id,
                )
                unresolved = {
                    ObservationResolutionState.UNRESOLVED,
                    ObservationResolutionState.CONFLICT,
                }
                if knowledge.release_state in {
                    ReleaseProjectionState.UNRESOLVED,
                    ReleaseProjectionState.CONFLICT,
                } or any(item.state in unresolved for item in knowledge.observations):
                    raise GovernanceSafetyError(
                        "event cannot be re-enabled while CPI knowledge is unresolved/conflicting"
                    )
                if (
                    knowledge.knowledge_fingerprint
                    != operator_verified_knowledge_fingerprint
                ):
                    raise GovernanceSafetyError(
                        "operator-verified knowledge fingerprint is stale"
                    )
                verified_fingerprint = knowledge.knowledge_fingerprint

            expected_version = _current_control_version(
                connection,
                scope_kind=scope_kind,
                event_occurrence_id=event_occurrence_id,
            )
            row = connection.execute(
                """
                SELECT apply_economic_serving_control(
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    scope_kind,
                    event_occurrence_id,
                    expected_version,
                    state.value,
                    reason_code,
                    principal.database_subject,
                    case_ref,
                    verified_fingerprint,
                    verification_ref,
                    recovery_snapshot_id,
                ),
            ).fetchone()
            if row is None:
                raise GovernanceSafetyError("serving-control activation returned no decision")
            return row[0]
