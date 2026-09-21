"""Application boundary for CPI W1 interpretation and serving governance."""

from __future__ import annotations

import base64
from contextlib import contextmanager
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
    with _owned_governance_transaction(connection):
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
            elif verification_ref is None or not verification_ref.strip():
                raise ValueError("domain re-enable requires verification_ref")

        with _owned_governance_transaction(connection):
            if (
                state is ServingControlState.ENABLED
                and scope_kind == "EVENT_OCCURRENCE"
            ):
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
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                ),
            ).fetchone()
            if row is None:
                raise GovernanceSafetyError("serving-control activation returned no decision")
            return row[0]
