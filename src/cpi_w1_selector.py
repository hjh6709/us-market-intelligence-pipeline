"""CPI W1 source reconstruction, PIT selection, and serving overlay."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.cpi_w1_contracts import (
    KnowledgeMode,
    ObservationMaterial,
    ObservationState,
    ReleaseProjectionState,
    ScheduleMaterial,
    ScheduleStatus,
    ServingControlState,
    SourceAuthorityRole,
    TimePrecision,
    canonical_material_json,
)
from src.cpi_w1_source import BlsCpiSourceContract


SELECTOR_CONTRACT_VERSION = "cpi-w1-selector-v1"
_CORE4 = (
    "CPI_HEADLINE_MOM",
    "CPI_HEADLINE_YOY",
    "CPI_CORE_MOM",
    "CPI_CORE_YOY",
)
_ALLOWED_OBSERVATION_ARTIFACT_RELATIONS = {
    "RELEASE_REPRESENTATION",
    "CORROBORATING_REPRESENTATION",
}


class ObservationResolutionState(StrEnum):
    VALUE = "VALUE"
    EXPLICIT_UNAVAILABLE = "EXPLICIT_UNAVAILABLE"
    UNRESOLVED = "UNRESOLVED"
    CONFLICT = "CONFLICT"


class SelectorDataIntegrityError(RuntimeError):
    """Durable evidence contradicts the selector contract."""


@dataclass(frozen=True)
class ObservationResolution:
    observation_code: str
    state: ObservationResolutionState
    normalized_value: Decimal | None
    material_fingerprints: tuple[str, ...]
    eligible_evidence_count: int


@dataclass(frozen=True)
class CpiEventKnowledge:
    event_occurrence_id: UUID
    reference_month: date
    mode: KnowledgeMode
    release_state: ReleaseProjectionState
    observations: tuple[ObservationResolution, ...]
    knowledge_fingerprint: str

    def observation(self, code: str) -> ObservationResolution:
        for item in self.observations:
            if item.observation_code == code:
                return item
        raise KeyError(code)


@dataclass(frozen=True)
class GovernedObservation:
    observation_code: str
    knowledge_state: ObservationResolutionState
    normalized_value: Decimal | None
    withheld: bool


@dataclass(frozen=True)
class GovernedCpiEvent:
    knowledge: CpiEventKnowledge
    effective_control_state: ServingControlState
    observations: tuple[GovernedObservation, ...]


@dataclass(frozen=True)
class _ScheduleEvidence:
    assertion_id: UUID
    material: ScheduleMaterial
    material_fingerprint: str
    source_contract_version: str
    artifact_contract_kind: str
    source_effective_date: date | None
    source_effective_at: datetime | None
    source_effective_precision: TimePrecision | None
    captured_at: datetime
    accepted_at: datetime
    capture_chronology_allowed: bool
    authority_role: SourceAuthorityRole


@dataclass(frozen=True)
class _ScheduleSelection:
    kind: str
    material: ScheduleMaterial | None
    material_fingerprints: tuple[str, ...]
    contains_canceled_material: bool


def _require_aware(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")


def _default_source_contract() -> BlsCpiSourceContract:
    path = Path(__file__).resolve().parents[1] / "config" / "cpi_w1_source_contract.json"
    return BlsCpiSourceContract.from_json(path)


def _resolution_from_materials(
    observation_code: str,
    rows: Iterable[tuple[str, ObservationState, Decimal | None]],
) -> ObservationResolution:
    unique: dict[str, tuple[ObservationState, Decimal | None]] = {}
    evidence_count = 0
    for fingerprint, state, normalized_value in rows:
        evidence_count += 1
        prior = unique.get(fingerprint)
        current = (state, normalized_value)
        if prior is not None and prior != current:
            raise SelectorDataIntegrityError(
                "one observation material fingerprint maps to different typed material"
            )
        unique[fingerprint] = current

    fingerprints = tuple(sorted(unique))
    if not fingerprints:
        return ObservationResolution(
            observation_code=observation_code,
            state=ObservationResolutionState.UNRESOLVED,
            normalized_value=None,
            material_fingerprints=(),
            eligible_evidence_count=0,
        )
    if len(fingerprints) > 1:
        return ObservationResolution(
            observation_code=observation_code,
            state=ObservationResolutionState.CONFLICT,
            normalized_value=None,
            material_fingerprints=fingerprints,
            eligible_evidence_count=evidence_count,
        )

    state, normalized_value = unique[fingerprints[0]]
    return ObservationResolution(
        observation_code=observation_code,
        state=ObservationResolutionState(state.value),
        normalized_value=normalized_value,
        material_fingerprints=fingerprints,
        eligible_evidence_count=evidence_count,
    )


def _safe_latest_schedule_group(
    groups: dict[str, list[_ScheduleEvidence]],
) -> str | None:
    """Return a uniquely later material only when chronology safely orders groups."""

    chronology: dict[str, tuple[date | None, datetime | None, datetime | None]] = {}
    for fingerprint, evidence in groups.items():
        exact = [row.source_effective_at for row in evidence if row.source_effective_at]
        dated = [row.source_effective_date for row in evidence if row.source_effective_date]
        captures = [
            row.captured_at
            for row in evidence
            if row.capture_chronology_allowed
        ]
        chronology[fingerprint] = (
            max(dated) if dated else None,
            max(exact) if exact else None,
            max(captures) if captures else None,
        )

    if all(item[0] is not None for item in chronology.values()):
        latest_date = max(item[0] for item in chronology.values() if item[0] is not None)
        date_winners = [
            fingerprint
            for fingerprint, item in chronology.items()
            if item[0] == latest_date
        ]
        if len(date_winners) == 1:
            return date_winners[0]

        if all(chronology[fingerprint][1] is not None for fingerprint in date_winners):
            latest_exact = max(
                chronology[fingerprint][1] for fingerprint in date_winners
            )
            exact_winners = [
                fingerprint
                for fingerprint in date_winners
                if chronology[fingerprint][1] == latest_exact
            ]
            if len(exact_winners) == 1:
                return exact_winners[0]
        return None

    if all(item[2] is not None for item in chronology.values()):
        latest_capture = max(item[2] for item in chronology.values())
        capture_winners = [
            fingerprint
            for fingerprint, item in chronology.items()
            if item[2] == latest_capture
        ]
        if len(capture_winners) == 1:
            return capture_winners[0]

    return None


def _select_schedule(evidence: Iterable[_ScheduleEvidence]) -> _ScheduleSelection:
    rows = list(evidence)
    if not rows:
        return _ScheduleSelection("UNRESOLVED", None, (), False)

    authoritative = [
        row for row in rows
        if row.authority_role is SourceAuthorityRole.AUTHORITATIVE
    ]
    fallback = [
        row for row in rows
        if row.authority_role is SourceAuthorityRole.FALLBACK_CORROBORATION
    ]
    active = authoritative or fallback
    if not active:
        return _ScheduleSelection("UNRESOLVED", None, (), False)

    groups: dict[str, list[_ScheduleEvidence]] = {}
    for row in active:
        groups.setdefault(row.material_fingerprint, []).append(row)

    fingerprints = tuple(sorted(groups))
    contains_canceled = any(
        row.material.schedule_status is ScheduleStatus.CANCELED
        for row in active
    )
    if len(groups) == 1:
        only = next(iter(groups.values()))
        return _ScheduleSelection(
            "RESOLVED",
            only[0].material,
            fingerprints,
            contains_canceled,
        )

    winner = _safe_latest_schedule_group(groups)
    if winner is None:
        return _ScheduleSelection(
            "CONFLICT",
            None,
            fingerprints,
            contains_canceled,
        )
    return _ScheduleSelection(
        "RESOLVED",
        groups[winner][0].material,
        fingerprints,
        groups[winner][0].material.schedule_status is ScheduleStatus.CANCELED,
    )


def _project_release(
    schedule: _ScheduleSelection,
    *,
    has_valid_release: bool,
    evaluation_at: datetime,
) -> ReleaseProjectionState:
    _require_aware(evaluation_at, "evaluation_at")

    if schedule.kind == "CONFLICT":
        if has_valid_release and not schedule.contains_canceled_material:
            return ReleaseProjectionState.DISCLOSED
        return ReleaseProjectionState.CONFLICT

    if has_valid_release:
        if (
            schedule.material is not None
            and schedule.material.schedule_status is ScheduleStatus.CANCELED
        ):
            return ReleaseProjectionState.CONFLICT
        return ReleaseProjectionState.DISCLOSED

    material = schedule.material
    if material is None:
        return ReleaseProjectionState.UNRESOLVED

    if material.schedule_status is ScheduleStatus.CANCELED:
        return ReleaseProjectionState.NO_RELEASE_EXPECTED
    if material.schedule_status is ScheduleStatus.DATE_PENDING:
        return ReleaseProjectionState.UNRESOLVED

    if material.time_precision is TimePrecision.EXACT:
        if material.scheduled_at is None:
            raise SelectorDataIntegrityError("EXACT schedule lost scheduled_at")
        if evaluation_at < material.scheduled_at:
            return ReleaseProjectionState.NOT_YET_DUE
        return ReleaseProjectionState.AWAITING_CONFIRMATION

    if material.time_precision is TimePrecision.DATE_ONLY:
        if material.scheduled_date is None or not material.schedule_timezone:
            raise SelectorDataIntegrityError("DATE_ONLY schedule is incomplete")
        try:
            local_date = evaluation_at.astimezone(
                ZoneInfo(material.schedule_timezone)
            ).date()
        except ZoneInfoNotFoundError as exc:
            raise SelectorDataIntegrityError(
                "schedule timezone is not a valid IANA zone"
            ) from exc
        if local_date < material.scheduled_date:
            return ReleaseProjectionState.NOT_YET_DUE
        if local_date == material.scheduled_date:
            return ReleaseProjectionState.DUE_DATE_UNTIMED
        return ReleaseProjectionState.AWAITING_CONFIRMATION

    raise SelectorDataIntegrityError("scheduled evidence has unknown precision")


def _knowledge_fingerprint(
    event_occurrence_id: UUID,
    release_state: ReleaseProjectionState,
    observations: Iterable[ObservationResolution],
) -> str:
    ordered = sorted(observations, key=lambda item: item.observation_code)
    payload = {
        "selector_contract_version": SELECTOR_CONTRACT_VERSION,
        "event_occurrence_id": event_occurrence_id,
        "release_projection_state": release_state,
        "observations": {
            item.observation_code: {
                "state": item.state,
                "material_fingerprints": list(item.material_fingerprints),
            }
            for item in ordered
        },
    }
    encoded = canonical_material_json(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CpiW1Selector:
    def __init__(
        self,
        source_contract: BlsCpiSourceContract | None = None,
    ) -> None:
        self.source_contract = source_contract or _default_source_contract()

    @staticmethod
    def _decision_states(
        connection: Any,
        subject_ids: set[UUID],
        cutoff: datetime | None,
    ) -> dict[UUID, str]:
        if not subject_ids:
            return {}
        if cutoff is None:
            rows = connection.execute(
                """
                SELECT DISTINCT ON (subject_id) subject_id, decision_state
                  FROM interpretation_decisions
                 WHERE subject_id = ANY(%s::uuid[])
                 ORDER BY subject_id, decision_version DESC
                """,
                (list(subject_ids),),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT DISTINCT ON (subject_id) subject_id, decision_state
                  FROM interpretation_decisions
                 WHERE subject_id = ANY(%s::uuid[])
                   AND applied_at <= %s
                 ORDER BY subject_id, decision_version DESC
                """,
                (list(subject_ids), cutoff),
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    @staticmethod
    def _valid(
        subject_id: UUID,
        decisions: dict[UUID, str],
    ) -> bool:
        return decisions.get(subject_id, "VALID") == "VALID"

    @staticmethod
    def _visible(accepted_at: datetime, cutoff: datetime | None) -> bool:
        return cutoff is None or accepted_at <= cutoff

    def _schedule_evidence(
        self,
        connection: Any,
        event_id: UUID,
        cutoff: datetime | None,
    ) -> list[_ScheduleEvidence]:
        rows = connection.execute(
            """
            SELECT
                s.schedule_assertion_id,
                s.schedule_status,
                s.scheduled_date,
                s.scheduled_at,
                s.schedule_timezone,
                s.time_precision,
                s.source_effective_date,
                s.source_effective_at,
                s.source_effective_precision,
                s.accepted_at,
                s.material_fingerprint,
                a.source_contract_version,
                a.artifact_contract_kind,
                a.captured_at
              FROM event_schedule_assertions s
              JOIN source_artifacts a
                ON a.artifact_id = s.source_artifact_id
             WHERE s.event_occurrence_id=%s
            """,
            (event_id,),
        ).fetchall()

        visible = [row for row in rows if self._visible(row[9], cutoff)]
        decisions = self._decision_states(
            connection,
            {row[0] for row in visible},
            cutoff,
        )
        result: list[_ScheduleEvidence] = []
        for row in visible:
            if not self._valid(row[0], decisions):
                continue
            (
                assertion_id,
                schedule_status,
                scheduled_date,
                scheduled_at,
                schedule_timezone,
                time_precision,
                source_effective_date,
                source_effective_at,
                source_effective_precision,
                accepted_at,
                stored_fingerprint,
                source_contract_version,
                artifact_contract_kind,
                captured_at,
            ) = row
            if source_contract_version != self.source_contract.contract_version:
                raise SelectorDataIntegrityError(
                    "selector does not know schedule source contract version"
                )
            try:
                role = SourceAuthorityRole(
                    self.source_contract.surface_role_for_artifact_kind(
                        artifact_contract_kind
                    )
                )
            except (ValueError, KeyError) as exc:
                raise SelectorDataIntegrityError(
                    "schedule artifact kind has no canonical source role"
                ) from exc
            if role not in {
                SourceAuthorityRole.AUTHORITATIVE,
                SourceAuthorityRole.FALLBACK_CORROBORATION,
            }:
                continue
            try:
                material = ScheduleMaterial(
                    schedule_status=ScheduleStatus(schedule_status),
                    scheduled_date=scheduled_date,
                    scheduled_at=scheduled_at,
                    schedule_timezone=schedule_timezone,
                    time_precision=(
                        TimePrecision(time_precision)
                        if time_precision is not None
                        else None
                    ),
                )
            except (ValueError, TypeError) as exc:
                raise SelectorDataIntegrityError(
                    "stored schedule material is invalid"
                ) from exc
            if material.fingerprint != stored_fingerprint:
                raise SelectorDataIntegrityError(
                    "stored schedule material fingerprint mismatch"
                )
            precision = (
                TimePrecision(source_effective_precision)
                if source_effective_precision is not None
                else None
            )
            result.append(
                _ScheduleEvidence(
                    assertion_id=assertion_id,
                    material=material,
                    material_fingerprint=stored_fingerprint,
                    source_contract_version=source_contract_version,
                    artifact_contract_kind=artifact_contract_kind,
                    source_effective_date=source_effective_date,
                    source_effective_at=source_effective_at,
                    source_effective_precision=precision,
                    captured_at=captured_at,
                    accepted_at=accepted_at,
                    capture_chronology_allowed=(
                        self.source_contract.capture_chronology_allowed_for_artifact_kind(
                            artifact_contract_kind
                        )
                    ),
                    authority_role=role,
                )
            )
        return result

    def _valid_release_links(
        self,
        connection: Any,
        event_id: UUID,
        cutoff: datetime | None,
    ) -> set[UUID]:
        rows = connection.execute(
            """
            SELECT disclosure_link_id, disclosure_id, relation_kind, accepted_at
              FROM event_disclosure_links
             WHERE event_occurrence_id=%s
            """,
            (event_id,),
        ).fetchall()
        visible = [row for row in rows if self._visible(row[3], cutoff)]
        decisions = self._decision_states(
            connection,
            {row[0] for row in visible},
            cutoff,
        )
        return {
            row[1]
            for row in visible
            if row[2] == "EVENT_RELEASE"
            and self._valid(row[0], decisions)
        }

    def _observation_resolutions(
        self,
        connection: Any,
        event_id: UUID,
        cutoff: datetime | None,
    ) -> tuple[ObservationResolution, ...]:
        rows = connection.execute(
            """
            SELECT
                o.assertion_id,
                o.observation_code,
                o.assertion_state,
                o.normalized_value,
                o.material_fingerprint,
                o.accepted_at,
                l.disclosure_link_id,
                l.relation_kind,
                l.accepted_at,
                da.disclosure_artifact_link_id,
                da.relation_kind,
                da.accepted_at
              FROM official_observation_assertions o
              JOIN event_disclosure_links l
                ON l.disclosure_link_id = o.disclosure_link_id
               AND l.event_occurrence_id = o.event_occurrence_id
               AND l.disclosure_id = o.disclosure_id
              JOIN event_disclosure_artifacts da
                ON da.disclosure_artifact_link_id = o.disclosure_artifact_link_id
               AND da.disclosure_id = o.disclosure_id
               AND da.artifact_id = o.source_artifact_id
             WHERE o.event_occurrence_id=%s
            """,
            (event_id,),
        ).fetchall()
        subject_ids: set[UUID] = set()
        for row in rows:
            subject_ids.update((row[0], row[6], row[9]))
        decisions = self._decision_states(connection, subject_ids, cutoff)

        by_code: dict[
            str,
            list[tuple[str, ObservationState, Decimal | None]],
        ] = {code: [] for code in _CORE4}

        for row in rows:
            (
                assertion_id,
                code,
                assertion_state,
                normalized_value,
                stored_fingerprint,
                assertion_accepted_at,
                disclosure_link_id,
                disclosure_relation_kind,
                disclosure_accepted_at,
                artifact_link_id,
                artifact_relation_kind,
                artifact_accepted_at,
            ) = row
            if code not in by_code:
                continue
            if not all(
                (
                    self._visible(assertion_accepted_at, cutoff),
                    self._visible(disclosure_accepted_at, cutoff),
                    self._visible(artifact_accepted_at, cutoff),
                    self._valid(assertion_id, decisions),
                    self._valid(disclosure_link_id, decisions),
                    self._valid(artifact_link_id, decisions),
                )
            ):
                continue
            if disclosure_relation_kind != "EVENT_RELEASE":
                continue
            if artifact_relation_kind not in _ALLOWED_OBSERVATION_ARTIFACT_RELATIONS:
                continue

            try:
                state = ObservationState(assertion_state)
                material = ObservationMaterial(
                    observation_code=code,
                    assertion_state=state,
                    normalized_value=normalized_value,
                )
            except (ValueError, TypeError) as exc:
                raise SelectorDataIntegrityError(
                    "stored observation material is invalid"
                ) from exc
            if material.fingerprint != stored_fingerprint:
                raise SelectorDataIntegrityError(
                    "stored observation material fingerprint mismatch"
                )
            by_code[code].append(
                (stored_fingerprint, state, normalized_value)
            )

        return tuple(
            _resolution_from_materials(code, by_code[code])
            for code in _CORE4
        )

    def select_event(
        self,
        connection: Any,
        event_id: UUID,
        mode: KnowledgeMode,
        as_of: datetime | None = None,
    ) -> CpiEventKnowledge:
        if not isinstance(mode, KnowledgeMode):
            mode = KnowledgeMode(mode)
        if mode is KnowledgeMode.SYSTEM_KNOWN_PIT:
            if as_of is None:
                raise ValueError("SYSTEM_KNOWN_PIT requires as_of")
            _require_aware(as_of, "as_of")
        elif as_of is not None:
            raise ValueError(
                "OFFICIAL_SOURCE_RECONSTRUCTION does not accept caller as_of"
            )

        with connection.transaction():
            row = connection.execute(
                """
                SELECT event_occurrence_id, reference_month
                  FROM core_event_occurrences
                 WHERE event_occurrence_id=%s
                   AND event_type='CPI'
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"CPI event occurrence not found: {event_id}")

            if mode is KnowledgeMode.SYSTEM_KNOWN_PIT:
                cutoff = as_of
                evaluation_at = as_of
            else:
                cutoff = None
                evaluation_at = connection.execute(
                    "SELECT CURRENT_TIMESTAMP"
                ).fetchone()[0]

            schedule = _select_schedule(
                self._schedule_evidence(connection, event_id, cutoff)
            )
            release_ids = self._valid_release_links(
                connection,
                event_id,
                cutoff,
            )
            observations = self._observation_resolutions(
                connection,
                event_id,
                cutoff,
            )
            release_state = _project_release(
                schedule,
                has_valid_release=bool(release_ids),
                evaluation_at=evaluation_at,
            )
            fingerprint = _knowledge_fingerprint(
                event_id,
                release_state,
                observations,
            )
            return CpiEventKnowledge(
                event_occurrence_id=row[0],
                reference_month=row[1],
                mode=mode,
                release_state=release_state,
                observations=observations,
                knowledge_fingerprint=fingerprint,
            )

    @staticmethod
    def _control_state(
        connection: Any,
        *,
        scope_kind: str,
        event_id: UUID | None,
        decision_time: datetime,
    ) -> ServingControlState:
        if scope_kind == "CPI_DOMAIN":
            row = connection.execute(
                """
                SELECT state
                  FROM economic_serving_control_decisions
                 WHERE scope_kind='CPI_DOMAIN'
                   AND applied_at <= %s
                 ORDER BY control_version DESC
                 LIMIT 1
                """,
                (decision_time,),
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT state
                  FROM economic_serving_control_decisions
                 WHERE scope_kind='EVENT_OCCURRENCE'
                   AND event_occurrence_id=%s
                   AND applied_at <= %s
                 ORDER BY control_version DESC
                 LIMIT 1
                """,
                (event_id, decision_time),
            ).fetchone()
        return (
            ServingControlState(row[0])
            if row is not None
            else ServingControlState.ENABLED
        )

    def apply_serving_overlay(
        self,
        connection: Any,
        knowledge: CpiEventKnowledge,
        decision_time: datetime | None = None,
    ) -> GovernedCpiEvent:
        with connection.transaction():
            if decision_time is None:
                decision_time = connection.execute(
                    "SELECT CURRENT_TIMESTAMP"
                ).fetchone()[0]
            else:
                _require_aware(decision_time, "decision_time")

            domain_state = self._control_state(
                connection,
                scope_kind="CPI_DOMAIN",
                event_id=None,
                decision_time=decision_time,
            )
            event_state = self._control_state(
                connection,
                scope_kind="EVENT_OCCURRENCE",
                event_id=knowledge.event_occurrence_id,
                decision_time=decision_time,
            )
            effective = (
                ServingControlState.WITHHELD
                if (
                    domain_state is ServingControlState.WITHHELD
                    or event_state is ServingControlState.WITHHELD
                )
                else ServingControlState.ENABLED
            )

            observations = tuple(
                GovernedObservation(
                    observation_code=item.observation_code,
                    knowledge_state=item.state,
                    normalized_value=(
                        None
                        if (
                            effective is ServingControlState.WITHHELD
                            and item.state is ObservationResolutionState.VALUE
                        )
                        else item.normalized_value
                    ),
                    withheld=(
                        effective is ServingControlState.WITHHELD
                        and item.state is ObservationResolutionState.VALUE
                    ),
                )
                for item in knowledge.observations
            )
            return GovernedCpiEvent(
                knowledge=knowledge,
                effective_control_state=effective,
                observations=observations,
            )
