"""CPI W1 knowledge selector, PIT reconstruction, and serving overlay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import UUID
from zoneinfo import ZoneInfo

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
    material_fingerprint,
)
from src.cpi_w1_source import BlsCpiSourceContract


SELECTOR_CONTRACT_VERSION = "cpi-selector-v1"
DEFAULT_SOURCE_CONTRACT_PATH = Path("config/cpi_w1_source_contract.json")
CORE4_CODES = (
    "CPI_HEADLINE_MOM",
    "CPI_HEADLINE_YOY",
    "CPI_CORE_MOM",
    "CPI_CORE_YOY",
)


class SelectorInvariantError(RuntimeError):
    pass


class ObservationResolutionState(StrEnum):
    UNRESOLVED = "UNRESOLVED"
    VALUE = "VALUE"
    EXPLICIT_UNAVAILABLE = "EXPLICIT_UNAVAILABLE"
    CONFLICT = "CONFLICT"


class ScheduleSelectionState(StrEnum):
    UNRESOLVED = "UNRESOLVED"
    RESOLVED = "RESOLVED"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class ObservationEvidence:
    observation_code: str
    assertion_state: ObservationState
    normalized_value: Decimal | None
    material_fingerprint: str


@dataclass(frozen=True)
class ObservationResolution:
    observation_code: str
    state: ObservationResolutionState
    normalized_value: Decimal | None
    material_fingerprints: tuple[str, ...]


@dataclass(frozen=True)
class ScheduleEvidence:
    schedule_status: ScheduleStatus
    scheduled_date: date | None
    scheduled_at: datetime | None
    schedule_timezone: str | None
    time_precision: TimePrecision | None
    source_effective_date: date | None
    source_effective_at: datetime | None
    source_effective_precision: TimePrecision | None
    material_fingerprint: str
    source_role: SourceAuthorityRole
    capture_chronology_allowed: bool = False
    captured_at: datetime | None = None


@dataclass(frozen=True)
class ScheduleSelection:
    state: ScheduleSelectionState
    evidence: ScheduleEvidence | None
    material_fingerprints: tuple[str, ...]


@dataclass(frozen=True)
class CpiEventKnowledge:
    event_occurrence_id: UUID
    reference_month: date
    mode: KnowledgeMode
    release_state: ReleaseProjectionState
    schedule: ScheduleSelection
    observations: Mapping[str, ObservationResolution]
    knowledge_fingerprint: str
    knowledge_as_of: datetime | None


@dataclass(frozen=True)
class GovernedObservation:
    observation_code: str
    state: str
    normalized_value: Decimal | None


@dataclass(frozen=True)
class GovernedCpiEvent:
    knowledge: CpiEventKnowledge
    serving_state: ServingControlState
    observations: Mapping[str, GovernedObservation]


def _validate_aware(value: datetime, *, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def resolve_observation_materials(
    observation_code: str,
    evidence: Iterable[ObservationEvidence],
) -> ObservationResolution:
    by_material: dict[str, ObservationEvidence] = {}
    for row in evidence:
        if row.observation_code != observation_code:
            raise SelectorInvariantError("observation evidence code mismatch")
        semantic = ObservationMaterial(
            observation_code=row.observation_code,
            assertion_state=row.assertion_state,
            normalized_value=row.normalized_value,
        )
        if semantic.fingerprint != row.material_fingerprint:
            raise SelectorInvariantError("stored observation material fingerprint mismatch")
        existing = by_material.setdefault(row.material_fingerprint, row)
        if (
            existing.assertion_state != row.assertion_state
            or existing.normalized_value != row.normalized_value
        ):
            raise SelectorInvariantError("same observation fingerprint has different material")

    fingerprints = tuple(sorted(by_material))
    if not fingerprints:
        return ObservationResolution(
            observation_code,
            ObservationResolutionState.UNRESOLVED,
            None,
            (),
        )
    if len(fingerprints) > 1:
        return ObservationResolution(
            observation_code,
            ObservationResolutionState.CONFLICT,
            None,
            fingerprints,
        )

    row = by_material[fingerprints[0]]
    if row.assertion_state is ObservationState.VALUE:
        return ObservationResolution(
            observation_code,
            ObservationResolutionState.VALUE,
            row.normalized_value,
            fingerprints,
        )
    return ObservationResolution(
        observation_code,
        ObservationResolutionState.EXPLICIT_UNAVAILABLE,
        None,
        fingerprints,
    )


def _validate_schedule_fingerprint(row: ScheduleEvidence) -> None:
    semantic = ScheduleMaterial(
        schedule_status=row.schedule_status,
        scheduled_date=row.scheduled_date,
        scheduled_at=row.scheduled_at,
        schedule_timezone=row.schedule_timezone,
        time_precision=row.time_precision,
    )
    if semantic.fingerprint != row.material_fingerprint:
        raise SelectorInvariantError("stored schedule material fingerprint mismatch")


def _chronology(row: ScheduleEvidence) -> tuple[date, datetime | None] | None:
    if row.source_effective_precision is TimePrecision.EXACT:
        if row.source_effective_date is None or row.source_effective_at is None:
            raise SelectorInvariantError("EXACT source chronology is incomplete")
        _validate_aware(row.source_effective_at, name="source_effective_at")
        return row.source_effective_date, row.source_effective_at
    if row.source_effective_precision is TimePrecision.DATE_ONLY:
        if row.source_effective_date is None or row.source_effective_at is not None:
            raise SelectorInvariantError("DATE_ONLY source chronology is invalid")
        return row.source_effective_date, None
    if (
        row.source_effective_precision is None
        and row.source_effective_date is None
        and row.source_effective_at is None
    ):
        if not row.capture_chronology_allowed:
            return None
        if row.captured_at is None:
            raise SelectorInvariantError("capture chronology requires captured_at")
        _validate_aware(row.captured_at, name="captured_at")
        instant = row.captured_at.astimezone(timezone.utc)
        return instant.date(), instant
    raise SelectorInvariantError("source chronology fields are inconsistent")


def _latest_comparable(rows: list[ScheduleEvidence]) -> ScheduleEvidence | None:
    ranked = [(row, _chronology(row)) for row in rows]
    if any(rank is None for _, rank in ranked):
        return None
    max_date = max(rank[0] for _, rank in ranked if rank is not None)
    same_date = [
        (row, rank)
        for row, rank in ranked
        if rank is not None and rank[0] == max_date
    ]
    if len(same_date) == 1:
        return same_date[0][0]
    if any(rank[1] is None for _, rank in same_date):
        materials = {row.material_fingerprint for row, _ in same_date}
        return same_date[0][0] if len(materials) == 1 else None
    max_instant = max(rank[1] for _, rank in same_date if rank[1] is not None)
    latest = [
        row
        for row, rank in same_date
        if rank[1] == max_instant
    ]
    return latest[0] if len({row.material_fingerprint for row in latest}) == 1 else None


def select_schedule_evidence(
    evidence: Iterable[ScheduleEvidence],
) -> ScheduleSelection:
    rows = list(evidence)
    if not rows:
        return ScheduleSelection(ScheduleSelectionState.UNRESOLVED, None, ())
    for row in rows:
        _validate_schedule_fingerprint(row)

    authoritative = [
        row for row in rows
        if row.source_role is SourceAuthorityRole.AUTHORITATIVE
    ]
    fallback = [
        row for row in rows
        if row.source_role is SourceAuthorityRole.FALLBACK_CORROBORATION
    ]
    active = authoritative if authoritative else fallback
    if not active:
        return ScheduleSelection(ScheduleSelectionState.UNRESOLVED, None, ())

    by_material: dict[str, list[ScheduleEvidence]] = {}
    for row in active:
        by_material.setdefault(row.material_fingerprint, []).append(row)
    fingerprints = tuple(sorted(by_material))
    if len(fingerprints) == 1:
        return ScheduleSelection(
            ScheduleSelectionState.RESOLVED,
            _latest_comparable(active) or active[0],
            fingerprints,
        )

    representatives = [
        _latest_comparable(group) or group[0]
        for group in by_material.values()
    ]
    chosen = _latest_comparable(representatives)
    if chosen is None:
        return ScheduleSelection(
            ScheduleSelectionState.CONFLICT,
            None,
            fingerprints,
        )
    return ScheduleSelection(
        ScheduleSelectionState.RESOLVED,
        chosen,
        (chosen.material_fingerprint,),
    )


def project_release_state(
    *,
    schedule: ScheduleSelection,
    valid_event_release_count: int,
    evaluation_time: datetime,
) -> ReleaseProjectionState:
    _validate_aware(evaluation_time, name="evaluation_time")
    if valid_event_release_count < 0:
        raise ValueError("valid_event_release_count must be non-negative")
    if valid_event_release_count > 1:
        return ReleaseProjectionState.CONFLICT
    if valid_event_release_count == 1:
        if (
            schedule.state is ScheduleSelectionState.RESOLVED
            and schedule.evidence is not None
            and schedule.evidence.schedule_status is ScheduleStatus.CANCELED
        ):
            return ReleaseProjectionState.CONFLICT
        return ReleaseProjectionState.DISCLOSED

    if schedule.state is ScheduleSelectionState.CONFLICT:
        return ReleaseProjectionState.CONFLICT
    if schedule.state is ScheduleSelectionState.UNRESOLVED or schedule.evidence is None:
        return ReleaseProjectionState.UNRESOLVED

    selected = schedule.evidence
    if selected.schedule_status is ScheduleStatus.CANCELED:
        return ReleaseProjectionState.NO_RELEASE_EXPECTED
    if selected.schedule_status is ScheduleStatus.DATE_PENDING:
        return ReleaseProjectionState.UNRESOLVED
    if selected.schedule_status is not ScheduleStatus.SCHEDULED:
        raise SelectorInvariantError("unsupported schedule status")

    if selected.time_precision is TimePrecision.EXACT:
        if selected.scheduled_at is None:
            raise SelectorInvariantError("EXACT schedule lacks scheduled_at")
        return (
            ReleaseProjectionState.NOT_YET_DUE
            if evaluation_time < selected.scheduled_at
            else ReleaseProjectionState.AWAITING_CONFIRMATION
        )
    if selected.time_precision is TimePrecision.DATE_ONLY:
        if selected.scheduled_date is None or not selected.schedule_timezone:
            raise SelectorInvariantError("DATE_ONLY schedule is incomplete")
        local_date = evaluation_time.astimezone(
            ZoneInfo(selected.schedule_timezone)
        ).date()
        if local_date < selected.scheduled_date:
            return ReleaseProjectionState.NOT_YET_DUE
        if local_date == selected.scheduled_date:
            return ReleaseProjectionState.DUE_DATE_UNTIMED
        return ReleaseProjectionState.AWAITING_CONFIRMATION
    raise SelectorInvariantError("scheduled evidence has no usable precision")


def build_knowledge_fingerprint(
    *,
    event_occurrence_id: UUID,
    release_state: ReleaseProjectionState,
    observations: Mapping[str, ObservationResolution],
) -> str:
    return material_fingerprint(
        {
            "selector_contract_version": SELECTOR_CONTRACT_VERSION,
            "event_occurrence_id": event_occurrence_id,
            "release_projection_state": release_state,
            "observations": {
                code: {
                    "state": observations[code].state.value,
                    "material_fingerprints": list(
                        sorted(observations[code].material_fingerprints)
                    ),
                }
                for code in sorted(observations)
            },
        }
    )


def apply_control_states(
    knowledge: CpiEventKnowledge,
    *,
    domain_state: ServingControlState,
    event_state: ServingControlState,
) -> GovernedCpiEvent:
    effective = (
        ServingControlState.WITHHELD
        if ServingControlState.WITHHELD in {domain_state, event_state}
        else ServingControlState.ENABLED
    )
    observations: dict[str, GovernedObservation] = {}
    for code, resolution in knowledge.observations.items():
        if (
            effective is ServingControlState.WITHHELD
            and resolution.state is ObservationResolutionState.VALUE
        ):
            observations[code] = GovernedObservation(code, "WITHHELD", None)
        else:
            observations[code] = GovernedObservation(
                code,
                resolution.state.value,
                resolution.normalized_value,
            )
    return GovernedCpiEvent(knowledge, effective, observations)


class CpiW1Selector:
    def __init__(
        self,
        connection: Any,
        *,
        source_contract_path: str | Path = DEFAULT_SOURCE_CONTRACT_PATH,
    ) -> None:
        self.connection = connection
        self.source_contract = BlsCpiSourceContract.from_json(source_contract_path)

    @staticmethod
    def _cutoff(mode: KnowledgeMode, as_of: datetime | None) -> datetime | None:
        if mode is KnowledgeMode.SYSTEM_KNOWN_PIT:
            if as_of is None:
                raise ValueError("SYSTEM_KNOWN_PIT requires as_of")
            _validate_aware(as_of, name="as_of")
            return as_of
        if as_of is not None:
            raise ValueError("OFFICIAL_SOURCE_RECONSTRUCTION does not accept as_of")
        return None

    def _schedule_rows(
        self,
        event_occurrence_id: UUID,
        cutoff: datetime | None,
    ) -> list[ScheduleEvidence]:
        rows = self.connection.execute(
            """
            SELECT s.schedule_status, s.scheduled_date, s.scheduled_at,
                   s.schedule_timezone, s.time_precision,
                   s.source_effective_date, s.source_effective_at,
                   s.source_effective_precision, s.material_fingerprint,
                   a.artifact_contract_kind, a.source_contract_version,
                   a.captured_at, COALESCE(d.decision_state, 'VALID')
              FROM event_schedule_assertions s
              JOIN source_artifacts a
                ON a.artifact_id=s.source_artifact_id
               AND a.source_code=s.source_code
              LEFT JOIN LATERAL (
                    SELECT decision_state
                      FROM interpretation_decisions
                     WHERE subject_id=s.schedule_assertion_id
                       AND (%s::timestamptz IS NULL OR applied_at <= %s)
                     ORDER BY decision_version DESC LIMIT 1
              ) d ON TRUE
             WHERE s.event_occurrence_id=%s
               AND (%s::timestamptz IS NULL OR s.accepted_at <= %s)
            """,
            (cutoff, cutoff, event_occurrence_id, cutoff, cutoff),
        ).fetchall()
        selected: list[ScheduleEvidence] = []
        for row in rows:
            (
                status, scheduled_date, scheduled_at, schedule_timezone, precision,
                effective_date, effective_at, effective_precision, fingerprint,
                artifact_kind, source_contract_version, captured_at, decision_state,
            ) = row
            if decision_state != "VALID":
                continue
            if source_contract_version != self.source_contract.contract_version:
                raise SelectorInvariantError("unsupported schedule source contract version")
            role = self.source_contract.surface_role_for_artifact_kind(artifact_kind)
            if role not in {
                SourceAuthorityRole.AUTHORITATIVE.value,
                SourceAuthorityRole.FALLBACK_CORROBORATION.value,
                SourceAuthorityRole.FIXTURE_ONLY.value,
            }:
                raise SelectorInvariantError("artifact is not a schedule source surface")
            selected.append(
                ScheduleEvidence(
                    ScheduleStatus(status),
                    scheduled_date,
                    scheduled_at,
                    schedule_timezone,
                    TimePrecision(precision) if precision else None,
                    effective_date,
                    effective_at,
                    TimePrecision(effective_precision) if effective_precision else None,
                    fingerprint,
                    SourceAuthorityRole(role),
                    self.source_contract.capture_chronology_allowed_for_artifact_kind(
                        artifact_kind
                    ),
                    captured_at,
                )
            )
        return selected

    def _valid_release_count(
        self,
        event_occurrence_id: UUID,
        cutoff: datetime | None,
    ) -> int:
        rows = self.connection.execute(
            """
            SELECT COALESCE(d.decision_state, 'VALID')
              FROM event_disclosure_links l
              LEFT JOIN LATERAL (
                    SELECT decision_state
                      FROM interpretation_decisions
                     WHERE subject_id=l.disclosure_link_id
                       AND (%s::timestamptz IS NULL OR applied_at <= %s)
                     ORDER BY decision_version DESC LIMIT 1
              ) d ON TRUE
             WHERE l.event_occurrence_id=%s
               AND l.relation_kind='EVENT_RELEASE'
               AND (%s::timestamptz IS NULL OR l.accepted_at <= %s)
            """,
            (cutoff, cutoff, event_occurrence_id, cutoff, cutoff),
        ).fetchall()
        return sum(1 for (state,) in rows if state == "VALID")

    def _observation_rows(
        self,
        event_occurrence_id: UUID,
        cutoff: datetime | None,
    ) -> dict[str, list[ObservationEvidence]]:
        rows = self.connection.execute(
            """
            SELECT o.observation_code, o.assertion_state, o.normalized_value,
                   o.material_fingerprint,
                   COALESCE(od.decision_state, 'VALID'),
                   COALESCE(ld.decision_state, 'VALID'),
                   COALESCE(ad.decision_state, 'VALID')
              FROM official_observation_assertions o
              JOIN event_disclosure_links l
                ON l.disclosure_link_id=o.disclosure_link_id
               AND l.event_occurrence_id=o.event_occurrence_id
               AND l.disclosure_id=o.disclosure_id
               AND l.relation_kind='EVENT_RELEASE'
              JOIN event_disclosure_artifacts da
                ON da.disclosure_artifact_link_id=o.disclosure_artifact_link_id
               AND da.disclosure_id=o.disclosure_id
               AND da.artifact_id=o.source_artifact_id
              LEFT JOIN LATERAL (
                    SELECT decision_state FROM interpretation_decisions
                     WHERE subject_id=o.assertion_id
                       AND (%s::timestamptz IS NULL OR applied_at <= %s)
                     ORDER BY decision_version DESC LIMIT 1
              ) od ON TRUE
              LEFT JOIN LATERAL (
                    SELECT decision_state FROM interpretation_decisions
                     WHERE subject_id=l.disclosure_link_id
                       AND (%s::timestamptz IS NULL OR applied_at <= %s)
                     ORDER BY decision_version DESC LIMIT 1
              ) ld ON TRUE
              LEFT JOIN LATERAL (
                    SELECT decision_state FROM interpretation_decisions
                     WHERE subject_id=da.disclosure_artifact_link_id
                       AND (%s::timestamptz IS NULL OR applied_at <= %s)
                     ORDER BY decision_version DESC LIMIT 1
              ) ad ON TRUE
             WHERE o.event_occurrence_id=%s
               AND (%s::timestamptz IS NULL OR o.accepted_at <= %s)
               AND (%s::timestamptz IS NULL OR l.accepted_at <= %s)
               AND (%s::timestamptz IS NULL OR da.accepted_at <= %s)
            """,
            (
                cutoff, cutoff, cutoff, cutoff, cutoff, cutoff,
                event_occurrence_id,
                cutoff, cutoff, cutoff, cutoff, cutoff, cutoff,
            ),
        ).fetchall()
        result = {code: [] for code in CORE4_CODES}
        for code, state, value, fingerprint, own_valid, link_valid, artifact_valid in rows:
            if code not in result or {own_valid, link_valid, artifact_valid} != {"VALID"}:
                continue
            result[code].append(
                ObservationEvidence(
                    code,
                    ObservationState(state),
                    value,
                    fingerprint,
                )
            )
        return result

    def select_event(
        self,
        event_occurrence_id: UUID,
        mode: KnowledgeMode,
        as_of: datetime | None = None,
    ) -> CpiEventKnowledge:
        cutoff = self._cutoff(mode, as_of)
        with self.connection.transaction():
            evaluation_time = (
                cutoff
                if mode is KnowledgeMode.SYSTEM_KNOWN_PIT
                else self.connection.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
            )
            event = self.connection.execute(
                """
                SELECT event_type, reference_month
                  FROM core_event_occurrences
                 WHERE event_occurrence_id=%s
                """,
                (event_occurrence_id,),
            ).fetchone()
            if event is None:
                raise KeyError(f"unknown CPI event occurrence: {event_occurrence_id}")
            if event[0] != "CPI":
                raise SelectorInvariantError("selector received non-CPI event")

            schedule = select_schedule_evidence(
                self._schedule_rows(event_occurrence_id, cutoff)
            )
            release_state = project_release_state(
                schedule=schedule,
                valid_event_release_count=self._valid_release_count(
                    event_occurrence_id,
                    cutoff,
                ),
                evaluation_time=evaluation_time,
            )
            evidence = self._observation_rows(event_occurrence_id, cutoff)
            observations = {
                code: resolve_observation_materials(code, evidence[code])
                for code in CORE4_CODES
            }
            fingerprint = build_knowledge_fingerprint(
                event_occurrence_id=event_occurrence_id,
                release_state=release_state,
                observations=observations,
            )
            return CpiEventKnowledge(
                event_occurrence_id,
                event[1],
                mode,
                release_state,
                schedule,
                observations,
                fingerprint,
                cutoff,
            )

    def apply_serving_overlay(
        self,
        knowledge: CpiEventKnowledge,
        decision_time: datetime,
    ) -> GovernedCpiEvent:
        _validate_aware(decision_time, name="decision_time")
        domain = self.connection.execute(
            """
            SELECT state FROM economic_serving_control_decisions
             WHERE scope_kind='CPI_DOMAIN' AND applied_at <= %s
             ORDER BY control_version DESC LIMIT 1
            """,
            (decision_time,),
        ).fetchone()
        event = self.connection.execute(
            """
            SELECT state FROM economic_serving_control_decisions
             WHERE scope_kind='EVENT_OCCURRENCE'
               AND event_occurrence_id=%s AND applied_at <= %s
             ORDER BY control_version DESC LIMIT 1
            """,
            (knowledge.event_occurrence_id, decision_time),
        ).fetchone()
        return apply_control_states(
            knowledge,
            domain_state=ServingControlState(domain[0]) if domain else ServingControlState.ENABLED,
            event_state=ServingControlState(event[0]) if event else ServingControlState.ENABLED,
        )
