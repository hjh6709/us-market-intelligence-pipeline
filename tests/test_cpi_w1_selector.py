import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

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
)
from src.cpi_w1_selector import (
    CpiEventKnowledge,
    ObservationEvidence,
    ObservationResolutionState,
    ScheduleEvidence,
    ScheduleSelection,
    ScheduleSelectionState,
    apply_control_states,
    build_knowledge_fingerprint,
    project_release_state,
    resolve_observation_materials,
    select_schedule_evidence,
)


EVENT_ID = UUID("00000000-0000-0000-0000-000000000321")


def schedule_row(
    *,
    status=ScheduleStatus.SCHEDULED,
    scheduled_date=date(2026, 9, 11),
    scheduled_at=datetime(2026, 9, 11, 12, 30, tzinfo=timezone.utc),
    timezone_name="America/New_York",
    precision=TimePrecision.EXACT,
    role=SourceAuthorityRole.AUTHORITATIVE,
    source_effective_date=None,
    source_effective_at=None,
    source_effective_precision=None,
):
    material = ScheduleMaterial(
        schedule_status=status,
        scheduled_date=scheduled_date,
        scheduled_at=scheduled_at,
        schedule_timezone=timezone_name,
        time_precision=precision,
    )
    return ScheduleEvidence(
        status,
        scheduled_date,
        scheduled_at,
        timezone_name,
        precision,
        source_effective_date,
        source_effective_at,
        source_effective_precision,
        material.fingerprint,
        role,
    )


def value_evidence(code: str, value: str) -> ObservationEvidence:
    material = ObservationMaterial(
        observation_code=code,
        assertion_state=ObservationState.VALUE,
        normalized_value=Decimal(value),
    )
    return ObservationEvidence(
        code,
        ObservationState.VALUE,
        Decimal(value),
        material.fingerprint,
    )


class CpiW1SelectorTest(unittest.TestCase):
    def test_same_material_from_multiple_artifacts_converges(self) -> None:
        row = value_evidence("CPI_CORE_MOM", "0.3")
        result = resolve_observation_materials("CPI_CORE_MOM", [row, row])
        self.assertEqual(result.state, ObservationResolutionState.VALUE)
        self.assertEqual(result.normalized_value, Decimal("0.3"))
        self.assertEqual(len(result.material_fingerprints), 1)

    def test_different_official_materials_conflict_without_latest_wins(self) -> None:
        result = resolve_observation_materials(
            "CPI_CORE_MOM",
            [
                value_evidence("CPI_CORE_MOM", "0.3"),
                value_evidence("CPI_CORE_MOM", "0.4"),
            ],
        )
        self.assertEqual(result.state, ObservationResolutionState.CONFLICT)
        self.assertIsNone(result.normalized_value)

    def test_explicit_unavailable_is_not_zero(self) -> None:
        semantic = ObservationMaterial(
            observation_code="CPI_HEADLINE_MOM",
            assertion_state=ObservationState.EXPLICIT_UNAVAILABLE,
            normalized_value=None,
        )
        result = resolve_observation_materials(
            "CPI_HEADLINE_MOM",
            [
                ObservationEvidence(
                    "CPI_HEADLINE_MOM",
                    ObservationState.EXPLICIT_UNAVAILABLE,
                    None,
                    semantic.fingerprint,
                )
            ],
        )
        self.assertEqual(result.state, ObservationResolutionState.EXPLICIT_UNAVAILABLE)
        self.assertIsNone(result.normalized_value)

    def test_authoritative_schedule_excludes_later_fallback(self) -> None:
        authoritative = schedule_row()
        fallback = schedule_row(
            scheduled_date=date(2026, 9, 12),
            scheduled_at=datetime(2026, 9, 12, 12, 30, tzinfo=timezone.utc),
            role=SourceAuthorityRole.FALLBACK_CORROBORATION,
        )
        selected = select_schedule_evidence([fallback, authoritative])
        self.assertEqual(selected.state, ScheduleSelectionState.RESOLVED)
        self.assertEqual(
            selected.evidence.material_fingerprint,
            authoritative.material_fingerprint,
        )

    def test_later_authoritative_source_chronology_supersedes(self) -> None:
        first = schedule_row(
            source_effective_date=date(2026, 8, 1),
            source_effective_at=datetime(2026, 8, 1, 15, tzinfo=timezone.utc),
            source_effective_precision=TimePrecision.EXACT,
        )
        second = schedule_row(
            scheduled_date=date(2026, 9, 12),
            scheduled_at=datetime(2026, 9, 12, 12, 30, tzinfo=timezone.utc),
            source_effective_date=date(2026, 8, 2),
            source_effective_at=datetime(2026, 8, 2, 15, tzinfo=timezone.utc),
            source_effective_precision=TimePrecision.EXACT,
        )
        selected = select_schedule_evidence([first, second])
        self.assertEqual(selected.state, ScheduleSelectionState.RESOLVED)
        self.assertEqual(
            selected.evidence.material_fingerprint,
            second.material_fingerprint,
        )

    def test_unknown_authoritative_chronology_remains_conflict(self) -> None:
        selected = select_schedule_evidence(
            [
                schedule_row(),
                schedule_row(
                    scheduled_date=date(2026, 9, 12),
                    scheduled_at=datetime(2026, 9, 12, 12, 30, tzinfo=timezone.utc),
                ),
            ]
        )
        self.assertEqual(selected.state, ScheduleSelectionState.CONFLICT)

    def test_exact_projection_before_and_after_due(self) -> None:
        selected = select_schedule_evidence([schedule_row()])
        self.assertEqual(
            project_release_state(
                schedule=selected,
                valid_event_release_count=0,
                evaluation_time=datetime(2026, 9, 11, 12, 29, tzinfo=timezone.utc),
            ),
            ReleaseProjectionState.NOT_YET_DUE,
        )
        self.assertEqual(
            project_release_state(
                schedule=selected,
                valid_event_release_count=0,
                evaluation_time=datetime(2026, 9, 11, 12, 31, tzinfo=timezone.utc),
            ),
            ReleaseProjectionState.AWAITING_CONFIRMATION,
        )

    def test_date_only_due_date_never_synthesizes_midnight(self) -> None:
        selected = select_schedule_evidence(
            [schedule_row(scheduled_at=None, precision=TimePrecision.DATE_ONLY)]
        )
        self.assertEqual(
            project_release_state(
                schedule=selected,
                valid_event_release_count=0,
                evaluation_time=datetime(2026, 9, 11, 16, tzinfo=timezone.utc),
            ),
            ReleaseProjectionState.DUE_DATE_UNTIMED,
        )

    def test_canceled_schedule_plus_release_is_conflict(self) -> None:
        canceled = select_schedule_evidence(
            [
                schedule_row(
                    status=ScheduleStatus.CANCELED,
                    scheduled_date=None,
                    scheduled_at=None,
                    timezone_name=None,
                    precision=None,
                )
            ]
        )
        self.assertEqual(
            project_release_state(
                schedule=canceled,
                valid_event_release_count=1,
                evaluation_time=datetime(2026, 9, 11, 13, tzinfo=timezone.utc),
            ),
            ReleaseProjectionState.CONFLICT,
        )

    def test_fingerprint_ignores_duplicate_provenance(self) -> None:
        codes = (
            "CPI_HEADLINE_MOM",
            "CPI_HEADLINE_YOY",
            "CPI_CORE_MOM",
            "CPI_CORE_YOY",
        )
        base = {code: resolve_observation_materials(code, []) for code in codes}
        row = value_evidence("CPI_CORE_MOM", "0.3")
        one, two = dict(base), dict(base)
        one["CPI_CORE_MOM"] = resolve_observation_materials("CPI_CORE_MOM", [row])
        two["CPI_CORE_MOM"] = resolve_observation_materials("CPI_CORE_MOM", [row, row])
        self.assertEqual(
            build_knowledge_fingerprint(
                event_occurrence_id=EVENT_ID,
                release_state=ReleaseProjectionState.DISCLOSED,
                observations=one,
            ),
            build_knowledge_fingerprint(
                event_occurrence_id=EVENT_ID,
                release_state=ReleaseProjectionState.DISCLOSED,
                observations=two,
            ),
        )

    def test_serving_deny_override_does_not_mutate_knowledge(self) -> None:
        codes = (
            "CPI_HEADLINE_MOM",
            "CPI_HEADLINE_YOY",
            "CPI_CORE_MOM",
            "CPI_CORE_YOY",
        )
        observations = {code: resolve_observation_materials(code, []) for code in codes}
        observations["CPI_CORE_MOM"] = resolve_observation_materials(
            "CPI_CORE_MOM",
            [value_evidence("CPI_CORE_MOM", "0.3")],
        )
        knowledge = CpiEventKnowledge(
            EVENT_ID,
            date(2026, 8, 1),
            KnowledgeMode.OFFICIAL_SOURCE_RECONSTRUCTION,
            ReleaseProjectionState.DISCLOSED,
            ScheduleSelection(ScheduleSelectionState.UNRESOLVED, None, ()),
            observations,
            build_knowledge_fingerprint(
                event_occurrence_id=EVENT_ID,
                release_state=ReleaseProjectionState.DISCLOSED,
                observations=observations,
            ),
            None,
        )
        governed = apply_control_states(
            knowledge,
            domain_state=ServingControlState.WITHHELD,
            event_state=ServingControlState.ENABLED,
        )
        self.assertIs(governed.knowledge, knowledge)
        self.assertEqual(governed.serving_state, ServingControlState.WITHHELD)
        self.assertEqual(governed.observations["CPI_CORE_MOM"].state, "WITHHELD")
        self.assertIsNone(governed.observations["CPI_CORE_MOM"].normalized_value)
        self.assertEqual(
            knowledge.observations["CPI_CORE_MOM"].normalized_value,
            Decimal("0.3"),
        )


if __name__ == "__main__":
    unittest.main()
