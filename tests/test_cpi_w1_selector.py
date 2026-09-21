import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from src.cpi_w1_contracts import (
    ObservationState,
    ReleaseProjectionState,
    ScheduleMaterial,
    ScheduleStatus,
    SourceAuthorityRole,
    TimePrecision,
)
from src.cpi_w1_selector import (
    ObservationResolution,
    ObservationResolutionState,
    _ScheduleEvidence,
    _ScheduleSelection,
    _knowledge_fingerprint,
    _project_release,
    _resolution_from_materials,
    _select_schedule,
)


UTC = timezone.utc
EVENT_ID = UUID("00000000-0000-0000-0000-000000000111")


class CpiW1SelectorTest(unittest.TestCase):
    def test_observation_resolution_vectors(self) -> None:
        unresolved = _resolution_from_materials("CPI_HEADLINE_MOM", [])
        self.assertEqual(unresolved.state, ObservationResolutionState.UNRESOLVED)

        value = _resolution_from_materials(
            "CPI_HEADLINE_MOM",
            [("a" * 64, ObservationState.VALUE, Decimal("0.3"))],
        )
        self.assertEqual(value.state, ObservationResolutionState.VALUE)
        self.assertEqual(value.normalized_value, Decimal("0.3"))

        unavailable = _resolution_from_materials(
            "CPI_HEADLINE_MOM",
            [("b" * 64, ObservationState.EXPLICIT_UNAVAILABLE, None)],
        )
        self.assertEqual(
            unavailable.state,
            ObservationResolutionState.EXPLICIT_UNAVAILABLE,
        )

        conflict = _resolution_from_materials(
            "CPI_HEADLINE_MOM",
            [
                ("a" * 64, ObservationState.VALUE, Decimal("0.3")),
                ("c" * 64, ObservationState.VALUE, Decimal("0.4")),
            ],
        )
        self.assertEqual(conflict.state, ObservationResolutionState.CONFLICT)
        self.assertIsNone(conflict.normalized_value)

    def test_same_material_from_multiple_evidence_rows_converges(self) -> None:
        resolved = _resolution_from_materials(
            "CPI_CORE_MOM",
            [
                ("d" * 64, ObservationState.VALUE, Decimal("0.2")),
                ("d" * 64, ObservationState.VALUE, Decimal("0.2")),
            ],
        )
        self.assertEqual(resolved.state, ObservationResolutionState.VALUE)
        self.assertEqual(resolved.eligible_evidence_count, 2)
        self.assertEqual(resolved.material_fingerprints, ("d" * 64,))

    def test_exact_schedule_release_projection(self) -> None:
        material = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 11),
            scheduled_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        selected = _ScheduleSelection(
            "RESOLVED",
            material,
            (material.fingerprint,),
            False,
        )
        self.assertEqual(
            _project_release(
                selected,
                has_valid_release=False,
                evaluation_at=datetime(2026, 9, 11, 12, 29, tzinfo=UTC),
            ),
            ReleaseProjectionState.NOT_YET_DUE,
        )
        self.assertEqual(
            _project_release(
                selected,
                has_valid_release=False,
                evaluation_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
            ),
            ReleaseProjectionState.AWAITING_CONFIRMATION,
        )

    def test_date_only_schedule_has_due_date_untimed_state(self) -> None:
        material = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 11),
            scheduled_at=None,
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.DATE_ONLY,
        )
        selected = _ScheduleSelection(
            "RESOLVED",
            material,
            (material.fingerprint,),
            False,
        )
        self.assertEqual(
            _project_release(
                selected,
                has_valid_release=False,
                evaluation_at=datetime(2026, 9, 11, 16, 0, tzinfo=UTC),
            ),
            ReleaseProjectionState.DUE_DATE_UNTIMED,
        )

    def test_canceled_schedule_and_release_is_conflict(self) -> None:
        material = ScheduleMaterial(
            schedule_status=ScheduleStatus.CANCELED,
            scheduled_date=None,
            scheduled_at=None,
            schedule_timezone=None,
            time_precision=None,
        )
        selected = _ScheduleSelection(
            "RESOLVED",
            material,
            (material.fingerprint,),
            True,
        )
        self.assertEqual(
            _project_release(
                selected,
                has_valid_release=False,
                evaluation_at=datetime(2026, 9, 11, tzinfo=UTC),
            ),
            ReleaseProjectionState.NO_RELEASE_EXPECTED,
        )
        self.assertEqual(
            _project_release(
                selected,
                has_valid_release=True,
                evaluation_at=datetime(2026, 9, 11, tzinfo=UTC),
            ),
            ReleaseProjectionState.CONFLICT,
        )

    def test_authoritative_schedule_excludes_fallback_disagreement(self) -> None:
        authoritative = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 11),
            scheduled_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        fallback = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 12),
            scheduled_at=datetime(2026, 9, 12, 12, 30, tzinfo=UTC),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        rows = [
            _ScheduleEvidence(
                assertion_id=UUID(int=1),
                material=authoritative,
                material_fingerprint=authoritative.fingerprint,
                source_contract_version="bls-cpi-source-v1",
                artifact_contract_kind="CPI_SCHEDULE_HTML",
                source_effective_date=None,
                source_effective_at=None,
                source_effective_precision=None,
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                accepted_at=datetime(2026, 1, 1, tzinfo=UTC),
                capture_chronology_allowed=False,
                authority_role=SourceAuthorityRole.AUTHORITATIVE,
            ),
            _ScheduleEvidence(
                assertion_id=UUID(int=2),
                material=fallback,
                material_fingerprint=fallback.fingerprint,
                source_contract_version="bls-cpi-source-v1",
                artifact_contract_kind="BLS_GLOBAL_ICS",
                source_effective_date=None,
                source_effective_at=None,
                source_effective_precision=None,
                captured_at=datetime(2026, 1, 2, tzinfo=UTC),
                accepted_at=datetime(2026, 1, 2, tzinfo=UTC),
                capture_chronology_allowed=False,
                authority_role=SourceAuthorityRole.FALLBACK_CORROBORATION,
            ),
        ]
        selected = _select_schedule(rows)
        self.assertEqual(selected.kind, "RESOLVED")
        self.assertEqual(selected.material, authoritative)

    def test_ambiguous_authoritative_materials_conflict_without_safe_chronology(self) -> None:
        first = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 11),
            scheduled_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        second = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 12),
            scheduled_at=datetime(2026, 9, 12, 12, 30, tzinfo=UTC),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        rows = []
        for idx, material in enumerate((first, second), 1):
            rows.append(
                _ScheduleEvidence(
                    assertion_id=UUID(int=idx),
                    material=material,
                    material_fingerprint=material.fingerprint,
                    source_contract_version="bls-cpi-source-v1",
                    artifact_contract_kind="CPI_SCHEDULE_HTML",
                    source_effective_date=None,
                    source_effective_at=None,
                    source_effective_precision=None,
                    captured_at=datetime(2026, 1, idx, tzinfo=UTC),
                    accepted_at=datetime(2026, 1, idx, tzinfo=UTC),
                    capture_chronology_allowed=False,
                    authority_role=SourceAuthorityRole.AUTHORITATIVE,
                )
            )
        self.assertEqual(_select_schedule(rows).kind, "CONFLICT")

    def test_knowledge_fingerprint_ignores_duplicate_provenance_and_order(self) -> None:
        a = ObservationResolution(
            observation_code="CPI_HEADLINE_MOM",
            state=ObservationResolutionState.VALUE,
            normalized_value=Decimal("0.3"),
            material_fingerprints=("f" * 64,),
            eligible_evidence_count=1,
        )
        b = ObservationResolution(
            observation_code="CPI_CORE_MOM",
            state=ObservationResolutionState.UNRESOLVED,
            normalized_value=None,
            material_fingerprints=(),
            eligible_evidence_count=0,
        )
        first = _knowledge_fingerprint(
            EVENT_ID,
            ReleaseProjectionState.DISCLOSED,
            (a, b),
        )
        duplicate_provenance = ObservationResolution(
            observation_code=a.observation_code,
            state=a.state,
            normalized_value=a.normalized_value,
            material_fingerprints=a.material_fingerprints,
            eligible_evidence_count=99,
        )
        second = _knowledge_fingerprint(
            EVENT_ID,
            ReleaseProjectionState.DISCLOSED,
            (b, duplicate_provenance),
        )
        self.assertEqual(first, second)

    def test_selector_requires_owned_repeatable_read_snapshot(self) -> None:
        source = Path("src/cpi_w1_selector.py").read_text(encoding="utf-8")
        self.assertIn(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            source,
        )
        self.assertIn("TransactionStatus.IDLE", source)
        self.assertIn("with _consistent_read_transaction(connection):", source)

    def test_live_governed_selector_has_atomic_entrypoint(self) -> None:
        import inspect
        from src.cpi_w1_selector import CpiW1Selector

        parameters = inspect.signature(
            CpiW1Selector.select_governed_event
        ).parameters
        self.assertEqual(tuple(parameters), ("self", "connection", "event_id"))

    def test_live_serving_overlay_has_no_caller_selected_decision_time(self) -> None:
        import inspect
        from src.cpi_w1_selector import CpiW1Selector

        parameters = inspect.signature(
            CpiW1Selector.apply_serving_overlay
        ).parameters
        self.assertNotIn("decision_time", parameters)

    def test_live_overlay_revalidates_knowledge_fingerprint(self) -> None:
        source = Path("src/cpi_w1_selector.py").read_text(encoding="utf-8")
        self.assertIn("knowledge changed before live serving overlay", source)
        self.assertIn("current.knowledge_fingerprint", source)

    def test_knowledge_fingerprint_changes_when_semantic_state_changes(self) -> None:
        base = ObservationResolution(
            observation_code="CPI_HEADLINE_MOM",
            state=ObservationResolutionState.VALUE,
            normalized_value=Decimal("0.3"),
            material_fingerprints=("a" * 64,),
            eligible_evidence_count=1,
        )
        conflict = ObservationResolution(
            observation_code="CPI_HEADLINE_MOM",
            state=ObservationResolutionState.CONFLICT,
            normalized_value=None,
            material_fingerprints=("a" * 64, "b" * 64),
            eligible_evidence_count=2,
        )
        first = _knowledge_fingerprint(
            EVENT_ID,
            ReleaseProjectionState.DISCLOSED,
            (base,),
        )
        second = _knowledge_fingerprint(
            EVENT_ID,
            ReleaseProjectionState.DISCLOSED,
            (conflict,),
        )
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
