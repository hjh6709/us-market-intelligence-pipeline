import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from src.cpi_w1_contracts import (
    DisclosureArtifactRelationKind,
    DisclosureRelationKind,
    KnowledgeMode,
    ObservationMaterial,
    ObservationState,
    PromotionFamily,
    ReleaseProjectionState,
    RunMode,
    ScheduleMaterial,
    ScheduleStatus,
    ServingControlState,
    TimePrecision,
    WorkOutcome,
    canonical_material_json,
    material_fingerprint,
)


class CpiW1ContractsTest(unittest.TestCase):
    def test_enum_values_match_canonical_contract(self) -> None:
        self.assertEqual(
            {item.value for item in RunMode},
            {"LIVE", "BACKFILL", "REPLAY"},
        )
        self.assertIn("QUARANTINED", {item.value for item in WorkOutcome})
        self.assertEqual(
            {item.value for item in ScheduleStatus},
            {"SCHEDULED", "DATE_PENDING", "CANCELED"},
        )
        self.assertEqual(
            {item.value for item in TimePrecision},
            {"EXACT", "DATE_ONLY"},
        )
        self.assertEqual(
            {item.value for item in DisclosureRelationKind},
            {"EVENT_RELEASE", "SUPPLEMENTAL_DISCLOSURE"},
        )
        self.assertEqual(
            {item.value for item in DisclosureArtifactRelationKind},
            {
                "RELEASE_REPRESENTATION",
                "CORROBORATING_REPRESENTATION",
                "CORRECTION_NOTICE",
            },
        )
        self.assertEqual(
            {item.value for item in ObservationState},
            {"VALUE", "EXPLICIT_UNAVAILABLE"},
        )
        self.assertEqual(
            {item.value for item in KnowledgeMode},
            {"OFFICIAL_SOURCE_RECONSTRUCTION", "SYSTEM_KNOWN_PIT"},
        )
        self.assertIn(
            ReleaseProjectionState.DUE_DATE_UNTIMED,
            set(ReleaseProjectionState),
        )
        self.assertEqual(
            {item.value for item in ServingControlState},
            {"ENABLED", "WITHHELD"},
        )
        self.assertEqual(
            {item.value for item in PromotionFamily},
            {
                "CPI_RELEASE_ENVELOPE_PROMOTE",
                "CPI_OBSERVATION_BUNDLE_PROMOTE",
            },
        )

    def test_material_fingerprint_is_mapping_order_independent(self) -> None:
        self.assertEqual(
            material_fingerprint({"value": Decimal("2.9"), "unit": "PERCENT"}),
            material_fingerprint({"unit": "PERCENT", "value": Decimal("2.9")}),
        )

    def test_semantically_equal_decimals_converge(self) -> None:
        self.assertEqual(
            material_fingerprint({"value": Decimal("2.90")}),
            material_fingerprint({"value": Decimal("2.9")}),
        )
        self.assertEqual(
            material_fingerprint({"value": Decimal("-0.0")}),
            material_fingerprint({"value": Decimal("0")}),
        )

    def test_binary_float_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            material_fingerprint({"value": 2.9})

    def test_datetime_is_canonicalized_to_same_utc_instant(self) -> None:
        utc_value = datetime(2026, 9, 11, 12, 30, tzinfo=timezone.utc)
        eastern_offset = timezone(timedelta(hours=-4))
        local_value = datetime(2026, 9, 11, 8, 30, tzinfo=eastern_offset)
        self.assertEqual(
            material_fingerprint({"at": utc_value}),
            material_fingerprint({"at": local_value}),
        )

    def test_naive_datetime_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            canonical_material_json({"at": datetime(2026, 9, 11, 8, 30)})

    def test_schedule_material_excludes_provenance(self) -> None:
        material = ScheduleMaterial(
            schedule_status=ScheduleStatus.SCHEDULED,
            scheduled_date=date(2026, 9, 11),
            scheduled_at=datetime(
                2026, 9, 11, 12, 30, tzinfo=timezone.utc
            ),
            schedule_timezone="America/New_York",
            time_precision=TimePrecision.EXACT,
        )
        self.assertEqual(
            set(material.payload()),
            {
                "schedule_status",
                "scheduled_date",
                "scheduled_at",
                "schedule_timezone",
                "time_precision",
            },
        )
        for forbidden in (
            "source_artifact_id",
            "source_url",
            "extractor_contract_version",
            "accepted_at",
            "actor_subject",
        ):
            self.assertNotIn(forbidden, material.payload())

    def test_date_pending_does_not_synthesize_midnight(self) -> None:
        material = ScheduleMaterial(
            schedule_status=ScheduleStatus.DATE_PENDING,
            scheduled_date=None,
            scheduled_at=None,
            schedule_timezone=None,
            time_precision=None,
        )
        self.assertIsNone(material.payload()["scheduled_at"])

    def test_date_only_rejects_synthetic_instant(self) -> None:
        with self.assertRaises(ValueError):
            ScheduleMaterial(
                schedule_status=ScheduleStatus.SCHEDULED,
                scheduled_date=date(2026, 9, 11),
                scheduled_at=datetime(
                    2026, 9, 11, 0, 0, tzinfo=timezone.utc
                ),
                schedule_timezone="America/New_York",
                time_precision=TimePrecision.DATE_ONLY,
            )

    def test_observation_material_uses_decimal_and_excludes_provenance(self) -> None:
        material = ObservationMaterial(
            observation_code="CPI_HEADLINE_YOY",
            assertion_state=ObservationState.VALUE,
            normalized_value=Decimal("3.4"),
        )
        self.assertEqual(
            set(material.payload()),
            {"observation_code", "assertion_state", "normalized_value"},
        )
        self.assertEqual(
            material.fingerprint,
            ObservationMaterial(
                observation_code="CPI_HEADLINE_YOY",
                assertion_state=ObservationState.VALUE,
                normalized_value=Decimal("3.40"),
            ).fingerprint,
        )

    def test_observation_value_requires_decimal(self) -> None:
        with self.assertRaises(TypeError):
            ObservationMaterial(
                observation_code="CPI_HEADLINE_YOY",
                assertion_state=ObservationState.VALUE,
                normalized_value=2.9,  # type: ignore[arg-type]
            )

    def test_explicit_unavailable_has_no_numeric_value(self) -> None:
        material = ObservationMaterial(
            observation_code="CPI_CORE_MOM",
            assertion_state=ObservationState.EXPLICIT_UNAVAILABLE,
            normalized_value=None,
        )
        self.assertIsNone(material.normalized_value)
        with self.assertRaises(ValueError):
            ObservationMaterial(
                observation_code="CPI_CORE_MOM",
                assertion_state=ObservationState.EXPLICIT_UNAVAILABLE,
                normalized_value=Decimal("0"),
            )


if __name__ == "__main__":
    unittest.main()
