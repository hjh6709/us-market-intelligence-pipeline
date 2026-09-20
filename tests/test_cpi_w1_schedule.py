import unittest
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.cpi_w1_contracts import (
    ScheduleStatus,
    SourceAuthorityRole,
    TimePrecision,
)
from src.cpi_w1_schedule import (
    ScheduleParseError,
    parse_cpi_schedule_html,
    parse_cpi_schedule_ics,
)


FIXTURES = Path("tests/fixtures/cpi_w1/schedule")
REFERENCE_MONTH = date(2026, 8, 1)


class CpiW1ScheduleTest(unittest.TestCase):
    def read(self, name: str) -> bytes:
        return (FIXTURES / name).read_bytes()

    def test_exact_schedule_uses_semantic_headers_and_eastern_zone(self) -> None:
        candidate = parse_cpi_schedule_html(
            self.read("exact.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(candidate.reference_month, REFERENCE_MONTH)
        self.assertEqual(candidate.scheduled_date, date(2026, 9, 11))
        self.assertEqual(candidate.schedule_timezone, "America/New_York")
        self.assertEqual(candidate.time_precision, TimePrecision.EXACT)
        self.assertEqual(
            candidate.scheduled_at,
            datetime(2026, 9, 11, 8, 30, tzinfo=ZoneInfo("America/New_York")),
        )
        self.assertEqual(candidate.source_role, SourceAuthorityRole.AUTHORITATIVE)

    def test_column_reordering_does_not_change_semantics(self) -> None:
        exact = parse_cpi_schedule_html(
            self.read("exact.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        reordered = parse_cpi_schedule_html(
            self.read("reordered.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(exact.material_fingerprint, reordered.material_fingerprint)

    def test_date_only_does_not_synthesize_midnight(self) -> None:
        candidate = parse_cpi_schedule_html(
            self.read("date_only.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(candidate.schedule_status, ScheduleStatus.SCHEDULED)
        self.assertEqual(candidate.time_precision, TimePrecision.DATE_ONLY)
        self.assertEqual(candidate.scheduled_date, date(2026, 9, 11))
        self.assertIsNone(candidate.scheduled_at)

    def test_explicit_pending_can_create_date_pending(self) -> None:
        candidate = parse_cpi_schedule_html(
            self.read("pending.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(candidate.schedule_status, ScheduleStatus.DATE_PENDING)
        self.assertIsNone(candidate.scheduled_date)
        self.assertIsNone(candidate.scheduled_at)

    def test_explicit_cancellation_is_required(self) -> None:
        candidate = parse_cpi_schedule_html(
            self.read("canceled.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(candidate.schedule_status, ScheduleStatus.CANCELED)
        self.assertIsNone(candidate.scheduled_at)

    def test_missing_row_is_unresolved_not_date_pending_or_canceled(self) -> None:
        with self.assertRaises(ScheduleParseError):
            parse_cpi_schedule_html(
                self.read("absent.html"),
                expected_reference_month=REFERENCE_MONTH,
                contract_version="bls-cpi-source-v1",
            )

    def test_stale_ics_is_fallback_not_authoritative(self) -> None:
        html = parse_cpi_schedule_html(
            self.read("exact.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        ics = parse_cpi_schedule_ics(
            self.read("stale.ics"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(ics.source_role, SourceAuthorityRole.FALLBACK_CORROBORATION)
        self.assertEqual(ics.scheduled_date, date(2026, 9, 10))
        self.assertNotEqual(html.material_fingerprint, ics.material_fingerprint)

    def test_source_effective_metadata_is_not_schedule_material(self) -> None:
        candidate = parse_cpi_schedule_html(
            self.read("exact.html"),
            expected_reference_month=REFERENCE_MONTH,
            contract_version="bls-cpi-source-v1",
        )
        from dataclasses import replace

        later = replace(candidate, source_effective_date=date(2026, 9, 1))
        self.assertEqual(candidate.material_fingerprint, later.material_fingerprint)

    def test_ambiguous_duplicate_rows_are_rejected(self) -> None:
        body = b"""<table>
        <tr><th>Reference Month</th><th>Release Date</th><th>Release Time</th></tr>
        <tr><td>August 2026</td><td>Sep. 11, 2026</td><td>08:30 AM</td></tr>
        <tr><td>August 2026</td><td>Sep. 12, 2026</td><td>08:30 AM</td></tr>
        </table>"""
        with self.assertRaises(ScheduleParseError):
            parse_cpi_schedule_html(
                body,
                expected_reference_month=REFERENCE_MONTH,
                contract_version="bls-cpi-source-v1",
            )


if __name__ == "__main__":
    unittest.main()
