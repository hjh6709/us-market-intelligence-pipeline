import hashlib
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from src.cpi_w1_contracts import ObservationState, TimePrecision
from src.cpi_w1_release import (
    CorrectionObservationBundleCandidate,
    ExpectedReleaseNotVisible,
    ObservationExtractionError,
    extract_core4_from_release_html,
    extract_release_envelope,
)


FIXTURES = Path("tests/fixtures/cpi_w1/html")


class CpiW1ReleaseHtmlTest(unittest.TestCase):
    def read(self, name: str) -> bytes:
        return (FIXTURES / name).read_bytes()

    def test_envelope_recognizes_program_reference_month_marker_and_table1(self) -> None:
        envelope = extract_release_envelope(
            self.read("normal_aug_2026.html"),
            expected_reference_month=date(2026, 8, 1),
        )
        self.assertEqual(envelope.event_type, "CPI")
        self.assertEqual(envelope.reference_month, date(2026, 8, 1))
        self.assertEqual(envelope.marker_semantics, "EMBARGO_LIFT")
        self.assertEqual(envelope.marker_date, date(2026, 9, 11))
        self.assertEqual(
            envelope.marker_at,
            datetime(2026, 9, 11, 8, 30, tzinfo=ZoneInfo("America/New_York")),
        )
        self.assertEqual(envelope.time_precision, TimePrecision.EXACT)

    def test_candidates_are_bound_to_exact_artifact_bytes(self) -> None:
        body = self.read("normal_aug_2026.html")
        expected = hashlib.sha256(body).hexdigest()
        envelope = extract_release_envelope(
            body,
            expected_reference_month=date(2026, 8, 1),
        )
        bundle = extract_core4_from_release_html(
            body,
            expected_reference_month=date(2026, 8, 1),
        )
        self.assertEqual(envelope.artifact_content_sha256, expected)
        self.assertEqual(bundle.artifact_content_sha256, expected)

    def test_correction_bundle_requires_unique_nonempty_core4_subset(self) -> None:
        bundle = extract_core4_from_release_html(
            self.read("normal_aug_2026.html"),
            expected_reference_month=date(2026, 8, 1),
        )
        one = (bundle.observations[0],)
        candidate = CorrectionObservationBundleCandidate(
            artifact_content_sha256="a" * 64,
            reference_month=date(2026, 8, 1),
            observations=one,
            extractor_contract_version="bls-cpi-correction-html-v1",
        )
        self.assertEqual(len(candidate.observations), 1)
        with self.assertRaises(ValueError):
            CorrectionObservationBundleCandidate(
                artifact_content_sha256="a" * 64,
                reference_month=date(2026, 8, 1),
                observations=(),
                extractor_contract_version="bls-cpi-correction-html-v1",
            )
        with self.assertRaises(ValueError):
            CorrectionObservationBundleCandidate(
                artifact_content_sha256="a" * 64,
                reference_month=date(2026, 8, 1),
                observations=(one[0], one[0]),
                extractor_contract_version="bls-cpi-correction-html-v1",
            )

    def test_stale_current_release_is_typed_not_visible(self) -> None:
        with self.assertRaises(ExpectedReleaseNotVisible):
            extract_release_envelope(
                self.read("wrong_reference_month.html"),
                expected_reference_month=date(2026, 8, 1),
            )

    def test_envelope_is_independent_from_core4_leaf_mapping(self) -> None:
        envelope = extract_release_envelope(
            self.read("unknown_leaf_heading.html"),
            expected_reference_month=date(2026, 8, 1),
        )
        self.assertEqual(envelope.reference_month, date(2026, 8, 1))
        with self.assertRaises(ObservationExtractionError):
            extract_core4_from_release_html(
                self.read("unknown_leaf_heading.html"),
                expected_reference_month=date(2026, 8, 1),
            )

    def test_core4_maps_semantic_headers_and_rows(self) -> None:
        bundle = extract_core4_from_release_html(
            self.read("normal_aug_2026.html"),
            expected_reference_month=date(2026, 8, 1),
        )
        values = bundle.by_code()
        self.assertEqual(values["CPI_HEADLINE_MOM"].normalized_value, Decimal("0.4"))
        self.assertEqual(values["CPI_HEADLINE_YOY"].normalized_value, Decimal("3.4"))
        self.assertEqual(values["CPI_CORE_MOM"].normalized_value, Decimal("0.3"))
        self.assertEqual(values["CPI_CORE_YOY"].normalized_value, Decimal("2.4"))

    def test_negative_and_zero_values_are_real_values(self) -> None:
        body = self.read("normal_aug_2026.html")
        body = body.replace(b">0.4<", b">-0.4<", 1)
        body = body.replace(b">0.3</td></tr>", b">0.0</td></tr>", 1)
        bundle = extract_core4_from_release_html(
            body,
            expected_reference_month=date(2026, 8, 1),
        )
        values = bundle.by_code()
        self.assertEqual(values["CPI_HEADLINE_MOM"].normalized_value, Decimal("-0.4"))
        self.assertIn(
            Decimal("0.0"),
            {item.normalized_value for item in bundle.observations},
        )

    def test_november_2025_missing_prior_month_supports_explicit_unavailable(self) -> None:
        bundle = extract_core4_from_release_html(
            self.read("nov_2025_explicit_unavailable.html"),
            expected_reference_month=date(2025, 11, 1),
        )
        values = bundle.by_code()
        self.assertEqual(
            values["CPI_HEADLINE_MOM"].assertion_state,
            ObservationState.EXPLICIT_UNAVAILABLE,
        )
        self.assertEqual(
            values["CPI_CORE_MOM"].assertion_state,
            ObservationState.EXPLICIT_UNAVAILABLE,
        )
        self.assertEqual(values["CPI_HEADLINE_YOY"].normalized_value, Decimal("2.7"))
        self.assertEqual(values["CPI_CORE_YOY"].normalized_value, Decimal("2.6"))
        self.assertIsNotNone(values["CPI_HEADLINE_MOM"].source_reason_text)
        self.assertEqual(values["CPI_HEADLINE_MOM"].source_value_text, "-")

    def test_dash_without_official_unavailability_context_quarantines(self) -> None:
        body = self.read("nov_2025_explicit_unavailable.html").replace(
            b"BLS did not collect survey data for October 2025 due to a lapse in appropriations.\nBLS was unable to retroactively collect these data.",
            b"No exceptional source context.",
        )
        with self.assertRaises(ObservationExtractionError):
            extract_core4_from_release_html(
                body,
                expected_reference_month=date(2025, 11, 1),
            )

    def test_duplicate_core_row_is_ambiguous(self) -> None:
        body = self.read("normal_aug_2026.html").replace(
            b"</table>",
            b"<tr><th>All items</th><td>100</td><td>1</td><td>1</td><td>1</td><td>9.9</td><td>1</td><td>1</td><td>1</td><td>9.9</td></tr></table>",
        )
        with self.assertRaises(ObservationExtractionError):
            extract_core4_from_release_html(
                body,
                expected_reference_month=date(2026, 8, 1),
            )

    def test_resource_amplification_via_colspan_is_rejected(self) -> None:
        body = self.read("normal_aug_2026.html").replace(
            b'colspan="3"',
            b'colspan="101"',
            1,
        )
        from src.cpi_w1_release import ReleaseEnvelopeError

        with self.assertRaises(ReleaseEnvelopeError):
            extract_release_envelope(
                body,
                expected_reference_month=date(2026, 8, 1),
            )

    def test_unrelated_global_words_do_not_create_explicit_unavailable(self) -> None:
        body = self.read("nov_2025_explicit_unavailable.html")
        body = body.replace(
            b"BLS did not collect survey data for October 2025 due to a lapse in appropriations.\nBLS was unable to retroactively collect these data.",
            b"October 2025 is mentioned here. Elsewhere BLS discusses data values are not available. A lapse in appropriations is also mentioned.",
        )
        with self.assertRaises(ObservationExtractionError):
            extract_core4_from_release_html(
                body,
                expected_reference_month=date(2025, 11, 1),
            )

    def test_binary_float_never_enters_observation_candidate(self) -> None:
        bundle = extract_core4_from_release_html(
            self.read("normal_aug_2026.html"),
            expected_reference_month=date(2026, 8, 1),
        )
        self.assertTrue(
            all(
                item.normalized_value is None
                or isinstance(item.normalized_value, Decimal)
                for item in bundle.observations
            )
        )


if __name__ == "__main__":
    unittest.main()
