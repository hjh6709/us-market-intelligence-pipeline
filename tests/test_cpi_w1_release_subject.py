import unittest

from src.cpi_w1_release_subject import ReleaseSubjectV1


VALID = {
    "schema": "cpi-w1-release-subject-v1",
    "source_code": "BLS",
    "artifact_contract_kind": "CPI_RELEASE_HTML",
    "source_contract_version": "bls-cpi-source-v1",
    "promotion_capability_id": "CPI_RELEASE_ENVELOPE",
    "extractor_contract_version": "bls-cpi-release-html-v1",
    "promotion_family": "CPI_RELEASE_ENVELOPE_PROMOTE",
}


class ReleaseSubjectV1Test(unittest.TestCase):
    def test_golden_canonical_json_and_digest(self) -> None:
        subject = ReleaseSubjectV1.from_mapping(VALID)
        self.assertEqual(
            subject.canonical_json,
            '{"artifact_contract_kind":"CPI_RELEASE_HTML",'
            '"extractor_contract_version":"bls-cpi-release-html-v1",'
            '"promotion_capability_id":"CPI_RELEASE_ENVELOPE",'
            '"promotion_family":"CPI_RELEASE_ENVELOPE_PROMOTE",'
            '"schema":"cpi-w1-release-subject-v1",'
            '"source_code":"BLS",'
            '"source_contract_version":"bls-cpi-source-v1"}',
        )
        self.assertEqual(
            subject.release_subject_digest,
            "9de99a4f1a4144d58105818aed05e85f05510add1bf6d7417261ee6229ca6a2b",
        )

    def test_mapping_order_does_not_change_identity(self) -> None:
        reversed_items = dict(reversed(tuple(VALID.items())))
        self.assertEqual(
            ReleaseSubjectV1.from_mapping(VALID).release_subject_digest,
            ReleaseSubjectV1.from_mapping(reversed_items).release_subject_digest,
        )

    def test_one_semantic_value_changes_identity(self) -> None:
        changed = dict(VALID, promotion_capability_id="CPI_OBSERVATION_BUNDLE")
        self.assertNotEqual(
            ReleaseSubjectV1.from_mapping(VALID).release_subject_digest,
            ReleaseSubjectV1.from_mapping(changed).release_subject_digest,
        )

    def test_rejects_extra_missing_and_null_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "unexpected keys"):
            ReleaseSubjectV1.from_mapping(dict(VALID, reference_month="2026-08"))
        missing = dict(VALID)
        missing.pop("promotion_family")
        with self.assertRaisesRegex(ValueError, "missing keys"):
            ReleaseSubjectV1.from_mapping(missing)
        with self.assertRaisesRegex(ValueError, "must be a string"):
            ReleaseSubjectV1.from_mapping(dict(VALID, source_code=None))

    def test_rejects_whitespace_and_case_typos_without_normalizing(self) -> None:
        for key, value in (
            ("source_code", " BLS"),
            ("artifact_contract_kind", "CPI_RELEASE_HTML "),
            ("extractor_contract_version", "BLS-cpi-release-html-v1"),
            ("promotion_family", "cpi_release_envelope_promote"),
        ):
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    ReleaseSubjectV1.from_mapping(dict(VALID, **{key: value}))

    def test_schema_marker_is_exact(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema"):
            ReleaseSubjectV1.from_mapping(dict(VALID, schema="cpi-w1-release-subject-v2"))


if __name__ == "__main__":
    unittest.main()
