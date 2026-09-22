import unittest
from pathlib import Path

from src.cpi_w1_release_gate import CpiW1ExtractorReleaseGate


ROOT = Path(__file__).resolve().parents[1]
GATE = ROOT / "config/cpi_w1_extractor_release_gate.json"


class CpiW1ReleaseGateTest(unittest.TestCase):
    def test_current_release_parser_is_fail_closed_until_official_corpus_ready(self) -> None:
        gate = CpiW1ExtractorReleaseGate.from_json(GATE)
        decision = gate.decision("bls-cpi-release-html-v1")
        self.assertFalse(decision.eligible)
        self.assertEqual(decision.reason_code, "OFFICIAL_CORPUS_NOT_READY")

    def test_unknown_extractor_is_not_implicitly_approved(self) -> None:
        gate = CpiW1ExtractorReleaseGate.from_json(GATE)
        decision = gate.decision("future-extractor-v99")
        self.assertFalse(decision.eligible)
        self.assertEqual(decision.reason_code, "EXTRACTOR_NOT_REVIEWED")

    def test_schedule_extractors_are_explicitly_fail_closed(self) -> None:
        gate = CpiW1ExtractorReleaseGate.from_json(GATE)
        for version in (
            "bls-cpi-schedule-html-v1",
            "bls-cpi-global-ics-v1",
            "bls-cpi-revised-release-dates-v1",
        ):
            self.assertFalse(gate.decision(version).eligible)

    def test_xlsx_and_correction_paths_are_explicitly_blocked(self) -> None:
        gate = CpiW1ExtractorReleaseGate.from_json(GATE)
        self.assertFalse(gate.decision("bls-cpi-table1-xlsx-v1").eligible)
        self.assertFalse(gate.decision("bls-cpi-correction-html-v1").eligible)


if __name__ == "__main__":
    unittest.main()
