import json
import tempfile
import unittest
from pathlib import Path

from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry
from src.cpi_w1_release_gate import CpiW1CapabilityReleaseGate


ROOT = Path(__file__).resolve().parents[1]
GATE = ROOT / "config/cpi_w1_extractor_release_gate.json"
REGISTRY = ROOT / "config/cpi_w1_promotion_capabilities.json"
EVIDENCE = ROOT / "config/cpi_w1_evidence_snapshots.json"


class CpiW1ReleaseGateTest(unittest.TestCase):
    def gate(self) -> CpiW1CapabilityReleaseGate:
        return CpiW1CapabilityReleaseGate.from_json(
            GATE,
            capability_registry_path=REGISTRY,
            evidence_snapshot_path=EVIDENCE,
        )

    def test_gate_has_semantic_snapshot_digest(self) -> None:
        gate = self.gate()
        self.assertRegex(gate.gate_snapshot_digest, r"^[0-9a-f]{64}$")

    def test_release_html_capabilities_have_independent_fail_closed_decisions(self) -> None:
        gate = self.gate()
        registry = PromotionCapabilityRegistry.from_json(REGISTRY)
        envelope_subject = registry.require(
            "BLS_CPI_RELEASE_ENVELOPE_HTML"
        ).release_subject.release_subject_digest
        core4_subject = registry.require(
            "BLS_CPI_CORE4_HTML"
        ).release_subject.release_subject_digest
        envelope = gate.decision(
            "BLS_CPI_RELEASE_ENVELOPE_HTML", envelope_subject
        )
        core4 = gate.decision("BLS_CPI_CORE4_HTML", core4_subject)
        self.assertFalse(envelope.eligible)
        self.assertFalse(core4.eligible)
        self.assertNotEqual(envelope.gate_decision_digest, core4.gate_decision_digest)
        self.assertNotEqual(envelope.evidence_snapshot_digest, core4.evidence_snapshot_digest)

    def test_every_active_capability_has_explicit_reviewed_decision(self) -> None:
        gate = self.gate()
        registry = PromotionCapabilityRegistry.from_json(REGISTRY)
        for capability in registry.active():
            decision = gate.decision(
                capability.promotion_capability_id,
                capability.release_subject.release_subject_digest,
            )
            self.assertEqual(decision.decision, "BLOCKED")
            self.assertIsNotNone(decision.reason_code)
            self.assertIsNotNone(decision.review_ref)
            self.assertRegex(decision.review_digest or "", r"^[0-9a-f]{64}$")

    def test_subject_mismatch_never_inherits_capability_decision(self) -> None:
        decision = self.gate().decision("BLS_CPI_CORE4_HTML", "0" * 64)
        self.assertFalse(decision.eligible)
        self.assertEqual(decision.reason_code, "RELEASE_SUBJECT_NOT_REVIEWED")
        self.assertIsNone(decision.gate_decision_digest)

    def test_gate_order_does_not_change_semantic_digest(self) -> None:
        raw = json.loads(GATE.read_text(encoding="utf-8"))
        raw["decisions"].reverse()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gate.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            reordered = CpiW1CapabilityReleaseGate.from_json(
                path,
                capability_registry_path=REGISTRY,
                evidence_snapshot_path=EVIDENCE,
            )
        self.assertEqual(reordered.gate_snapshot_digest, self.gate().gate_snapshot_digest)


if __name__ == "__main__":
    unittest.main()
