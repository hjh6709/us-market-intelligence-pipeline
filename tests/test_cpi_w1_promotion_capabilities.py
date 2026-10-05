import json
import tempfile
import unittest
from pathlib import Path

from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


REGISTRY = Path("config/cpi_w1_promotion_capabilities.json")


class PromotionCapabilityRegistryTest(unittest.TestCase):
    def test_checked_in_registry_separates_independent_release_html_capabilities(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(REGISTRY)
        envelope = registry.require("BLS_CPI_RELEASE_ENVELOPE_HTML")
        core4 = registry.require("BLS_CPI_CORE4_HTML")
        self.assertEqual(envelope.extractor_contract_version, "bls-cpi-release-envelope-html-v1")
        self.assertEqual(envelope.promotion_family, "CPI_RELEASE_ENVELOPE_PROMOTE")
        self.assertEqual(core4.extractor_contract_version, "bls-cpi-core4-html-v1")
        self.assertEqual(core4.promotion_family, "CPI_OBSERVATION_BUNDLE_PROMOTE")
        self.assertNotEqual(
            envelope.release_subject.release_subject_digest,
            core4.release_subject.release_subject_digest,
        )

    def test_registry_covers_each_current_artifact_family_pair(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(REGISTRY)
        pairs = {
            (item.artifact_contract_kind, item.promotion_family)
            for item in registry.capabilities
        }
        self.assertEqual(
            pairs,
            {
                ("CPI_SCHEDULE_HTML", "CPI_SCHEDULE_ASSERTION_PROMOTE"),
                ("BLS_GLOBAL_ICS", "CPI_SCHEDULE_ASSERTION_PROMOTE"),
                ("BLS_REVISED_RELEASE_DATES_HTML", "CPI_SCHEDULE_ASSERTION_PROMOTE"),
                ("CPI_RELEASE_HTML", "CPI_RELEASE_ENVELOPE_PROMOTE"),
                ("CPI_RELEASE_HTML", "CPI_OBSERVATION_BUNDLE_PROMOTE"),
                ("CPI_TABLE1_XLSX", "CPI_CORROBORATING_REPRESENTATION_PROMOTE"),
                ("CPI_TABLE1_XLSX", "CPI_OBSERVATION_BUNDLE_PROMOTE"),
                ("CPI_CORRECTION_HTML", "CPI_CORRECTION_NOTICE_PROMOTE"),
                ("CPI_CORRECTION_HTML", "CPI_CORRECTION_OBSERVATION_PROMOTE"),
            },
        )

    def test_lifecycle_controls_scheduling_not_runtime_authority(self) -> None:
        raw = json.loads(REGISTRY.read_text(encoding="utf-8"))
        self.assertNotIn("enabled", REGISTRY.read_text(encoding="utf-8"))
        raw["capabilities"][0]["lifecycle"] = "RETIRED"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            registry = PromotionCapabilityRegistry.from_json(path)
        retired_id = raw["capabilities"][0]["promotion_capability_id"]
        self.assertEqual(registry.require(retired_id).lifecycle, "RETIRED")
        self.assertNotIn(retired_id, {item.promotion_capability_id for item in registry.active()})

    def test_active_capabilities_are_resolved_by_exact_artifact_contract(self) -> None:
        registry = PromotionCapabilityRegistry.from_json(REGISTRY)
        capabilities = registry.active_for_artifact(
            source_code="BLS",
            artifact_contract_kind="CPI_RELEASE_HTML",
            source_contract_version="bls-cpi-source-v1",
        )
        self.assertEqual(
            tuple(item.promotion_capability_id for item in capabilities),
            ("BLS_CPI_RELEASE_ENVELOPE_HTML", "BLS_CPI_CORE4_HTML"),
        )

    def test_rejects_runtime_enabled_flag_and_duplicate_capability_id(self) -> None:
        raw = json.loads(REGISTRY.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            raw["capabilities"][0]["enabled"] = True
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unexpected keys"):
                PromotionCapabilityRegistry.from_json(path)

            raw["capabilities"][0].pop("enabled")
            raw["capabilities"].append(dict(raw["capabilities"][0]))
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate promotion capability"):
                PromotionCapabilityRegistry.from_json(path)


if __name__ == "__main__":
    unittest.main()
