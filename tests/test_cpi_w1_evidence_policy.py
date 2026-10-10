import copy
import importlib
import importlib.util
import json
from pathlib import Path
import unittest

from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


class CapabilityEvidencePolicyTest(unittest.TestCase):
    def api(self):
        self.assertIsNotNone(importlib.util.find_spec("src.cpi_w1_evidence_policy"), "missing R1 policy implementation")
        return importlib.import_module("src.cpi_w1_evidence_policy")

    def registry(self):
        return PromotionCapabilityRegistry.from_json("config/cpi_w1_promotion_capabilities.json")

    def policies(self):
        return self.api().CapabilityEvidencePolicyRegistry.from_json(
            Path("config/cpi_w1_capability_evidence_policies.json"), self.registry()
        )

    def test_every_active_capability_has_verified_policy_and_no_zero_official_allowance(self):
        policies = self.policies()
        self.assertEqual(len(policies.policies), 9)
        for capability in self.registry().active():
            policy = policies.require(capability.promotion_capability_id)
            self.assertEqual(policy.release_subject_digest, capability.release_subject.release_subject_digest)
            self.assertFalse(policy.zero_official_allowed)
        self.assertEqual(sum(not p.complete for p in policies.policies), 6)

    def test_missing_active_policy_rejected_by_complete_registry_validation(self):
        policies = self.policies()
        partial = self.api().CapabilityEvidencePolicyRegistry(policies.policies[:-1])
        with self.assertRaises(ValueError):
            partial.require_active_coverage(self.registry())

    def test_policy_rejects_wrong_subject_unknown_keys_and_duplicate_id(self):
        api = self.api()
        raw = self.policies().policies[0].payload()
        for change in ({"release_subject_digest": "f" * 64}, {"unknown": True}, {"zero_official_allowed": "false"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                api.CapabilityEvidencePolicyV1.from_mapping(dict(raw, **change), self.registry())
        with self.assertRaises(ValueError):
            api.CapabilityEvidencePolicyRegistry((self.policies().policies[0],) * 2)

    def test_policy_digest_set_order_independence_and_rule_change_sensitivity(self):
        api = self.api()
        raw = self.policies().require("BLS_CPI_CORE4_HTML").payload()
        shuffled = copy.deepcopy(raw)
        shuffled["required_exceptional_cases"].reverse()
        a = api.CapabilityEvidencePolicyV1.from_mapping(raw, self.registry())
        b = api.CapabilityEvidencePolicyV1.from_mapping(shuffled, self.registry())
        self.assertEqual(a.policy_digest, b.policy_digest)
        changed = copy.deepcopy(raw)
        changed["coverage_rule"]["end_reference_month"] = "2026-07"
        self.assertNotEqual(a.policy_digest, api.CapabilityEvidencePolicyV1.from_mapping(changed, self.registry()).policy_digest)

    def test_canonical_nested_order_unicode_and_escapes(self):
        api = self.api()
        value = {"z": [True, None, 3], "a": {"text": 'é\n"\\'}}
        expected = '{"a":{"text":"é\\n\\"\\\\"},"z":[true,null,3]}'.encode()
        self.assertEqual(api.canonical_evidence_bytes(value), expected)
        self.assertEqual(api.canonical_evidence_bytes(dict(reversed(list(value.items())))), expected)
        self.assertNotEqual(api.canonical_evidence_bytes("é"), api.canonical_evidence_bytes("e\u0301"))

    def test_duplicate_keys_rejected_before_conversion_including_nested_and_escaped(self):
        api = self.api()
        for raw in (b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}', b'{"a":1,"\\u0061":2}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                api.parse_evidence_json(raw)

    def test_forbidden_floats_and_noncanonical_numeric_representations(self):
        api = self.api()
        for value in (1.0, float("nan"), float("inf"), {"nested": [0.5]}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                api.canonical_evidence_bytes(value)
        for raw in (b'1.0', b'1e0', b'NaN', b'-0', b' {"a":1}', b'{"a":1}\n'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                api.parse_evidence_json(raw, require_canonical=True)

    def test_parser_preserves_unicode_and_canonical_requires_exact_bytes(self):
        api = self.api()
        self.assertEqual(api.parse_evidence_json(b'{"a":"\\u00e9"}'), {"a": "é"})
        with self.assertRaises(ValueError):
            api.parse_evidence_json(b'{"a":"\\u00e9"}', require_canonical=True)


if __name__ == "__main__":
    unittest.main()
