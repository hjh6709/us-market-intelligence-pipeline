"""Immutable capability evidence policies and canonical bytes (not release grants)."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Mapping

from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


def canonical_evidence_bytes(value: object) -> bytes:
    def validate(item):
        if item is None or type(item) in (str, bool, int):
            if isinstance(item, str) and "\x00" in item:
                raise ValueError("NUL is outside PostgreSQL JSON vocabulary")
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for key, child in item.items():
                validate(key)
                validate(child)
            return
        raise ValueError("canonical evidence forbids floats and unsupported JSON types")
    validate(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def parse_evidence_json(raw: bytes, *, require_canonical: bool = False) -> object:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    def forbidden(value):
        raise ValueError("floating-point JSON is forbidden")
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                       parse_float=forbidden, parse_constant=forbidden)
    encoded = canonical_evidence_bytes(value)
    if require_canonical and raw != encoded:
        raise ValueError("noncanonical evidence bytes")
    return value


_KEYS = frozenset({"schema", "policy_version", "promotion_capability_id", "release_subject_digest",
    "coverage_mode", "coverage_rule", "required_official_evidence_classes", "required_conformance_classes",
    "required_exceptional_cases", "zero_official_allowed", "complete", "incomplete_reasons"})
_SET_FIELDS = ("required_official_evidence_classes", "required_conformance_classes",
               "required_exceptional_cases", "incomplete_reasons")


@dataclass(frozen=True)
class CapabilityEvidencePolicyV1:
    _bytes: bytes

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], registry: PromotionCapabilityRegistry):
        raw = dict(value)
        if set(raw) != _KEYS:
            raise ValueError("policy requires exact keys")
        if raw["schema"] != "cpi-w1-capability-evidence-policy-v1" or raw["policy_version"] != "cpi-w1-capability-evidence-v1":
            raise ValueError("unsupported evidence policy schema/version")
        capability = registry.require(raw["promotion_capability_id"])
        if raw["release_subject_digest"] != capability.release_subject.release_subject_digest:
            raise ValueError("policy release subject mismatch")
        for field in ("zero_official_allowed", "complete"):
            if type(raw[field]) is not bool:
                raise ValueError("policy flags must be booleans")
        if raw["zero_official_allowed"]:
            raise ValueError("R1 policies require official evidence")
        for field in _SET_FIELDS:
            values = raw[field]
            if not isinstance(values, list) or any(not isinstance(v, str) or not v or v != v.strip() for v in values):
                raise ValueError("policy obligations must be canonical string lists")
            if len(set(values)) != len(values):
                raise ValueError("duplicate policy obligation")
            raw[field] = sorted(values)
        if not raw["required_official_evidence_classes"] or not raw["required_conformance_classes"]:
            raise ValueError("official/conformance obligations are required")
        rule = raw["coverage_rule"]
        if not isinstance(rule, dict):
            raise ValueError("coverage rule must be an object")
        mode = raw["coverage_mode"]
        if mode == "HISTORICAL_MONTHLY_BASELINE":
            if set(rule) != {"start_reference_month", "end_reference_month", "verified_cancellation_required"}:
                raise ValueError("invalid historical coverage rule")
            for key in ("start_reference_month", "end_reference_month"):
                if not isinstance(rule[key], str) or re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", rule[key]) is None:
                    raise ValueError("invalid policy month")
            if rule["start_reference_month"] > rule["end_reference_month"] or rule["verified_cancellation_required"] is not True:
                raise ValueError("invalid interval/cancellation requirement")
        elif mode == "EXPLICIT_HISTORICAL_EXCEPTIONS":
            if set(rule) != {"reference_months"} or rule["reference_months"] != ["2025-10"]:
                raise ValueError("R1 exception coverage must match declared historical exception")
        elif mode == "UNFROZEN_SURFACE_COVERAGE":
            if set(rule) != {"surface"} or rule["surface"] != capability.artifact_contract_kind or raw["complete"]:
                raise ValueError("unfrozen surface cannot be complete")
        else:
            raise ValueError("unsupported policy coverage mode")
        if raw["complete"] == bool(raw["incomplete_reasons"]):
            raise ValueError("policy completeness/reasons disagree")
        return cls(canonical_evidence_bytes(raw))

    def payload(self) -> dict:
        return parse_evidence_json(self._bytes, require_canonical=True)

    @property
    def policy_digest(self):
        return hashlib.sha256(self._bytes).hexdigest()

    @property
    def canonical_bytes(self):
        return self._bytes

    @property
    def promotion_capability_id(self):
        return self.payload()["promotion_capability_id"]

    @property
    def release_subject_digest(self):
        return self.payload()["release_subject_digest"]

    @property
    def complete(self):
        return self.payload()["complete"]

    @property
    def zero_official_allowed(self):
        return self.payload()["zero_official_allowed"]


@dataclass(frozen=True)
class CapabilityEvidencePolicyRegistry:
    policies: tuple[CapabilityEvidencePolicyV1, ...]

    def __post_init__(self):
        ids = [p.promotion_capability_id for p in self.policies]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate capability evidence policy")

    @classmethod
    def from_json(cls, path: Path, registry: PromotionCapabilityRegistry):
        raw = parse_evidence_json(path.read_bytes())
        if not isinstance(raw, dict) or set(raw) != {"schema", "policies"} or raw["schema"] != "cpi-w1-capability-evidence-policy-registry-v1":
            raise ValueError("invalid evidence policy registry")
        return cls(tuple(CapabilityEvidencePolicyV1.from_mapping(p, registry) for p in raw["policies"]))

    def require(self, capability_id: str):
        for policy in self.policies:
            if policy.promotion_capability_id == capability_id:
                return policy
        raise KeyError(capability_id)

    def require_active_coverage(self, registry: PromotionCapabilityRegistry):
        for capability in registry.active():
            try:
                self.require(capability.promotion_capability_id)
            except KeyError as exc:
                raise ValueError("ACTIVE capability has no policy") from exc
