"""Checked-in CPI W1 promotion capability registry."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from src.cpi_w1_contracts import PromotionFamily
from src.cpi_w1_release_subject import ReleaseSubjectV1


_SCHEMA_VERSION = "cpi-w1-promotion-capability-registry-v1"
_CONTRACT_VERSION = "cpi-w1-promotion-capabilities-v1"
_ROOT_KEYS = frozenset({"schema_version", "contract_version", "capabilities"})
_CAPABILITY_KEYS = frozenset(
    {
        "promotion_capability_id",
        "source_code",
        "artifact_contract_kind",
        "source_contract_version",
        "extractor_contract_version",
        "promotion_family",
        "lifecycle",
    }
)


@dataclass(frozen=True)
class PromotionCapability:
    promotion_capability_id: str
    source_code: str
    artifact_contract_kind: str
    source_contract_version: str
    extractor_contract_version: str
    promotion_family: str
    lifecycle: str

    def __post_init__(self) -> None:
        if self.lifecycle not in {"ACTIVE", "RETIRED"}:
            raise ValueError("capability lifecycle must be ACTIVE or RETIRED")
        if self.promotion_family not in {item.value for item in PromotionFamily}:
            raise ValueError("unknown CPI W1 promotion family")
        self.release_subject

    @property
    def release_subject(self) -> ReleaseSubjectV1:
        return ReleaseSubjectV1(
            source_code=self.source_code,
            artifact_contract_kind=self.artifact_contract_kind,
            source_contract_version=self.source_contract_version,
            promotion_capability_id=self.promotion_capability_id,
            extractor_contract_version=self.extractor_contract_version,
            promotion_family=self.promotion_family,
        )


@dataclass(frozen=True)
class PromotionCapabilityRegistry:
    capabilities: tuple[PromotionCapability, ...]
    registry_fingerprint: str

    @classmethod
    def from_json(cls, path: str | Path) -> "PromotionCapabilityRegistry":
        raw_bytes = Path(path).read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("capability registry must be an object")
        _require_exact_keys(raw, _ROOT_KEYS, "registry")
        if raw["schema_version"] != _SCHEMA_VERSION:
            raise ValueError("unsupported capability registry schema")
        if raw["contract_version"] != _CONTRACT_VERSION:
            raise ValueError("unsupported capability registry contract")
        items = raw["capabilities"]
        if not isinstance(items, list) or not items:
            raise ValueError("capabilities must be a non-empty list")

        capabilities: list[PromotionCapability] = []
        seen_ids: set[str] = set()
        seen_subjects: set[str] = set()
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(f"capability {index} must be an object")
            _require_exact_keys(item, _CAPABILITY_KEYS, f"capability {index}")
            capability = PromotionCapability(**item)
            if capability.promotion_capability_id in seen_ids:
                raise ValueError(
                    f"duplicate promotion capability: {capability.promotion_capability_id}"
                )
            digest = capability.release_subject.release_subject_digest
            if digest in seen_subjects:
                raise ValueError("duplicate release subject in capability registry")
            seen_ids.add(capability.promotion_capability_id)
            seen_subjects.add(digest)
            capabilities.append(capability)
        return cls(
            capabilities=tuple(capabilities),
            registry_fingerprint=hashlib.sha256(raw_bytes).hexdigest(),
        )

    def require(self, promotion_capability_id: str) -> PromotionCapability:
        matches = [
            item
            for item in self.capabilities
            if item.promotion_capability_id == promotion_capability_id
        ]
        if len(matches) != 1:
            raise KeyError(promotion_capability_id)
        return matches[0]

    def active(self) -> tuple[PromotionCapability, ...]:
        return tuple(item for item in self.capabilities if item.lifecycle == "ACTIVE")


def _require_exact_keys(
    value: dict[str, object], expected: frozenset[str], label: str
) -> None:
    actual = frozenset(value)
    missing = expected - actual
    if missing:
        raise ValueError(f"{label} missing keys: {sorted(missing)}")
    extra = actual - expected
    if extra:
        raise ValueError(f"{label} unexpected keys: {sorted(extra)}")
