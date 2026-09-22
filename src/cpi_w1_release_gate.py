"""Checked-in extractor release-gate snapshot for CPI W1 runtime orchestration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


_SCHEMA_VERSION = "cpi-w1-extractor-release-gate-v1"


@dataclass(frozen=True)
class ExtractorGateDecision:
    extractor_contract_version: str
    eligible: bool
    reason_code: str | None
    review_ref: str | None


class CpiW1ExtractorReleaseGate:
    def __init__(
        self,
        *,
        source_contract_version: str,
        approved_extractors: frozenset[str],
        blocked_extractors: dict[str, dict[str, str]],
        gate_fingerprint: str,
    ) -> None:
        self.source_contract_version = source_contract_version
        self.approved_extractors = approved_extractors
        self.blocked_extractors = blocked_extractors
        self.gate_fingerprint = gate_fingerprint

    @classmethod
    def from_json(cls, path: str | Path) -> "CpiW1ExtractorReleaseGate":
        raw_bytes = Path(path).read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
        if raw.get("schema_version") != _SCHEMA_VERSION:
            raise ValueError("unsupported CPI extractor release-gate schema")
        if raw.get("source_contract_version") != "bls-cpi-source-v1":
            raise ValueError("unsupported CPI release-gate source contract")
        approved = raw.get("approved_extractors")
        blocked = raw.get("blocked_extractors")
        if not isinstance(approved, list) or not isinstance(blocked, dict):
            raise ValueError("invalid CPI extractor release-gate payload")
        approved_set = frozenset(str(item) for item in approved)
        if approved_set.intersection(blocked):
            raise ValueError("extractor cannot be both approved and blocked")
        for version in approved_set:
            if not version or version != version.strip():
                raise ValueError("approved extractor version must be canonical")
        normalized_blocked: dict[str, dict[str, str]] = {}
        for version, detail in blocked.items():
            if not isinstance(detail, dict):
                raise ValueError("blocked extractor detail must be an object")
            reason = detail.get("reason_code")
            review_ref = detail.get("review_ref")
            if (
                not isinstance(reason, str)
                or not reason
                or reason != reason.strip()
                or not isinstance(review_ref, str)
                or not review_ref
                or review_ref != review_ref.strip()
            ):
                raise ValueError("blocked extractor requires reason_code and review_ref")
            normalized_blocked[str(version)] = {
                "reason_code": reason,
                "review_ref": review_ref,
            }
        return cls(
            source_contract_version=raw["source_contract_version"],
            approved_extractors=approved_set,
            blocked_extractors=normalized_blocked,
            gate_fingerprint=hashlib.sha256(raw_bytes).hexdigest(),
        )

    def decision(self, extractor_contract_version: str) -> ExtractorGateDecision:
        if extractor_contract_version in self.approved_extractors:
            return ExtractorGateDecision(
                extractor_contract_version=extractor_contract_version,
                eligible=True,
                reason_code=None,
                review_ref=None,
            )
        detail = self.blocked_extractors.get(extractor_contract_version)
        if detail is None:
            return ExtractorGateDecision(
                extractor_contract_version=extractor_contract_version,
                eligible=False,
                reason_code="EXTRACTOR_NOT_REVIEWED",
                review_ref=None,
            )
        return ExtractorGateDecision(
            extractor_contract_version=extractor_contract_version,
            eligible=False,
            reason_code=detail["reason_code"],
            review_ref=detail["review_ref"],
        )
