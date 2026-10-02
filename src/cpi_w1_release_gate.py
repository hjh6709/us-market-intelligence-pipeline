"""Capability-scoped checked-in release gate for CPI W1 promotion."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotRegistry
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry


_SCHEMA_VERSION = "cpi-w1-capability-release-gate-v2"
_POLICY_VERSION = "cpi-w1-gate-v2"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_DECISION_KEYS = frozenset(
    {
        "promotion_capability_id",
        "release_subject_digest",
        "evidence_snapshot_digest",
        "decision",
        "reason_code",
        "review_ref",
        "review_digest",
        "gate_decision_digest",
    }
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CapabilityGateDecision:
    promotion_capability_id: str
    release_subject_digest: str
    evidence_snapshot_digest: str | None
    gate_policy_version: str
    decision: str
    reason_code: str
    review_ref: str | None
    review_digest: str | None
    gate_decision_digest: str | None

    @property
    def eligible(self) -> bool:
        return self.decision == "ELIGIBLE"


@dataclass(frozen=True)
class CpiW1CapabilityReleaseGate:
    decisions: dict[tuple[str, str], CapabilityGateDecision]
    gate_snapshot_digest: str

    @classmethod
    def from_json(
        cls,
        path: str | Path,
        *,
        capability_registry_path: str | Path = Path(
            "config/cpi_w1_promotion_capabilities.json"
        ),
        evidence_snapshot_path: str | Path = Path(
            "config/cpi_w1_evidence_snapshots.json"
        ),
    ) -> "CpiW1CapabilityReleaseGate":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if frozenset(raw) != {"schema_version", "gate_policy_version", "decisions"}:
            raise ValueError("capability release-gate keys must be exact")
        if raw["schema_version"] != _SCHEMA_VERSION:
            raise ValueError("unsupported CPI capability release-gate schema")
        if raw["gate_policy_version"] != _POLICY_VERSION:
            raise ValueError("unsupported CPI gate policy")
        entries = raw["decisions"]
        if not isinstance(entries, list) or not entries:
            raise ValueError("capability release-gate decisions must not be empty")

        capabilities = PromotionCapabilityRegistry.from_json(
            capability_registry_path
        )
        evidence = PromotionEvidenceSnapshotRegistry.from_json(
            evidence_snapshot_path
        )
        decisions: dict[tuple[str, str], CapabilityGateDecision] = {}
        digest_material: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict) or frozenset(entry) != _DECISION_KEYS:
                raise ValueError("gate decision keys must be exact")
            capability = capabilities.require(entry["promotion_capability_id"])
            subject_digest = capability.release_subject.release_subject_digest
            if entry["release_subject_digest"] != subject_digest:
                raise ValueError("gate decision release subject mismatch")
            snapshot = evidence.require(capability.promotion_capability_id)
            if snapshot.release_subject_digest != subject_digest:
                raise ValueError("evidence snapshot release subject mismatch")
            if entry["evidence_snapshot_digest"] != snapshot.evidence_snapshot_digest:
                raise ValueError("gate decision evidence snapshot mismatch")
            if entry["decision"] not in {"ELIGIBLE", "BLOCKED"}:
                raise ValueError("gate decision must be ELIGIBLE or BLOCKED")
            for field in ("reason_code", "review_ref"):
                value = entry[field]
                if not isinstance(value, str) or not value or value != value.strip():
                    raise ValueError(f"gate decision {field} must be canonical")
            if _DIGEST.fullmatch(entry["review_digest"]) is None:
                raise ValueError("gate decision review_digest must be lowercase SHA-256")
            material = {
                key: entry[key]
                for key in _DECISION_KEYS
                if key != "gate_decision_digest"
            }
            material["gate_policy_version"] = _POLICY_VERSION
            decision_digest = _digest(material)
            if entry["gate_decision_digest"] != decision_digest:
                raise ValueError("gate decision digest mismatch")
            key = (capability.promotion_capability_id, subject_digest)
            if key in decisions:
                raise ValueError("duplicate capability gate decision")
            decisions[key] = CapabilityGateDecision(
                promotion_capability_id=capability.promotion_capability_id,
                release_subject_digest=subject_digest,
                evidence_snapshot_digest=snapshot.evidence_snapshot_digest,
                gate_policy_version=_POLICY_VERSION,
                decision=entry["decision"],
                reason_code=entry["reason_code"],
                review_ref=entry["review_ref"],
                review_digest=entry["review_digest"],
                gate_decision_digest=decision_digest,
            )
            digest_material.append(material | {"gate_decision_digest": decision_digest})

        active_ids = {
            capability.promotion_capability_id for capability in capabilities.active()
        }
        if {key[0] for key in decisions} != active_ids:
            raise ValueError("gate decisions must cover every active capability exactly")
        return cls(
            decisions=decisions,
            gate_snapshot_digest=_digest(
                sorted(
                    digest_material,
                    key=lambda item: (
                        item["promotion_capability_id"],
                        item["release_subject_digest"],
                    ),
                )
            ),
        )

    @property
    def gate_fingerprint(self) -> str:
        return self.gate_snapshot_digest

    def decision(
        self,
        promotion_capability_id: str,
        release_subject_digest: str | None = None,
    ) -> CapabilityGateDecision:
        if release_subject_digest is not None:
            matched = self.decisions.get(
                (promotion_capability_id, release_subject_digest)
            )
            if matched is not None:
                return matched
        return CapabilityGateDecision(
            promotion_capability_id=promotion_capability_id,
            release_subject_digest=release_subject_digest or "0" * 64,
            evidence_snapshot_digest=None,
            gate_policy_version=_POLICY_VERSION,
            decision="BLOCKED",
            reason_code="RELEASE_SUBJECT_NOT_REVIEWED",
            review_ref=None,
            review_digest=None,
            gate_decision_digest=None,
        )


# Transitional import compatibility; semantic decisions are capability scoped.
CpiW1ExtractorReleaseGate = CpiW1CapabilityReleaseGate
ExtractorGateDecision = CapabilityGateDecision
