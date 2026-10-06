"""Immutable CPI W1 release authorization material."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotV1
from src.cpi_w1_release_gate import CapabilityGateDecision


_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*-v[1-9][0-9]*$")


class PromotionAuthorizationError(ValueError):
    """The reviewed evidence is not eligible for authorization."""


def _canonical_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise PromotionAuthorizationError(f"{field} must be canonical and non-empty")
    return value


def _digest(value: str, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise PromotionAuthorizationError(f"{field} must be lowercase SHA-256")
    return value


@dataclass(frozen=True)
class ExecutorProvenanceV1:
    source_revision: str
    workload_artifact_digest: str
    job_contract_version: str

    def __post_init__(self) -> None:
        _canonical_text(self.source_revision, "source_revision")
        _digest(self.workload_artifact_digest, "workload_artifact_digest")
        if _VERSION.fullmatch(self.job_contract_version) is None:
            raise PromotionAuthorizationError(
                "job_contract_version must be a canonical version token"
            )


@dataclass(frozen=True)
class PromotionAuthorizationMaterialV1:
    release_subject_digest: str
    evidence_snapshot_digest: str
    gate_decision_digest: str
    gate_policy_version: str
    authorization_policy_version: str
    executor_source_revision: str
    executor_workload_artifact_digest: str
    executor_job_contract_version: str
    review_ref: str
    review_digest: str

    SCHEMA = "cpi-w1-promotion-authorization-material-v1"
    POLICY = "cpi-w1-authorization-v1"

    def __post_init__(self) -> None:
        for field in (
            "release_subject_digest",
            "evidence_snapshot_digest",
            "gate_decision_digest",
            "executor_workload_artifact_digest",
            "review_digest",
        ):
            _digest(getattr(self, field), field)
        for field in (
            "gate_policy_version",
            "authorization_policy_version",
            "executor_job_contract_version",
        ):
            if _VERSION.fullmatch(getattr(self, field)) is None:
                raise PromotionAuthorizationError(
                    f"{field} must be a canonical version token"
                )
        _canonical_text(self.executor_source_revision, "executor_source_revision")
        _canonical_text(self.review_ref, "review_ref")
        if self.authorization_policy_version != self.POLICY:
            raise PromotionAuthorizationError("unsupported authorization policy")

    @classmethod
    def from_review(
        cls,
        *,
        evidence: PromotionEvidenceSnapshotV1,
        gate_decision: CapabilityGateDecision,
        executor: ExecutorProvenanceV1,
        review_ref: str,
        review_digest: str,
    ) -> "PromotionAuthorizationMaterialV1":
        if not gate_decision.eligible:
            raise PromotionAuthorizationError("gate decision is not eligible")
        if gate_decision.gate_decision_digest is None:
            raise PromotionAuthorizationError("gate decision has no immutable digest")
        if gate_decision.release_subject_digest != evidence.release_subject_digest:
            raise PromotionAuthorizationError("gate and evidence subject mismatch")
        if gate_decision.evidence_snapshot_digest != evidence.evidence_snapshot_digest:
            raise PromotionAuthorizationError("gate and evidence snapshot mismatch")
        return cls(
            release_subject_digest=evidence.release_subject_digest,
            evidence_snapshot_digest=evidence.evidence_snapshot_digest,
            gate_decision_digest=gate_decision.gate_decision_digest,
            gate_policy_version=gate_decision.gate_policy_version,
            authorization_policy_version=cls.POLICY,
            executor_source_revision=executor.source_revision,
            executor_workload_artifact_digest=executor.workload_artifact_digest,
            executor_job_contract_version=executor.job_contract_version,
            review_ref=review_ref,
            review_digest=review_digest,
        )

    def payload(self) -> dict[str, str]:
        return {
            "schema": self.SCHEMA,
            "release_subject_digest": self.release_subject_digest,
            "evidence_snapshot_digest": self.evidence_snapshot_digest,
            "gate_decision_digest": self.gate_decision_digest,
            "gate_policy_version": self.gate_policy_version,
            "authorization_policy_version": self.authorization_policy_version,
            "executor_source_revision": self.executor_source_revision,
            "executor_workload_artifact_digest": self.executor_workload_artifact_digest,
            "executor_job_contract_version": self.executor_job_contract_version,
            "review_ref": self.review_ref,
            "review_digest": self.review_digest,
        }

    @property
    def canonical_json(self) -> str:
        return json.dumps(
            self.payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    @property
    def authorization_material_digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode("utf-8")).hexdigest()
