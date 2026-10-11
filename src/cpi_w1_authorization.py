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
        raise PromotionAuthorizationError("LEGACY_AUTHORIZATION_RETIRED")

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


@dataclass(frozen=True)
class PromotionAuthorizationMaterialV2:
    _bytes: bytes
    SCHEMA = "cpi-w1-promotion-authorization-material-v2"
    POLICY = "cpi-w1-authorization-v2"

    @classmethod
    def from_review(cls, *, evidence, gate_decision, executor, current_policy, source_contract_digest, review):
        from src.cpi_w1_evidence_snapshot_v2 import PromotionEvidenceSnapshotV2
        from src.cpi_w1_evidence_policy import canonical_evidence_bytes
        if not isinstance(evidence, PromotionEvidenceSnapshotV2):
            raise PromotionAuthorizationError("V2 authorization requires explicit V2 evidence")
        if not current_policy.complete or evidence.capability_evidence_policy_digest != current_policy.policy_digest:
            raise PromotionAuthorizationError("current evidence policy mismatch or incomplete")
        if (evidence.release_subject_digest != current_policy.release_subject_digest
            or evidence.promotion_capability_id != current_policy.promotion_capability_id
            or evidence.source_contract_digest != source_contract_digest):
            raise PromotionAuthorizationError("subject/capability/source contract mismatch")
        if evidence.tested_workload_artifact_digest is None or evidence.tested_executor_source_revision is None:
            raise PromotionAuthorizationError("eligible authorization requires tested build")
        if (executor.workload_artifact_digest != evidence.tested_workload_artifact_digest
            or executor.job_contract_version != evidence.tested_job_contract_version
            or executor.source_revision != evidence.tested_executor_source_revision):
            raise PromotionAuthorizationError("executor is not the exact tested build")
        if (not gate_decision.eligible or gate_decision.gate_decision_digest is None
            or gate_decision.release_subject_digest != evidence.release_subject_digest
            or gate_decision.promotion_capability_id != evidence.promotion_capability_id
            or gate_decision.evidence_snapshot_digest != evidence.evidence_snapshot_digest):
            raise PromotionAuthorizationError("gate/evidence identity mismatch or blocked")
        bindings = {key: evidence.payload()[key] for key in (
            "release_subject_digest", "promotion_capability_id", "capability_evidence_policy_digest",
            "source_contract_digest", "tested_source_content_digest", "tested_executor_source_revision",
            "tested_workload_artifact_digest", "tested_job_contract_version")}
        bindings.update(evidence_snapshot_digest=evidence.evidence_snapshot_digest,
                        gate_policy_version=gate_decision.gate_policy_version)
        if review.payload()["purpose"] != "PROMOTION_AUTHORIZATION":
            raise PromotionAuthorizationError("authorization review purpose required")
        review.require_bindings(bindings)
        if gate_decision.review_ref != review.review_ref or gate_decision.review_digest != review.review_digest:
            raise PromotionAuthorizationError("gate review does not match verified review")
        return cls(canonical_evidence_bytes({"schema": cls.SCHEMA,
            "authorization_policy_version": cls.POLICY, "release_subject_digest": evidence.release_subject_digest,
            "evidence_snapshot_digest": evidence.evidence_snapshot_digest,
            "gate_decision_digest": gate_decision.gate_decision_digest,
            "gate_policy_version": gate_decision.gate_policy_version,
            "executor_source_revision": executor.source_revision,
            "executor_workload_artifact_digest": executor.workload_artifact_digest,
            "executor_job_contract_version": executor.job_contract_version,
            "review_ref": review.review_ref, "review_digest": review.review_digest}))

    def payload(self):
        from src.cpi_w1_evidence_policy import parse_evidence_json
        return parse_evidence_json(self._bytes, require_canonical=True)

    def __getattr__(self, name):
        values = self.payload()
        if name in values:
            return values[name]
        raise AttributeError(name)

    @property
    def canonical_json(self):
        return self._bytes.decode("utf-8")

    @property
    def authorization_material_digest(self):
        return hashlib.sha256(self._bytes).hexdigest()
