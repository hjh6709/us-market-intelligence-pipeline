"""Byte-integrity and semantic-binding checks; not reviewer identity or approval IAM."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path, PurePosixPath
import re

from src.cpi_w1_evidence_policy import parse_evidence_json


class ReviewArtifactError(ValueError):
    pass


AUTHORIZATION_BINDING_KEYS = frozenset({"release_subject_digest", "promotion_capability_id",
    "capability_evidence_policy_digest", "evidence_snapshot_digest", "source_contract_digest",
    "tested_source_content_digest", "tested_executor_source_revision", "tested_workload_artifact_digest",
    "tested_job_contract_version", "gate_policy_version"})
DIFF_BINDING_KEYS = frozenset({"release_subject_digest", "promotion_capability_id", "artifact_sha256",
    "extractor_contract_version", "expected_semantics_sha256", "actual_semantics_sha256"})


@dataclass(frozen=True)
class ReviewArtifactV1:
    review_ref: str
    raw_bytes: bytes

    def __post_init__(self):
        value = parse_evidence_json(self.raw_bytes, require_canonical=True)
        if not isinstance(value, dict) or set(value) != {"schema", "purpose", "bindings"} or value["schema"] != "cpi-w1-review-artifact-v1":
            raise ReviewArtifactError("unsupported review schema")
        keys = {"PROMOTION_AUTHORIZATION": AUTHORIZATION_BINDING_KEYS, "EXPECTED_DIFF": DIFF_BINDING_KEYS}.get(value["purpose"])
        bindings = value["bindings"]
        if keys is None or not isinstance(bindings, dict) or set(bindings) != keys:
            raise ReviewArtifactError("review purpose/binding keys mismatch")
        for field, item in bindings.items():
            if not isinstance(item, str) or not item or item != item.strip():
                raise ReviewArtifactError("review bindings require exact non-null identities")
            if field.endswith(("digest", "sha256")) and re.fullmatch(r"[0-9a-f]{64}", item) is None:
                raise ReviewArtifactError("review binding digest must be lowercase SHA-256")

    @property
    def review_digest(self):
        return hashlib.sha256(self.raw_bytes).hexdigest()

    def payload(self):
        return parse_evidence_json(self.raw_bytes, require_canonical=True)

    def require_bindings(self, expected):
        if self.payload()["bindings"] != dict(expected):
            raise ReviewArtifactError("review semantic bindings mismatch")


def load_review_artifact(repo_root: Path, review_ref: str, claimed_digest: str) -> ReviewArtifactV1:
    if not isinstance(review_ref, str) or not review_ref or "\\" in review_ref:
        raise ReviewArtifactError("review reference must be repository relative")
    relative = PurePosixPath(review_ref)
    if relative.is_absolute() or str(relative) != review_ref or any(p in {".", ".."} for p in relative.parts):
        raise ReviewArtifactError("noncanonical review path")
    root = repo_root.resolve()
    target = root
    for part in relative.parts:
        target = target / part
        if target.is_symlink():
            raise ReviewArtifactError("review symlink forbidden")
    if not target.is_file() or not target.resolve().is_relative_to(root):
        raise ReviewArtifactError("review file missing or out of root")
    raw = target.read_bytes()
    if hashlib.sha256(raw).hexdigest() != claimed_digest:
        raise ReviewArtifactError("review byte digest mismatch")
    return ReviewArtifactV1(review_ref, raw)
