"""Immutable technical evidence identity for CPI W1 promotion release review."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass


_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*-v[1-9][0-9]*$")


@dataclass(frozen=True)
class PromotionEvidenceSnapshotV1:
    release_subject_digest: str
    corpus_snapshot_digest: str
    expected_diff_approvals_digest: str
    replay_result_digest: str
    tested_job_contract_version: str
    tested_source_revision: str
    tested_workload_artifact_digest: str | None
    evidence_policy_version: str

    SCHEMA = "cpi-w1-promotion-evidence-snapshot-v1"

    def __post_init__(self) -> None:
        for field in (
            "release_subject_digest",
            "corpus_snapshot_digest",
            "expected_diff_approvals_digest",
            "replay_result_digest",
        ):
            if _DIGEST.fullmatch(getattr(self, field)) is None:
                raise ValueError(f"{field} must be lowercase SHA-256")
        if (
            self.tested_workload_artifact_digest is not None
            and _DIGEST.fullmatch(self.tested_workload_artifact_digest) is None
        ):
            raise ValueError(
                "tested_workload_artifact_digest must be lowercase SHA-256 or null"
            )
        for field in ("tested_job_contract_version", "evidence_policy_version"):
            if _VERSION.fullmatch(getattr(self, field)) is None:
                raise ValueError(f"{field} must be a canonical version token")
        if (
            not isinstance(self.tested_source_revision, str)
            or not self.tested_source_revision
            or self.tested_source_revision != self.tested_source_revision.strip()
        ):
            raise ValueError("tested_source_revision must be canonical and non-empty")

    def payload(self) -> dict[str, str | None]:
        return {
            "schema": self.SCHEMA,
            "release_subject_digest": self.release_subject_digest,
            "corpus_snapshot_digest": self.corpus_snapshot_digest,
            "expected_diff_approvals_digest": self.expected_diff_approvals_digest,
            "replay_result_digest": self.replay_result_digest,
            "tested_job_contract_version": self.tested_job_contract_version,
            "tested_source_revision": self.tested_source_revision,
            "tested_workload_artifact_digest": self.tested_workload_artifact_digest,
            "evidence_policy_version": self.evidence_policy_version,
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
    def evidence_snapshot_digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode("utf-8")).hexdigest()
