"""Immutable technical evidence identity for CPI W1 promotion release review."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Mapping


_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*-v[1-9][0-9]*$")
TESTED_SOURCE_PATHS: tuple[str, ...] = (
    "src/cpi_w1_release.py",
    "src/cpi_w1_schedule.py",
    "scripts/replay_cpi_w1_corpus.py",
    "config/cpi_w1_promotion_capabilities.json",
)


def tested_source_revision_digest(repo_root: str | Path) -> str:
    root = Path(repo_root).resolve()
    material = [
        {
            "path": relative_path,
            "sha256": hashlib.sha256((root / relative_path).read_bytes()).hexdigest(),
        }
        for relative_path in TESTED_SOURCE_PATHS
    ]
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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

    SCHEMA: ClassVar[str] = "cpi-w1-promotion-evidence-snapshot-v1"
    FIELDS: ClassVar[tuple[str, ...]] = (
        "release_subject_digest",
        "corpus_snapshot_digest",
        "expected_diff_approvals_digest",
        "replay_result_digest",
        "tested_job_contract_version",
        "tested_source_revision",
        "tested_workload_artifact_digest",
        "evidence_policy_version",
    )

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

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PromotionEvidenceSnapshotV1":
        if frozenset(value) != frozenset(cls.FIELDS):
            raise ValueError("evidence snapshot fields must be exact")
        return cls(**{field: value[field] for field in cls.FIELDS})


@dataclass(frozen=True)
class PromotionEvidenceSnapshotRegistry:
    by_capability: dict[str, PromotionEvidenceSnapshotV1]

    @classmethod
    def from_json(cls, path: str | Path) -> "PromotionEvidenceSnapshotRegistry":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if frozenset(raw) != {"schema_version", "snapshots"}:
            raise ValueError("evidence snapshot registry keys must be exact")
        if raw["schema_version"] != "cpi-w1-evidence-snapshot-registry-v1":
            raise ValueError("unsupported evidence snapshot registry schema")
        items = raw["snapshots"]
        if not isinstance(items, list) or not items:
            raise ValueError("evidence snapshot registry must not be empty")
        result: dict[str, PromotionEvidenceSnapshotV1] = {}
        seen_digests: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("evidence snapshot entry must be an object")
            expected_keys = {
                "promotion_capability_id",
                "evidence_snapshot_digest",
                *PromotionEvidenceSnapshotV1.FIELDS,
            }
            if frozenset(item) != frozenset(expected_keys):
                raise ValueError("evidence snapshot registry entry keys must be exact")
            capability_id = item["promotion_capability_id"]
            if not isinstance(capability_id, str) or capability_id in result:
                raise ValueError("evidence snapshot capability must be unique")
            snapshot = PromotionEvidenceSnapshotV1.from_mapping(
                {field: item[field] for field in PromotionEvidenceSnapshotV1.FIELDS}
            )
            if item["evidence_snapshot_digest"] != snapshot.evidence_snapshot_digest:
                raise ValueError("evidence snapshot digest mismatch")
            if snapshot.evidence_snapshot_digest in seen_digests:
                raise ValueError("evidence snapshot digest must be unique")
            result[capability_id] = snapshot
            seen_digests.add(snapshot.evidence_snapshot_digest)
        return cls(by_capability=result)

    def require(self, promotion_capability_id: str) -> PromotionEvidenceSnapshotV1:
        try:
            return self.by_capability[promotion_capability_id]
        except KeyError as exc:
            raise KeyError(promotion_capability_id) from exc
