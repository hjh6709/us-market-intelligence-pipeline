"""Explicit V2 evidence identity; never reinterpret historical V1 snapshots."""
from dataclasses import dataclass
import hashlib
import re

from src.cpi_w1_evidence_policy import canonical_evidence_bytes, parse_evidence_json


_KEYS = frozenset({"schema", "evidence_policy_version", "promotion_capability_id", "release_subject_digest",
    "capability_evidence_policy_digest", "capability_corpus_snapshot_digest", "expected_diff_approvals_digest",
    "replay_result_digest", "source_contract_digest", "tested_source_content_digest",
    "tested_executor_source_revision", "tested_workload_artifact_digest", "tested_job_contract_version"})


@dataclass(frozen=True)
class PromotionEvidenceSnapshotV2:
    _bytes: bytes
    SCHEMA = "cpi-w1-promotion-evidence-snapshot-v2"
    POLICY = "cpi-w1-evidence-v2"

    def __post_init__(self):
        raw = parse_evidence_json(self._bytes, require_canonical=True)
        if not isinstance(raw, dict) or set(raw) != _KEYS or raw["schema"] != self.SCHEMA or raw["evidence_policy_version"] != self.POLICY:
            raise ValueError("invalid V2 snapshot schema/fields")
        for field, item in raw.items():
            if field in {"tested_executor_source_revision", "tested_workload_artifact_digest"} and item is None:
                continue
            if not isinstance(item, str) or not item or item != item.strip():
                raise ValueError("V2 identity must be canonical text")
            if field.endswith("digest") and re.fullmatch(r"[0-9a-f]{64}", item) is None:
                raise ValueError("V2 digest must be lowercase SHA-256")
        if re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*-v[1-9][0-9]*", raw["tested_job_contract_version"]) is None:
            raise ValueError("invalid tested job version")

    @classmethod
    def from_mapping(cls, value):
        return cls(canonical_evidence_bytes(dict(value)))

    def payload(self):
        return parse_evidence_json(self._bytes, require_canonical=True)

    def __getattr__(self, name):
        if name in _KEYS:
            return self.payload()[name]
        raise AttributeError(name)

    @property
    def canonical_json(self):
        return self._bytes.decode("utf-8")

    @property
    def evidence_snapshot_digest(self):
        return hashlib.sha256(self._bytes).hexdigest()
