"""Canonical CPI W1 release-authorization subject identity."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import ClassVar, Mapping


_UPPER_TOKEN = re.compile(r"^[A-Z][A-Z0-9_]*$")
_VERSION_TOKEN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*-v[1-9][0-9]*$")


@dataclass(frozen=True)
class ReleaseSubjectV1:
    source_code: str
    artifact_contract_kind: str
    source_contract_version: str
    promotion_capability_id: str
    extractor_contract_version: str
    promotion_family: str

    SCHEMA: ClassVar[str] = "cpi-w1-release-subject-v1"
    _FIELDS: ClassVar[tuple[str, ...]] = (
        "source_code",
        "artifact_contract_kind",
        "source_contract_version",
        "promotion_capability_id",
        "extractor_contract_version",
        "promotion_family",
    )
    _INPUT_KEYS: ClassVar[frozenset[str]] = frozenset(("schema", *_FIELDS))

    def __post_init__(self) -> None:
        for field in self._FIELDS:
            value = getattr(self, field)
            if not isinstance(value, str):
                raise ValueError(f"{field} must be a string")
            if not value or value != value.strip():
                raise ValueError(f"{field} must be a non-empty exact token")

        for field in (
            "source_code",
            "artifact_contract_kind",
            "promotion_capability_id",
            "promotion_family",
        ):
            if _UPPER_TOKEN.fullmatch(getattr(self, field)) is None:
                raise ValueError(f"{field} must be a canonical uppercase token")

        for field in ("source_contract_version", "extractor_contract_version"):
            if _VERSION_TOKEN.fullmatch(getattr(self, field)) is None:
                raise ValueError(f"{field} must be a canonical version token")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReleaseSubjectV1":
        keys = frozenset(value)
        missing = cls._INPUT_KEYS - keys
        if missing:
            raise ValueError(f"missing keys: {sorted(missing, key=str)}")
        extra = keys - cls._INPUT_KEYS
        if extra:
            raise ValueError(f"unexpected keys: {sorted(extra, key=str)}")
        if value["schema"] != cls.SCHEMA:
            raise ValueError("schema must be cpi-w1-release-subject-v1")
        return cls(**{field: value[field] for field in cls._FIELDS})  # type: ignore[arg-type]

    def payload(self) -> dict[str, str]:
        return {
            "schema": self.SCHEMA,
            **{field: getattr(self, field) for field in self._FIELDS},
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
    def release_subject_digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode("utf-8")).hexdigest()
