"""CPI W1 semantic contracts.

This module is intentionally dependency-free.  It defines target-only semantic
vocabulary and deterministic material identity without modifying legacy
platform_contracts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Mapping
from uuid import UUID


class RunMode(StrEnum):
    LIVE = "LIVE"
    BACKFILL = "BACKFILL"
    REPLAY = "REPLAY"


class WorkOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"
    DATA_NOT_AVAILABLE = "DATA_NOT_AVAILABLE"
    SKIPPED = "SKIPPED"


class ScheduleStatus(StrEnum):
    SCHEDULED = "SCHEDULED"
    DATE_PENDING = "DATE_PENDING"
    CANCELED = "CANCELED"


class TimePrecision(StrEnum):
    EXACT = "EXACT"
    DATE_ONLY = "DATE_ONLY"


class DisclosureRelationKind(StrEnum):
    EVENT_RELEASE = "EVENT_RELEASE"
    SUPPLEMENTAL_DISCLOSURE = "SUPPLEMENTAL_DISCLOSURE"


class DisclosureArtifactRelationKind(StrEnum):
    RELEASE_REPRESENTATION = "RELEASE_REPRESENTATION"
    CORROBORATING_REPRESENTATION = "CORROBORATING_REPRESENTATION"
    CORRECTION_NOTICE = "CORRECTION_NOTICE"


class ObservationState(StrEnum):
    VALUE = "VALUE"
    EXPLICIT_UNAVAILABLE = "EXPLICIT_UNAVAILABLE"


class KnowledgeMode(StrEnum):
    OFFICIAL_SOURCE_RECONSTRUCTION = "OFFICIAL_SOURCE_RECONSTRUCTION"
    SYSTEM_KNOWN_PIT = "SYSTEM_KNOWN_PIT"


class ReleaseProjectionState(StrEnum):
    NOT_YET_DUE = "NOT_YET_DUE"
    DUE_DATE_UNTIMED = "DUE_DATE_UNTIMED"
    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
    DISCLOSED = "DISCLOSED"
    NO_RELEASE_EXPECTED = "NO_RELEASE_EXPECTED"
    UNRESOLVED = "UNRESOLVED"
    CONFLICT = "CONFLICT"


class ServingControlState(StrEnum):
    ENABLED = "ENABLED"
    WITHHELD = "WITHHELD"


class PromotionFamily(StrEnum):
    CPI_RELEASE_ENVELOPE_PROMOTE = "CPI_RELEASE_ENVELOPE_PROMOTE"
    CPI_CORROBORATING_REPRESENTATION_PROMOTE = "CPI_CORROBORATING_REPRESENTATION_PROMOTE"
    CPI_OBSERVATION_BUNDLE_PROMOTE = "CPI_OBSERVATION_BUNDLE_PROMOTE"


class SourceAuthorityRole(StrEnum):
    AUTHORITATIVE = "AUTHORITATIVE"
    FALLBACK_CORROBORATION = "FALLBACK_CORROBORATION"
    FIXTURE_ONLY = "FIXTURE_ONLY"


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("semantic decimal must be finite")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _canonicalize(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        raise TypeError("binary floating point is forbidden in CPI semantic material")
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("semantic datetime must be timezone-aware")
        normalized = value.astimezone(timezone.utc)
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonicalize(item) for item in value]
    raise TypeError(f"unsupported semantic material type: {type(value).__name__}")


def canonical_material_json(payload: Mapping[str, object]) -> str:
    """Return stable JSON for typed semantic material only."""
    canonical = _canonicalize(payload)
    return json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def material_fingerprint(payload: Mapping[str, object]) -> str:
    """Hash canonical typed semantic material.

    Callers must build payloads from typed material builders below rather than
    passing artifact IDs, URLs, parser versions, actors, or timestamps that are
    provenance rather than semantics.
    """
    encoded = canonical_material_json(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ScheduleMaterial:
    schedule_status: ScheduleStatus
    scheduled_date: date | None
    scheduled_at: datetime | None
    schedule_timezone: str | None
    time_precision: TimePrecision | None

    def __post_init__(self) -> None:
        if self.scheduled_at is not None and (
            self.scheduled_at.tzinfo is None
            or self.scheduled_at.utcoffset() is None
        ):
            raise ValueError("scheduled_at must be timezone-aware")

        if self.schedule_status is ScheduleStatus.DATE_PENDING:
            if any(
                value is not None
                for value in (
                    self.scheduled_date,
                    self.scheduled_at,
                    self.schedule_timezone,
                    self.time_precision,
                )
            ):
                raise ValueError("DATE_PENDING must not manufacture schedule time")
            return

        if self.time_precision is TimePrecision.EXACT:
            if (
                self.scheduled_date is None
                or self.scheduled_at is None
                or not self.schedule_timezone
            ):
                raise ValueError("EXACT schedule requires date, instant, and timezone")
        elif self.time_precision is TimePrecision.DATE_ONLY:
            if (
                self.scheduled_date is None
                or self.scheduled_at is not None
                or not self.schedule_timezone
            ):
                raise ValueError("DATE_ONLY schedule requires date/timezone and no instant")
        elif self.schedule_status is ScheduleStatus.SCHEDULED:
            raise ValueError("SCHEDULED requires known time precision")

    def payload(self) -> dict[str, object]:
        return {
            "schedule_status": self.schedule_status,
            "scheduled_date": self.scheduled_date,
            "scheduled_at": self.scheduled_at,
            "schedule_timezone": self.schedule_timezone,
            "time_precision": self.time_precision,
        }

    @property
    def fingerprint(self) -> str:
        return material_fingerprint(self.payload())


@dataclass(frozen=True)
class ObservationMaterial:
    observation_code: str
    assertion_state: ObservationState
    normalized_value: Decimal | None

    def __post_init__(self) -> None:
        if not self.observation_code or self.observation_code != self.observation_code.strip():
            raise ValueError("observation_code must be a non-empty canonical token")
        if self.assertion_state is ObservationState.VALUE:
            if self.normalized_value is None:
                raise ValueError("VALUE requires normalized_value")
            if not isinstance(self.normalized_value, Decimal):
                raise TypeError("normalized_value must be Decimal")
            if not self.normalized_value.is_finite():
                raise ValueError("normalized_value must be finite")
        elif self.normalized_value is not None:
            raise ValueError("EXPLICIT_UNAVAILABLE must not carry a numeric value")

    def payload(self) -> dict[str, object]:
        return {
            "observation_code": self.observation_code,
            "assertion_state": self.assertion_state,
            "normalized_value": self.normalized_value,
        }

    @property
    def fingerprint(self) -> str:
        return material_fingerprint(self.payload())
