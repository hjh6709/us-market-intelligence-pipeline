"""Validated value objects for FRED series metadata and observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


FRED_SERIES: tuple[str, ...] = (
    "CPIAUCSL",
    "CPILFESL",
    "PCEPI",
    "PCEPILFE",
    "UNRATE",
    "DFF",
    "DGS2",
    "DGS10",
    "VIXCLS",
)


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _date(payload: Mapping[str, Any], field: str) -> date:
    value = _required_text(payload, field)
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO date") from error


def _aware_datetime(payload: Mapping[str, Any], field: str) -> datetime:
    value = _required_text(payload, field)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO datetime") from error
    return _require_aware(parsed, field)


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _supported_series(series_id: str) -> str:
    if series_id not in FRED_SERIES:
        raise ValueError(f"unsupported FRED series: {series_id}")
    return series_id


@dataclass(frozen=True, slots=True)
class MacroSeries:
    series_id: str
    title: str
    frequency: str
    units: str
    seasonal_adjustment: str
    observation_start: date
    observation_end: date
    last_updated: datetime
    notes: str | None
    source: str
    ingested_at: datetime

    @classmethod
    def from_fred(
        cls,
        payload: Mapping[str, Any],
        ingested_at: datetime,
    ) -> MacroSeries:
        observation_start = _date(payload, "observation_start")
        observation_end = _date(payload, "observation_end")
        if observation_start > observation_end:
            raise ValueError("observation_start must not be after observation_end")
        notes = payload.get("notes")
        if notes is not None and not isinstance(notes, str):
            raise ValueError("notes must be a string or null")

        return cls(
            series_id=_supported_series(_required_text(payload, "id")),
            title=_required_text(payload, "title"),
            frequency=_required_text(payload, "frequency"),
            units=_required_text(payload, "units"),
            seasonal_adjustment=_required_text(payload, "seasonal_adjustment"),
            observation_start=observation_start,
            observation_end=observation_end,
            last_updated=_aware_datetime(payload, "last_updated"),
            notes=notes,
            source="fred",
            ingested_at=_require_aware(ingested_at, "ingested_at"),
        )


@dataclass(frozen=True, slots=True)
class MacroObservation:
    series_id: str
    observation_date: date
    value: Decimal | None
    realtime_start: date
    realtime_end: date
    source: str
    ingested_at: datetime

    @classmethod
    def from_fred(
        cls,
        series_id: str,
        payload: Mapping[str, Any],
        ingested_at: datetime,
    ) -> MacroObservation:
        realtime_start = _date(payload, "realtime_start")
        realtime_end = _date(payload, "realtime_end")
        if realtime_start > realtime_end:
            raise ValueError("realtime_start must not be after realtime_end")

        raw_value = _required_text(payload, "value")
        value = None
        if raw_value != ".":
            try:
                value = Decimal(raw_value)
            except InvalidOperation as error:
                raise ValueError("value must be a numeric value or '.'") from error
            if not value.is_finite():
                raise ValueError("value must be a finite numeric value")

        return cls(
            series_id=_supported_series(series_id),
            observation_date=_date(payload, "date"),
            value=value,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            source="fred",
            ingested_at=_require_aware(ingested_at, "ingested_at"),
        )
