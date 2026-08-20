"""Airflow-independent orchestration for one FRED series ingestion."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any

from src.fred import FredClient, FredWindow
from src.macro_models import MacroObservation, MacroSeries
from src.macro_repository import MacroUpsertResult, upsert_macro_batch


WINDOW_FIELDS = (
    "realtime_start",
    "realtime_end",
    "observation_start",
    "observation_end",
)


@dataclass(frozen=True, slots=True)
class FredIngestionSummary:
    series_id: str
    raw_count: int
    normalized_count: int
    missing_count: int
    upserted_count: int
    observation_start: str
    observation_end: str

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


def resolve_fred_window(
    logical_date: date,
    conf: Mapping[str, str],
) -> FredWindow:
    """Resolve the overlapping daily window or an explicit manual backfill."""
    supplied = [field for field in WINDOW_FIELDS if field in conf]
    if supplied:
        if len(supplied) != len(WINDOW_FIELDS):
            raise ValueError("manual FRED window requires all four date fields")
        values = {field: _parse_date(conf[field], field) for field in WINDOW_FIELDS}
        window = FredWindow(**values)
        if window.observation_end > window.realtime_end:
            raise ValueError("observation_end must not be after realtime_end")
        return window

    return FredWindow(
        realtime_start=logical_date - timedelta(days=6),
        realtime_end=logical_date,
        observation_start=_years_before(logical_date, 2),
        observation_end=logical_date,
    )


def ingest_fred_series(
    series_id: str,
    window: FredWindow,
    *,
    client: FredClient,
    database_url: str,
    clock: Callable[[], datetime],
    repository: Callable[..., MacroUpsertResult] = upsert_macro_batch,
) -> FredIngestionSummary:
    """Fetch, validate and atomically persist one series without Airflow state."""
    ingested_at = clock()
    raw_series = client.fetch_series(series_id, as_of=window.realtime_end)
    raw_observations = client.fetch_observations(series_id, window)
    series = MacroSeries.from_fred(raw_series, ingested_at)
    observations = [
        MacroObservation.from_fred(series_id, row, ingested_at)
        for row in raw_observations
    ]
    result = repository(series, observations, database_url=database_url)
    if result.series_id != series_id:
        raise ValueError("repository returned a different series_id")

    return FredIngestionSummary(
        series_id=series_id,
        raw_count=len(raw_observations),
        normalized_count=len(observations),
        missing_count=result.missing_count,
        upserted_count=result.observation_count,
        observation_start=window.observation_start.isoformat(),
        observation_end=window.observation_end.isoformat(),
    )


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO date") from error


def _years_before(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)
