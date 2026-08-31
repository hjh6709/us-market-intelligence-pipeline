"""Build point-in-time macro context for official economic events."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

import psycopg

from src.cpi_ingestion import CpiRelease
from src.fred_client import FredClient, MacroObservation


@dataclass(frozen=True)
class MacroSeriesSpec:
    series_id: str
    title: str
    frequency: str
    units: str
    seasonal_adjustment: str | None
    lookback_days: int


@dataclass(frozen=True)
class EventMacroContext:
    economic_event_id: str
    series_id: str
    observation_date: date
    realtime_start: date
    realtime_end: date
    value: Decimal | None


MACRO_SERIES = {
    "CPIAUCSL": MacroSeriesSpec(
        "CPIAUCSL", "Consumer Price Index", "Monthly", "Index", "Seasonally Adjusted", 400
    ),
    "CPILFESL": MacroSeriesSpec(
        "CPILFESL", "Core Consumer Price Index", "Monthly", "Index", "Seasonally Adjusted", 400
    ),
    "PCEPI": MacroSeriesSpec(
        "PCEPI", "Personal Consumption Expenditures Price Index", "Monthly", "Index", "Seasonally Adjusted", 400
    ),
    "PCEPILFE": MacroSeriesSpec(
        "PCEPILFE", "Core PCE Price Index", "Monthly", "Index", "Seasonally Adjusted", 400
    ),
    "UNRATE": MacroSeriesSpec(
        "UNRATE", "Unemployment Rate", "Monthly", "Percent", "Seasonally Adjusted", 400
    ),
    "PAYEMS": MacroSeriesSpec(
        "PAYEMS", "All Employees, Total Nonfarm", "Monthly", "Thousands of Persons", "Seasonally Adjusted", 400
    ),
    "DFF": MacroSeriesSpec(
        "DFF", "Effective Federal Funds Rate", "Daily", "Percent", None, 14
    ),
    "DGS2": MacroSeriesSpec(
        "DGS2", "2-Year Treasury Constant Maturity Rate", "Daily", "Percent", None, 14
    ),
    "DGS10": MacroSeriesSpec(
        "DGS10", "10-Year Treasury Constant Maturity Rate", "Daily", "Percent", None, 14
    ),
    "VIXCLS": MacroSeriesSpec(
        "VIXCLS", "CBOE Volatility Index", "Daily", "Index", None, 14
    ),
}


def select_latest_available(
    observations: Sequence[MacroObservation],
    *,
    as_of: date,
) -> MacroObservation:
    """Return the latest observation whose vintage was valid on ``as_of``."""
    candidates = [
        observation
        for observation in observations
        if observation.observation_date <= as_of
        and observation.is_valid_on(as_of)
        and observation.value is not None
    ]
    if not candidates:
        raise ValueError("no point-in-time observation was available")
    return max(candidates, key=lambda observation: observation.observation_date)


def fetch_event_macro_context(
    client: FredClient,
    releases: Sequence[CpiRelease],
    *,
    series: dict[str, MacroSeriesSpec] = MACRO_SERIES,
) -> list[EventMacroContext]:
    contexts: list[EventMacroContext] = []
    for release in releases:
        as_of = release.release_date
        for spec in series.values():
            observations = client.fetch_observations(
                series_id=spec.series_id,
                observation_start=as_of - timedelta(days=spec.lookback_days),
                observation_end=as_of,
                vintage_dates=[as_of],
            )
            selected = select_latest_available(observations, as_of=as_of)
            contexts.append(
                EventMacroContext(
                    economic_event_id=release.event_id,
                    series_id=selected.series_id,
                    observation_date=selected.observation_date,
                    realtime_start=selected.realtime_start,
                    realtime_end=selected.realtime_end,
                    value=selected.value,
                )
            )
    return contexts


def upsert_event_macro_context(
    contexts: Sequence[EventMacroContext],
    *,
    database_url: str,
) -> int:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO macro_event_contexts (
                    economic_event_id, series_id, observation_date,
                    realtime_start, realtime_end, value
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (economic_event_id, series_id) DO UPDATE SET
                    observation_date = EXCLUDED.observation_date,
                    realtime_start = EXCLUDED.realtime_start,
                    realtime_end = EXCLUDED.realtime_end,
                    value = EXCLUDED.value,
                    ingested_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        item.economic_event_id,
                        item.series_id,
                        item.observation_date,
                        item.realtime_start,
                        item.realtime_end,
                        item.value,
                    )
                    for item in contexts
                ],
            )
    return len(contexts)
