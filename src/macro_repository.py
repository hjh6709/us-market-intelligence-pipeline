"""Transactional PostgreSQL repository for FRED metadata and vintages."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import psycopg

from src.macro_models import MacroObservation, MacroSeries


UPSERT_MACRO_SERIES_SQL = """
INSERT INTO macro_series (
    series_id, title, frequency, units, seasonal_adjustment,
    observation_start, observation_end, last_updated, notes,
    source, ingested_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (series_id)
DO UPDATE SET
    title = EXCLUDED.title,
    frequency = EXCLUDED.frequency,
    units = EXCLUDED.units,
    seasonal_adjustment = EXCLUDED.seasonal_adjustment,
    observation_start = EXCLUDED.observation_start,
    observation_end = EXCLUDED.observation_end,
    last_updated = EXCLUDED.last_updated,
    notes = EXCLUDED.notes,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at
"""

UPSERT_MACRO_OBSERVATION_SQL = """
INSERT INTO macro_observations (
    series_id, observation_date, value, realtime_start,
    realtime_end, source, ingested_at
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (series_id, observation_date, realtime_start)
DO UPDATE SET
    value = EXCLUDED.value,
    realtime_end = EXCLUDED.realtime_end,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at
"""


@dataclass(frozen=True, slots=True)
class MacroUpsertResult:
    series_id: str
    observation_count: int
    missing_count: int


@dataclass(frozen=True, slots=True)
class MacroQualityResult:
    expected_series: tuple[str, ...]
    series_count: int
    observation_count: int
    missing_count: int
    missing_series: tuple[str, ...]


def macro_series_row(series: MacroSeries) -> tuple:
    return (
        series.series_id,
        series.title,
        series.frequency,
        series.units,
        series.seasonal_adjustment,
        series.observation_start,
        series.observation_end,
        series.last_updated,
        series.notes,
        series.source,
        series.ingested_at,
    )


def macro_observation_rows(
    series: MacroSeries,
    observations: Iterable[MacroObservation],
) -> list[tuple]:
    rows = []
    for observation in observations:
        if observation.series_id != series.series_id:
            raise ValueError("all observations must belong to the same series")
        rows.append(
            (
                observation.series_id,
                observation.observation_date,
                observation.value,
                observation.realtime_start,
                observation.realtime_end,
                observation.source,
                observation.ingested_at,
            )
        )
    return rows


def upsert_macro_batch(
    series: MacroSeries,
    observations: Iterable[MacroObservation],
    *,
    database_url: str,
) -> MacroUpsertResult:
    """Atomically upsert one series and all observations for its window."""
    rows = macro_observation_rows(series, observations)
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(UPSERT_MACRO_SERIES_SQL, macro_series_row(series))
            if rows:
                cursor.executemany(UPSERT_MACRO_OBSERVATION_SQL, rows)
    return MacroUpsertResult(
        series_id=series.series_id,
        observation_count=len(rows),
        missing_count=sum(row[2] is None for row in rows),
    )


def read_macro_quality(
    *,
    database_url: str,
    expected_series: Sequence[str],
) -> MacroQualityResult:
    """Return small count-only quality evidence for the configured series."""
    expected = tuple(dict.fromkeys(expected_series))
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(
                """
                SELECT series_id
                FROM macro_series
                WHERE series_id = ANY(%s)
                ORDER BY series_id
                """,
                (list(expected),),
            )
            present = tuple(row[0] for row in cursor.fetchall())
            cursor.execute(
                """
                SELECT count(*), count(*) FILTER (WHERE value IS NULL)
                FROM macro_observations
                WHERE series_id = ANY(%s)
                """,
                (list(expected),),
            )
            observation_count, missing_count = cursor.fetchone()

    present_set = set(present)
    return MacroQualityResult(
        expected_series=expected,
        series_count=len(present),
        observation_count=observation_count,
        missing_count=missing_count,
        missing_series=tuple(
            series_id for series_id in expected if series_id not in present_set
        ),
    )
