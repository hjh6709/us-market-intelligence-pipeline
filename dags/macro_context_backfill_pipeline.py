"""Collect point-in-time FRED/ALFRED context for confirmed economic events."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
import os
from pathlib import Path

import psycopg
from airflow.sdk import Param, dag, get_current_context, task

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.economic_event_ingestion import upsert_economic_events
from src.economic_event_schedule import EconomicRelease, load_event_catalog
from src.macro_context_ingestion import (
    MACRO_SERIES,
    fetch_event_macro_context,
    upsert_event_macro_context,
)


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def _restore_release(values: dict) -> EconomicRelease:
    values = dict(values)
    values["release_date"] = date.fromisoformat(values["release_date"])
    values["released_at"] = datetime.fromisoformat(
        values["released_at"].replace("Z", "+00:00")
    )
    return EconomicRelease(**values)


@dag(
    dag_id="macro_context_backfill_pipeline",
    description="Attach point-in-time FRED/ALFRED indicators to official releases",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    params={
        "event_types": Param(
            ["CPI", "EMPLOYMENT", "PCE", "FOMC"],
            type="array",
            items={"type": "string"},
            minItems=1,
            uniqueItems=True,
        ),
        "release_from": Param("2022-01-01", type="string", format="date"),
        "release_to": Param("2026-08-26", type="string", format="date"),
        "series": Param(
            sorted(MACRO_SERIES),
            type="array",
            items={"type": "string"},
            minItems=1,
            uniqueItems=True,
        ),
        "force_refresh": Param(False, type="boolean"),
    },
    tags=["macro", "economic-events", "fred", "alfred", "backfill"],
)
def build_macro_context_backfill_pipeline():
    @task(task_id="build_event_work_items")
    def build_event_work_items() -> list[dict]:
        params = get_current_context()["params"]
        requested_types = {str(value).upper() for value in params["event_types"]}
        unknown_series = set(params["series"]) - set(MACRO_SERIES)
        if unknown_series:
            raise ValueError(f"unknown FRED series: {sorted(unknown_series)}")
        release_from = date.fromisoformat(params["release_from"])
        release_to = date.fromisoformat(params["release_to"])
        if release_to < release_from:
            raise ValueError("release_to must not be before release_from")
        releases = [
            release
            for release in load_event_catalog()
            if release.event_type in requested_types
            and release_from <= release.release_date <= release_to
        ]
        if not releases:
            raise ValueError("no confirmed economic release matches the parameters")
        work = []
        for release in releases:
            values = asdict(release)
            values["release_date"] = release.release_date.isoformat()
            values["released_at"] = release.released_at.isoformat()
            values["series"] = list(params["series"])
            values["force_refresh"] = bool(params["force_refresh"])
            work.append(values)
        return work

    @task(
        task_id="collect_event_macro_context",
        pool="fred_api_pool",
        max_active_tis_per_dag=1,
    )
    def collect_event_macro_context(values: dict) -> dict:
        from src.cpi_ingestion import _settings
        from src.fred_client import FredClient

        release_values = dict(values)
        series_ids = release_values.pop("series")
        force_refresh = release_values.pop("force_refresh")
        release = _restore_release(release_values)
        database_url = _database_url()
        if not force_refresh:
            with psycopg.connect(database_url, connect_timeout=5) as connection:
                existing = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM macro_event_contexts
                    WHERE economic_event_id = %s AND series_id = ANY(%s)
                    """,
                    (release.event_id, series_ids),
                ).fetchone()[0]
            if existing == len(series_ids):
                return {
                    "event_id": release.event_id,
                    "event_type": release.event_type,
                    "stored_contexts": 0,
                    "reused_contexts": existing,
                }
        fred_api_key, database_url = _settings(os.environ, Path(".env"))
        selected = {series_id: MACRO_SERIES[series_id] for series_id in series_ids}
        contexts = fetch_event_macro_context(
            FredClient(fred_api_key, timeout_seconds=30.0),
            [release],
            series=selected,
            request_interval_seconds=0.55,
        )
        upsert_economic_events([release], database_url=database_url)
        stored = upsert_event_macro_context(contexts, database_url=database_url)
        return {
            "event_id": release.event_id,
            "event_type": release.event_type,
            "stored_contexts": stored,
            "reused_contexts": 0,
        }

    @task(task_id="verify_macro_context")
    def verify_macro_context(results: list[dict]) -> dict:
        expected = len(results) * len(get_current_context()["params"]["series"])
        accounted = sum(
            item["stored_contexts"] + item["reused_contexts"] for item in results
        )
        if accounted != expected:
            raise RuntimeError(
                f"macro context verification failed: expected={expected}, "
                f"accounted={accounted}"
            )
        return {
            "events": len(results),
            "expected_contexts": expected,
            "stored_contexts": sum(item["stored_contexts"] for item in results),
            "reused_contexts": sum(item["reused_contexts"] for item in results),
        }

    results = collect_event_macro_context.expand(values=build_event_work_items())
    verify_macro_context(results)


macro_context_backfill_pipeline = build_macro_context_backfill_pipeline()
