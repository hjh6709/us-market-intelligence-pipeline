"""Daily FRED/ALFRED vintage ingestion managed by Airflow 3."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from src.fred import FredClient, FredWindow
from src.fred_pipeline import ingest_fred_series, resolve_fred_window
from src.macro_models import FRED_SERIES
from src.macro_repository import read_macro_quality


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


@dag(
    dag_id="fred_macro_daily",
    schedule="0 14 * * *",
    start_date=datetime(2026, 8, 20, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=2),
    },
    tags=["macro", "fred"],
)
def fred_macro_daily():
    @task(task_id="resolve_window")
    def resolve_window_task() -> dict[str, str]:
        context = get_current_context()
        logical = context["logical_date"]
        dag_run = context.get("dag_run")
        conf = dict(dag_run.conf or {}) if dag_run is not None else {}
        window = resolve_fred_window(logical.date(), conf)
        return {
            "realtime_start": window.realtime_start.isoformat(),
            "realtime_end": window.realtime_end.isoformat(),
            "observation_start": window.observation_start.isoformat(),
            "observation_end": window.observation_end.isoformat(),
        }

    @task(task_id="ingest_series")
    def ingest_series_task(series_id: str, window_values: dict[str, str]) -> dict:
        window = FredWindow(
            **{name: date.fromisoformat(value) for name, value in window_values.items()}
        )
        summary = ingest_fred_series(
            series_id,
            window,
            client=FredClient(_required_environment("FRED_API_KEY")),
            database_url=_required_environment("DATABASE_URL"),
            clock=lambda: datetime.now(UTC),
        )
        return summary.as_dict()

    @task(task_id="quality_gate")
    def quality_gate_task(summaries) -> dict:
        rows = list(summaries)
        reported = [row["series_id"] for row in rows]
        if sorted(reported) != sorted(FRED_SERIES):
            raise ValueError("mapped ingestion did not report all configured series")
        quality = read_macro_quality(
            database_url=_required_environment("DATABASE_URL"),
            expected_series=FRED_SERIES,
        )
        if quality.missing_series or quality.series_count != len(FRED_SERIES):
            raise ValueError("PostgreSQL quality gate is missing configured series")
        return {
            "series_count": quality.series_count,
            "observation_count": quality.observation_count,
            "missing_count": quality.missing_count,
        }

    window_values = resolve_window_task()
    summaries = ingest_series_task.partial(window_values=window_values).expand(series_id=list(FRED_SERIES))
    return quality_gate_task(summaries)


fred_macro_daily()
