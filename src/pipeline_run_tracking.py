"""Persist auditable pipeline run, work-item, and quality-check state."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import psycopg


@dataclass(frozen=True)
class PipelineRun:
    pipeline_run_id: str
    dag_id: str
    config: Mapping[str, Any]
    config_hash: str
    data_cutoff: datetime
    code_version: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None


@dataclass(frozen=True)
class PipelineWorkItem:
    pipeline_run_id: str
    economic_event_id: str
    symbol: str
    stage: str
    status: str
    attempt_count: int
    manifest_path: str | None = None
    input_count: int | None = None
    output_count: int | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineCheck:
    pipeline_run_id: str
    economic_event_id: str
    symbol: str
    stage: str
    check_name: str
    expected_value: str | None
    actual_value: str | None
    status: str
    alert_status: str
    checked_at: datetime


def canonical_config_hash(config: Mapping[str, Any]) -> str:
    """Return a stable SHA-256 digest for a JSON-compatible run config."""

    canonical = json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_DATABASE_URL = re.compile(r"postgres(?:ql)?://[^\s]+", re.IGNORECASE)
_SECRET_QUERY = re.compile(
    r"(?P<name>api[_-]?key|access[_-]?token|secret|password)=(?P<value>[^&\s]+)",
    re.IGNORECASE,
)


def redact_error_message(message: str, *, max_length: int = 500) -> str:
    """Remove common credentials before an exception is persisted."""

    redacted = _DATABASE_URL.sub("[database-url-redacted]", message)
    redacted = _SECRET_QUERY.sub(
        lambda match: f"{match.group('name')}=[redacted]", redacted
    )
    return redacted[:max_length]


def start_pipeline_run(database_url: str, run: PipelineRun) -> None:
    """Insert a run once; the run id identifies one Airflow execution."""

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline_run_id, dag_id, config_json, config_hash,
                    data_cutoff, code_version, status, started_at, finished_at
                ) VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id) DO NOTHING
                """,
                (
                    run.pipeline_run_id,
                    run.dag_id,
                    json.dumps(run.config, default=str),
                    run.config_hash,
                    run.data_cutoff,
                    run.code_version,
                    run.status,
                    run.started_at,
                    run.finished_at,
                ),
            )


def mark_work_item(database_url: str, item: PipelineWorkItem) -> None:
    """Upsert the latest state of one event-symbol-stage work item."""

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_work_items (
                    pipeline_run_id, economic_event_id, symbol, stage, status,
                    attempt_count, manifest_path, input_count, output_count,
                    error_code, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, economic_event_id, symbol, stage)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    attempt_count = EXCLUDED.attempt_count,
                    manifest_path = EXCLUDED.manifest_path,
                    input_count = EXCLUDED.input_count,
                    output_count = EXCLUDED.output_count,
                    error_code = EXCLUDED.error_code,
                    error_message = EXCLUDED.error_message,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    item.pipeline_run_id,
                    item.economic_event_id,
                    item.symbol,
                    item.stage,
                    item.status,
                    item.attempt_count,
                    item.manifest_path,
                    item.input_count,
                    item.output_count,
                    item.error_code,
                    None
                    if item.error_message is None
                    else redact_error_message(item.error_message),
                ),
            )


def record_pipeline_check(database_url: str, check: PipelineCheck) -> None:
    """Upsert a quality check so a later successful retry can resolve it."""

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_run_checks (
                    pipeline_run_id, economic_event_id, symbol, stage,
                    check_name, expected_value, actual_value, status,
                    alert_status, checked_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    pipeline_run_id, economic_event_id, symbol, stage, check_name
                ) DO UPDATE SET
                    expected_value = EXCLUDED.expected_value,
                    actual_value = EXCLUDED.actual_value,
                    status = EXCLUDED.status,
                    alert_status = EXCLUDED.alert_status,
                    checked_at = EXCLUDED.checked_at
                """,
                (
                    check.pipeline_run_id,
                    check.economic_event_id,
                    check.symbol,
                    check.stage,
                    check.check_name,
                    check.expected_value,
                    check.actual_value,
                    check.status,
                    check.alert_status,
                    check.checked_at,
                ),
            )


def finish_pipeline_run(
    database_url: str,
    pipeline_run_id: str,
    status: str,
    finished_at: datetime,
) -> None:
    """Mark a run terminal without changing its immutable input config."""

    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, finished_at = %s
                WHERE pipeline_run_id = %s
                """,
                (status, finished_at, pipeline_run_id),
            )
