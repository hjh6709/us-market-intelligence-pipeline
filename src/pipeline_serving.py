"""Read-only product views over durable pipeline audit tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from pydantic import BaseModel, Field


@dataclass(frozen=True)
class PipelineRunRecord:
    pipeline_run_id: str
    dag_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    work_item_count: int
    failed_item_count: int
    warning_check_count: int
    failed_check_count: int
    open_alert_count: int
    observed_coverage_check_count: int
    input_count: int | None
    output_count: int | None


class PipelineRunSummary(BaseModel):
    pipeline_run_id: str
    dag_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_seconds: float | None
    work_item_count: int = Field(ge=0)
    failed_item_count: int = Field(ge=0)
    warning_check_count: int = Field(ge=0)
    failed_check_count: int = Field(ge=0)
    open_alert_count: int = Field(ge=0)
    observed_coverage_check_count: int = Field(ge=0)
    quality_contract: str
    input_count: int | None = Field(default=None, ge=0)
    output_count: int | None = Field(default=None, ge=0)


class PipelineOverviewView(BaseModel):
    latest_run: PipelineRunSummary | None
    recent_runs: list[PipelineRunSummary]


class PipelineWorkItemView(BaseModel):
    economic_event_id: str
    symbol: str
    stage: str
    status: str
    attempt_count: int
    input_count: int | None
    output_count: int | None
    error_code: str | None
    error_message: str | None
    updated_at: datetime


class PipelineCheckView(BaseModel):
    economic_event_id: str
    symbol: str
    stage: str
    check_name: str
    expected_value: str | None
    actual_value: str | None
    status: str
    alert_status: str
    checked_at: datetime


class PipelineRunDetailView(BaseModel):
    run: PipelineRunSummary
    config: dict[str, Any]
    config_hash: str
    data_cutoff: datetime
    code_version: str
    work_items: list[PipelineWorkItemView]
    checks: list[PipelineCheckView]


class PipelineQualityGroup(BaseModel):
    check_name: str
    status: str
    count: int = Field(ge=0)


class PipelineQualityView(BaseModel):
    pipeline_run_id: str
    groups: list[PipelineQualityGroup]


class LineageNode(BaseModel):
    id: str
    label: str
    plane: str


class LineageEdge(BaseModel):
    source: str
    target: str
    label: str | None = None


class PipelineLineageView(BaseModel):
    scope: str = "project-level"
    nodes: list[LineageNode]
    edges: list[LineageEdge]


class PipelineNotFoundError(LookupError):
    pass


class PostgresPipelineRepository:
    def __init__(self, database_url: str, *, connect=psycopg.connect) -> None:
        self.database_url = database_url
        self._connect = connect

    def _connection(self):
        return self._connect(self.database_url, connect_timeout=5)

    def list_runs(
        self,
        *,
        dag_id: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[PipelineRunRecord]:
        filters: list[str] = []
        params: list[object] = []
        if dag_id is not None:
            filters.append("r.dag_id = %s")
            params.append(dag_id)
        if status is not None:
            filters.append("r.status = %s")
            params.append(status)
        where = "WHERE " + " AND ".join(filters) if filters else ""
        params.extend((limit, offset))
        sql = f"""
            WITH work AS (
                SELECT pipeline_run_id,
                       count(*) AS work_item_count,
                       count(*) FILTER (WHERE status = 'FAILED') AS failed_item_count,
                       sum(input_count) AS input_count,
                       sum(output_count) AS output_count
                FROM pipeline_work_items GROUP BY pipeline_run_id
            ), checks AS (
                SELECT pipeline_run_id,
                       count(*) FILTER (WHERE status = 'WARN') AS warning_check_count,
                       count(*) FILTER (WHERE status = 'FAIL') AS failed_check_count,
                       count(*) FILTER (WHERE alert_status = 'OPEN') AS open_alert_count
                       ,count(*) FILTER (WHERE check_name = 'observed_bar_coverage')
                           AS observed_coverage_check_count
                FROM pipeline_run_checks GROUP BY pipeline_run_id
            )
            SELECT r.pipeline_run_id, r.dag_id, r.status, r.started_at, r.finished_at,
                   coalesce(work.work_item_count, 0),
                   coalesce(work.failed_item_count, 0),
                   coalesce(checks.warning_check_count, 0),
                   coalesce(checks.failed_check_count, 0),
                   coalesce(checks.open_alert_count, 0),
                   coalesce(checks.observed_coverage_check_count, 0),
                   work.input_count, work.output_count
            FROM pipeline_runs AS r
            LEFT JOIN work USING (pipeline_run_id)
            LEFT JOIN checks USING (pipeline_run_id)
            {where}
            ORDER BY r.started_at DESC, r.pipeline_run_id DESC
            LIMIT %s OFFSET %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return [PipelineRunRecord(*row) for row in cursor.fetchall()]

    def get_run_metadata(self, pipeline_run_id: str):
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT config_json, config_hash, data_cutoff, code_version
                   FROM pipeline_runs WHERE pipeline_run_id = %s""",
                (pipeline_run_id,),
            )
            return cursor.fetchone()

    def get_run(self, pipeline_run_id: str):
        records = self.list_runs(limit=1)
        record = next(
            (item for item in records if item.pipeline_run_id == pipeline_run_id), None
        )
        if record is None:
            # The newest run may not be the requested run; use a bounded direct query.
            records = self._list_one(pipeline_run_id)
            record = records[0] if records else None
        if record is None:
            return None
        metadata = self.get_run_metadata(pipeline_run_id)
        if metadata is None:
            return None
        return record, metadata, self.list_work_items(pipeline_run_id), self.list_checks(pipeline_run_id)

    def _list_one(self, pipeline_run_id: str) -> list[PipelineRunRecord]:
        # Reuse the aggregate query while keeping the public filters intentionally small.
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT r.pipeline_run_id, r.dag_id, r.status, r.started_at, r.finished_at,
                       count(DISTINCT (w.economic_event_id, w.symbol, w.stage)),
                       count(DISTINCT (w.economic_event_id, w.symbol, w.stage))
                           FILTER (WHERE w.status = 'FAILED'),
                       count(DISTINCT (c.economic_event_id, c.symbol, c.stage, c.check_name))
                           FILTER (WHERE c.status = 'WARN'),
                       count(DISTINCT (c.economic_event_id, c.symbol, c.stage, c.check_name))
                           FILTER (WHERE c.status = 'FAIL'),
                       count(DISTINCT (c.economic_event_id, c.symbol, c.stage, c.check_name))
                           FILTER (WHERE c.alert_status = 'OPEN'),
                       count(DISTINCT (c.economic_event_id, c.symbol, c.stage, c.check_name))
                           FILTER (WHERE c.check_name = 'observed_bar_coverage'),
                       (SELECT sum(input_count) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id),
                       (SELECT sum(output_count) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id)
                FROM pipeline_runs r
                LEFT JOIN pipeline_work_items w USING (pipeline_run_id)
                LEFT JOIN pipeline_run_checks c USING (pipeline_run_id)
                WHERE r.pipeline_run_id = %s
                GROUP BY r.pipeline_run_id
                """,
                (pipeline_run_id,),
            )
            return [PipelineRunRecord(*row) for row in cursor.fetchall()]

    def list_work_items(self, pipeline_run_id: str) -> list[PipelineWorkItemView]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT economic_event_id, symbol, stage, status, attempt_count,
                          input_count, output_count, error_code, error_message, updated_at
                   FROM pipeline_work_items WHERE pipeline_run_id = %s
                   ORDER BY economic_event_id, symbol, stage LIMIT 500""",
                (pipeline_run_id,),
            )
            return [PipelineWorkItemView(**dict(zip(PipelineWorkItemView.model_fields, row))) for row in cursor.fetchall()]

    def list_checks(self, pipeline_run_id: str) -> list[PipelineCheckView]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT economic_event_id, symbol, stage, check_name, expected_value,
                          actual_value, status, alert_status, checked_at
                   FROM pipeline_run_checks WHERE pipeline_run_id = %s
                   ORDER BY economic_event_id, symbol, stage, check_name LIMIT 1000""",
                (pipeline_run_id,),
            )
            return [PipelineCheckView(**dict(zip(PipelineCheckView.model_fields, row))) for row in cursor.fetchall()]

    def quality_groups(self, pipeline_run_id: str) -> list[PipelineQualityGroup]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT check_name, status, count(*)
                   FROM pipeline_run_checks WHERE pipeline_run_id = %s
                   GROUP BY check_name, status ORDER BY check_name, status""",
                (pipeline_run_id,),
            )
            return [PipelineQualityGroup(check_name=row[0], status=row[1], count=row[2]) for row in cursor.fetchall()]


class PipelineServingService:
    def __init__(self, repository: PostgresPipelineRepository) -> None:
        self.repository = repository

    @staticmethod
    def _summary(record: PipelineRunRecord) -> PipelineRunSummary:
        duration = None
        if record.finished_at is not None:
            duration = (record.finished_at - record.started_at).total_seconds()
        quality_contract = (
            "collection-coverage-separated"
            if record.observed_coverage_check_count
            else "legacy-pre-separation"
        )
        return PipelineRunSummary(
            **record.__dict__,
            duration_seconds=duration,
            quality_contract=quality_contract,
        )

    def overview(self) -> PipelineOverviewView:
        records = self.repository.list_runs(limit=5, offset=0)
        summaries = [self._summary(item) for item in records]
        return PipelineOverviewView(
            latest_run=summaries[0] if summaries else None,
            recent_runs=summaries,
        )

    def list_runs(self, **filters) -> list[PipelineRunSummary]:
        return [self._summary(item) for item in self.repository.list_runs(**filters)]

    def detail(self, pipeline_run_id: str) -> PipelineRunDetailView:
        result = self.repository.get_run(pipeline_run_id)
        if result is None:
            raise PipelineNotFoundError(pipeline_run_id)
        record, metadata, work_items, checks = result
        config, config_hash, data_cutoff, code_version = metadata
        return PipelineRunDetailView(
            run=self._summary(record),
            config=config,
            config_hash=config_hash,
            data_cutoff=data_cutoff,
            code_version=code_version,
            work_items=work_items,
            checks=checks,
        )

    def quality(self, pipeline_run_id: str) -> PipelineQualityView:
        if self.repository.get_run_metadata(pipeline_run_id) is None:
            raise PipelineNotFoundError(pipeline_run_id)
        return PipelineQualityView(
            pipeline_run_id=pipeline_run_id,
            groups=self.repository.quality_groups(pipeline_run_id),
        )

    def lineage(self) -> PipelineLineageView:
        nodes = [
            LineageNode(id="official_releases", label="Official economic releases", plane="data"),
            LineageNode(id="alpaca_bars", label="Alpaca SIP bars", plane="data"),
            LineageNode(id="fred_alfred", label="FRED / ALFRED", plane="data"),
            LineageNode(id="market_bars", label="PostgreSQL market_bars", plane="storage"),
            LineageNode(id="macro_context", label="PIT macro contexts", plane="storage"),
            LineageNode(id="impacts", label="Event impacts", plane="analytics"),
            LineageNode(id="strategy", label="Research baseline", plane="analytics"),
            LineageNode(id="serving", label="Serving API / Research UI", plane="serving"),
            LineageNode(id="archived_sip_trades", label="Archived SIP trades", plane="validation"),
            LineageNode(id="kafka", label="Kafka", plane="validation"),
            LineageNode(id="spark", label="Spark", plane="validation"),
        ]
        edges = [
            LineageEdge(source="official_releases", target="market_bars", label="Airflow bounds"),
            LineageEdge(source="alpaca_bars", target="market_bars"),
            LineageEdge(source="official_releases", target="macro_context"),
            LineageEdge(source="fred_alfred", target="macro_context"),
            LineageEdge(source="market_bars", target="impacts"),
            LineageEdge(source="macro_context", target="serving"),
            LineageEdge(source="impacts", target="strategy"),
            LineageEdge(source="impacts", target="serving"),
            LineageEdge(source="strategy", target="serving"),
            LineageEdge(source="archived_sip_trades", target="kafka"),
            LineageEdge(source="kafka", target="spark"),
            LineageEdge(source="spark", target="market_bars", label="bounded validation path"),
        ]
        return PipelineLineageView(nodes=nodes, edges=edges)
