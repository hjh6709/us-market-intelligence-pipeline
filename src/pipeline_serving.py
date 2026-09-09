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


class PageMetadata(BaseModel):
    limit: int
    offset: int
    total: int
    has_more: bool


class PipelineRunDetailView(BaseModel):
    run: PipelineRunSummary
    config: dict[str, Any]
    config_hash: str
    data_cutoff: datetime
    code_version: str
    work_items: list[PipelineWorkItemView]
    checks: list[PipelineCheckView]
    work_items_page: PageMetadata
    checks_page: PageMetadata


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

    def get_run(self, pipeline_run_id: str, *, limit=100, work_offset=0, check_offset=0):
        records = self._list_one(pipeline_run_id)
        record = records[0] if records else None
        if record is None:
            return None
        metadata = self.get_run_metadata(pipeline_run_id)
        if metadata is None:
            return None
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute("""SELECT
                (SELECT count(*) FROM pipeline_work_items WHERE pipeline_run_id=%s),
                (SELECT count(*) FROM pipeline_run_checks WHERE pipeline_run_id=%s)""",
                (pipeline_run_id, pipeline_run_id))
            totals = cursor.fetchone()
        return (record, metadata,
                self.list_work_items(pipeline_run_id, limit=limit, offset=work_offset),
                self.list_checks(pipeline_run_id, limit=limit, offset=check_offset), totals)

    def _list_one(self, pipeline_run_id: str) -> list[PipelineRunRecord]:
        # Reuse the aggregate query while keeping the public filters intentionally small.
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT r.pipeline_run_id, r.dag_id, r.status, r.started_at, r.finished_at,
                       (SELECT count(*) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id),
                       (SELECT count(*) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id AND status='FAILED'),
                       (SELECT count(*) FROM pipeline_run_checks WHERE pipeline_run_id=r.pipeline_run_id AND status='WARN'),
                       (SELECT count(*) FROM pipeline_run_checks WHERE pipeline_run_id=r.pipeline_run_id AND status='FAIL'),
                       (SELECT count(*) FROM pipeline_run_checks WHERE pipeline_run_id=r.pipeline_run_id AND alert_status='OPEN'),
                       (SELECT count(*) FROM pipeline_run_checks WHERE pipeline_run_id=r.pipeline_run_id AND check_name='observed_bar_coverage'),
                       (SELECT sum(input_count) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id),
                       (SELECT sum(output_count) FROM pipeline_work_items WHERE pipeline_run_id=r.pipeline_run_id)
                FROM pipeline_runs r WHERE r.pipeline_run_id = %s
                """,
                (pipeline_run_id,),
            )
            return [PipelineRunRecord(*row) for row in cursor.fetchall()]

    def list_work_items(self, pipeline_run_id: str, *, limit=100, offset=0) -> list[PipelineWorkItemView]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT economic_event_id, symbol, stage, status, attempt_count,
                          input_count, output_count, error_code, error_message, updated_at
                   FROM pipeline_work_items WHERE pipeline_run_id = %s
                   ORDER BY economic_event_id, symbol, stage LIMIT %s OFFSET %s""",
                (pipeline_run_id, limit, offset),
            )
            return [PipelineWorkItemView(**dict(zip(PipelineWorkItemView.model_fields, row))) for row in cursor.fetchall()]

    def list_checks(self, pipeline_run_id: str, *, limit=100, offset=0) -> list[PipelineCheckView]:
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT economic_event_id, symbol, stage, check_name, expected_value,
                          actual_value, status, alert_status, checked_at
                   FROM pipeline_run_checks WHERE pipeline_run_id = %s
                   ORDER BY economic_event_id, symbol, stage, check_name LIMIT %s OFFSET %s""",
                (pipeline_run_id, limit, offset),
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

    def detail(self, pipeline_run_id: str, *, limit=100, work_offset=0, check_offset=0) -> PipelineRunDetailView:
        if not 1 <= limit <= 500 or work_offset < 0 or check_offset < 0:
            raise ValueError("invalid detail pagination")
        result = self.repository.get_run(pipeline_run_id, limit=limit,
                                         work_offset=work_offset, check_offset=check_offset)
        if result is None:
            raise PipelineNotFoundError(pipeline_run_id)
        record, metadata, work_items, checks, totals = result
        config, config_hash, data_cutoff, code_version = metadata
        return PipelineRunDetailView(
            run=self._summary(record),
            config=config,
            config_hash=config_hash,
            data_cutoff=data_cutoff,
            code_version=code_version,
            work_items=work_items,
            checks=checks,
            work_items_page=PageMetadata(limit=limit, offset=work_offset, total=totals[0], has_more=work_offset+len(work_items)<totals[0]),
            checks_page=PageMetadata(limit=limit, offset=check_offset, total=totals[1], has_more=check_offset+len(checks)<totals[1]),
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
            LineageNode(id="airflow", label="Airflow · 수집 작업 조정", plane="orchestration"),
            LineageNode(id="run_tracking", label="pipeline_runs / work_items / checks", plane="operations"),
            LineageNode(id="pipelines_ui", label="Pipelines UI · 실행 감사", plane="operations"),
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
            LineageEdge(source="official_releases", target="airflow", label="공식 발표별 범위"),
            LineageEdge(source="airflow", target="market_bars", label="제공자 봉 수집 · 저장"),
            LineageEdge(source="airflow", target="macro_context", label="시점 보존 거시 환경"),
            LineageEdge(source="airflow", target="run_tracking", label="실행 · 작업 · 품질 기록"),
            LineageEdge(source="run_tracking", target="pipelines_ui", label="읽기 전용 조회"),
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
