import os
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, Path as ApiPath, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.serving_models import (
    BarView,
    CrossAssetComparisonView,
    EventSummary,
    EventSymbolDetail,
    HistoricalComparisonView,
    StrategySummaryView,
)
from src.serving_repository import PostgresServingRepository
from src.serving_service import ServingNotFoundError, ServingService
from src.pipeline_serving import (
    PipelineLineageView,
    PipelineNotFoundError,
    PipelineOverviewView,
    PipelineQualityView,
    PipelineRunDetailView,
    PipelineRunSummary,
    PipelineServingService,
    PostgresPipelineRepository,
)
from src.paper_execution import BrokerUnavailable, OrderIntent
from src.paper_web import (
    ConfiguredPaperWebGateway,
    PaperConfigurationError,
    PaperConfirmationRequest,
    PaperConfirmationError,
    PaperOrderNotFoundError,
    PaperSubmitRequest,
)


EventType = Literal["CPI", "EMPLOYMENT", "PCE", "FOMC"]
Timeframe = Literal["1m", "3m", "5m"]
ImpactWindow = Literal["PRE_60M", "POST_5M", "POST_30M", "POST_60M"]
SymbolPath = Annotated[str, ApiPath(pattern=r"^[A-Z][A-Z0-9.]{0,9}$")]
TEMPLATE_PATH = Path(__file__).with_name("templates") / "dashboard.html"
PIPELINES_TEMPLATE_PATH = Path(__file__).with_name("templates") / "pipelines.html"
PAPER_TEMPLATE_PATH = Path(__file__).with_name("templates") / "paper.html"


def create_app(
    service: ServingService | None = None,
    pipeline_service: PipelineServingService | None = None,
    paper_service=None,
) -> FastAPI:
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    serving_service = service or ServingService(
        PostgresServingRepository(database_url)
    )
    pipeline_serving = pipeline_service or PipelineServingService(
        PostgresPipelineRepository(database_url)
    )
    paper_serving = paper_service or ConfiguredPaperWebGateway(database_url)
    app = FastAPI(
        title="U.S. Market Intelligence Serving API",
        version="1.0.0",
        description="Read-only economic-event research results. No broker order routes.",
    )
    app.mount(
        "/static",
        StaticFiles(directory=Path(__file__).with_name("static")),
        name="static",
    )

    @app.exception_handler(ServingNotFoundError)
    async def not_found_handler(
        _request: Request, error: ServingNotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"detail": f"{error.resource} not found"},
        )

    @app.exception_handler(PipelineNotFoundError)
    async def pipeline_not_found_handler(
        _request: Request, _error: PipelineNotFoundError
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "pipeline run not found"})

    @app.exception_handler(PaperConfirmationError)
    async def paper_confirmation_handler(
        _request: Request, error: PaperConfirmationError
    ) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(error)})

    @app.exception_handler(PaperConfigurationError)
    async def paper_configuration_handler(
        _request: Request, _error: PaperConfigurationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "paper sandbox is not configured"}
        )

    @app.exception_handler(BrokerUnavailable)
    async def broker_unavailable_handler(
        _request: Request, _error: BrokerUnavailable
    ) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "paper broker unavailable"})

    @app.exception_handler(PaperOrderNotFoundError)
    async def paper_order_not_found_handler(
        _request: Request, _error: PaperOrderNotFoundError
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "paper order not found"})

    @app.get("/health")
    def health() -> JSONResponse:
        healthy = serving_service.health()
        return JSONResponse(
            status_code=200 if healthy else 503,
            content={"status": "ok" if healthy else "unavailable", "database": "ok" if healthy else "unavailable"},
        )

    @app.get("/api/v1/events", response_model=list[EventSummary])
    def list_events(
        event_type: EventType | None = None,
        released_from: date | None = None,
        released_to: date | None = None,
    ) -> list[EventSummary]:
        return serving_service.list_events(event_type, released_from, released_to)

    @app.get("/api/v1/events/{event_id}/symbols", response_model=list[str])
    def list_symbols(event_id: str) -> list[str]:
        return serving_service.list_symbols(event_id)

    @app.get(
        "/api/v1/events/{event_id}/symbols/{symbol}",
        response_model=EventSymbolDetail,
    )
    def event_symbol_detail(event_id: str, symbol: SymbolPath) -> EventSymbolDetail:
        return serving_service.get_event_symbol_detail(event_id, symbol)

    @app.get(
        "/api/v1/events/{event_id}/symbols/{symbol}/bars",
        response_model=list[BarView],
    )
    def bars(
        event_id: str,
        symbol: SymbolPath,
        timeframe: Annotated[Timeframe, Query()] = "1m",
    ) -> list[BarView]:
        return serving_service.get_bars(event_id, symbol, timeframe)

    @app.get("/api/v1/strategy/summary", response_model=StrategySummaryView)
    def strategy_summary() -> StrategySummaryView:
        return serving_service.get_strategy_summary()

    @app.get(
        "/api/v1/research/historical", response_model=HistoricalComparisonView
    )
    def historical_comparison(
        event_type: EventType,
        symbol: Annotated[str, Query(pattern=r"^[A-Z][A-Z0-9.]{0,9}$")],
        window: ImpactWindow,
    ) -> HistoricalComparisonView:
        return serving_service.get_historical_comparison(event_type, symbol, window)

    @app.get(
        "/api/v1/research/cross-asset", response_model=CrossAssetComparisonView
    )
    def cross_asset_comparison(
        event_id: str,
        window: ImpactWindow,
    ) -> CrossAssetComparisonView:
        return serving_service.get_cross_asset_comparison(event_id, window)

    @app.get("/api/v1/pipelines/overview", response_model=PipelineOverviewView)
    def pipeline_overview() -> PipelineOverviewView:
        return pipeline_serving.overview()

    @app.get("/api/v1/pipelines/runs", response_model=list[PipelineRunSummary])
    def pipeline_runs(
        dag_id: str | None = None,
        status: Literal["RUNNING", "SUCCEEDED", "FAILED"] | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> list[PipelineRunSummary]:
        return pipeline_serving.list_runs(
            dag_id=dag_id, status=status, limit=limit, offset=offset
        )

    @app.get(
        "/api/v1/pipelines/runs/{pipeline_run_id}",
        response_model=PipelineRunDetailView,
    )
    def pipeline_run_detail(pipeline_run_id: str) -> PipelineRunDetailView:
        return pipeline_serving.detail(pipeline_run_id)

    @app.get("/api/v1/pipelines/quality", response_model=PipelineQualityView)
    def pipeline_quality(pipeline_run_id: str) -> PipelineQualityView:
        return pipeline_serving.quality(pipeline_run_id)

    @app.get("/api/v1/pipelines/lineage", response_model=PipelineLineageView)
    def pipeline_lineage() -> PipelineLineageView:
        return pipeline_serving.lineage()

    @app.get("/api/v1/paper/account")
    def paper_account() -> dict:
        return paper_serving.account()

    @app.get("/api/v1/paper/orders")
    def paper_orders() -> list[dict]:
        return paper_serving.orders()

    @app.post("/api/v1/paper/orders/review")
    def review_paper_order(intent: OrderIntent) -> dict:
        return paper_serving.review(intent)

    @app.post("/api/v1/paper/orders")
    def submit_paper_order(request: PaperSubmitRequest) -> dict:
        return paper_serving.submit(request.intent, request.confirmation)

    @app.post("/api/v1/paper/recovery")
    def recover_paper_orders() -> dict:
        return paper_serving.recover()

    @app.post("/api/v1/paper/orders/{request_id}/reconcile")
    def reconcile_paper_order(request_id: str) -> dict:
        return paper_serving.reconcile(request_id)

    @app.post("/api/v1/paper/orders/{request_id}/cancel")
    def cancel_paper_order(
        request_id: str, request: PaperConfirmationRequest
    ) -> dict:
        return paper_serving.cancel(request_id, request.confirmation)

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(TEMPLATE_PATH.read_text(encoding="utf-8"))

    @app.get("/pipelines", response_class=HTMLResponse)
    def pipelines_dashboard() -> HTMLResponse:
        return HTMLResponse(PIPELINES_TEMPLATE_PATH.read_text(encoding="utf-8"))

    @app.get("/paper", response_class=HTMLResponse)
    def paper_dashboard() -> HTMLResponse:
        return HTMLResponse(PAPER_TEMPLATE_PATH.read_text(encoding="utf-8"))

    return app


app = create_app()
