import os
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, Path as ApiPath, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.serving_models import (
    BarView,
    EventSummary,
    EventSymbolDetail,
    StrategySummaryView,
)
from src.serving_repository import PostgresServingRepository
from src.serving_service import ServingNotFoundError, ServingService


EventType = Literal["CPI", "EMPLOYMENT", "PCE", "FOMC"]
Timeframe = Literal["1m", "3m", "5m"]
SymbolPath = Annotated[str, ApiPath(pattern=r"^[A-Z][A-Z0-9.]{0,9}$")]
TEMPLATE_PATH = Path(__file__).with_name("templates") / "dashboard.html"


def create_app(service: ServingService | None = None) -> FastAPI:
    serving_service = service or ServingService(
        PostgresServingRepository(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    )
    app = FastAPI(
        title="U.S. Market Intelligence Serving API",
        version="1.0.0",
        description="Read-only economic-event research results. No broker order routes.",
    )

    @app.exception_handler(ServingNotFoundError)
    async def not_found_handler(
        _request: Request, error: ServingNotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"detail": f"{error.resource} not found"},
        )

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

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(TEMPLATE_PATH.read_text(encoding="utf-8"))

    return app


app = create_app()
