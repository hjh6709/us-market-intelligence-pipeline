from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


ResearchSignal = Literal["LONG", "SHORT", "FLAT"]
CheckStatus = Literal["PASS", "FAIL"]
ExecutionStage = Literal[
    "RESEARCH_ONLY",
    "PAPER_TRADING",
    "HUMAN_APPROVAL",
    "LIMITED_LIVE",
    "AUTOMATED_LIVE",
]
OrderAction = Literal["NO_TRADE"]
CoverageStatus = Literal["COMPLETE", "PARTIAL"]
Timeframe = Literal["1m", "3m", "5m"]


class EventSummary(BaseModel):
    event_id: str
    event_type: str
    reference_period: str
    released_at: datetime
    source: str | None = None
    quality_status: str | None = None


class ImpactView(BaseModel):
    window_name: str
    return_pct: Decimal | None = None
    market_return_pct: Decimal | None = None
    excess_return_pct: Decimal | None = None
    volume: Decimal | None = None
    realized_volatility: Decimal | None = None
    coverage_status: str


class MacroContextView(BaseModel):
    series_id: str
    series_name: str | None = None
    observation_date: date
    value: Decimal
    vintage_date: date | None = None


class SimulationView(BaseModel):
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    gross_return_pct: Decimal | None = None
    transaction_cost_bps: Decimal
    net_return_pct: Decimal | None = None
    coverage_status: str


class ReadinessCheckView(BaseModel):
    name: str
    status: CheckStatus
    reason: str | None = None


class ExecutionReadinessView(BaseModel):
    stage: ExecutionStage
    order_action: OrderAction
    eligible_for_order: bool
    requires_human_approval: bool
    checks: list[ReadinessCheckView]
    reasons: list[str]


class EventSymbolDetail(BaseModel):
    event: EventSummary
    symbol: str
    impacts: list[ImpactView]
    macro_context: list[MacroContextView]
    research_signal: ResearchSignal
    simulation: SimulationView | None
    execution_readiness: ExecutionReadinessView


class BarView(BaseModel):
    symbol: str
    timeframe: Timeframe
    window_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    source_bar_count: int | None = None
    expected_bar_count: int | None = None
    coverage_status: str | None = None


class StrategySummaryView(BaseModel):
    strategy_name: str
    strategy_version: str
    total_count: int = Field(ge=0)
    eligible_count: int = Field(ge=0)
    mean_net_return_pct: Decimal | None = None
    positive_count: int = Field(ge=0)
    positive_rate_pct: Decimal | None = None
