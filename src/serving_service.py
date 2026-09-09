from datetime import date
from decimal import Decimal

from src.execution_readiness import ReadinessInput, evaluate_execution_readiness
from src.serving_models import (
    BarView,
    CrossAssetComparisonView,
    CrossAssetImpactPoint,
    DatasetMetricView,
    EventSummary,
    EventSymbolDetail,
    HistoricalComparisonView,
    HistoricalImpactPoint,
    ImpactView,
    MacroContextView,
    PlatformOverviewView,
    ResearchProvenanceView,
    SimulationView,
    StrategySummaryView,
)
from src.serving_repository import EventRecord, PostgresServingRepository
from src.platform_contracts import (
    ANALYSIS_VERSION,
    MARKET_FEED,
    MARKET_SOURCE,
    STRATEGY_NAME,
    STRATEGY_VERSION,
)


def _provenance() -> ResearchProvenanceView:
    return ResearchProvenanceView(
        source=MARKET_SOURCE,
        feed=MARKET_FEED,
        analysis_version=ANALYSIS_VERSION,
        strategy_name=STRATEGY_NAME,
        strategy_version=STRATEGY_VERSION,
    )


class ServingNotFoundError(LookupError):
    def __init__(self, resource: str) -> None:
        self.resource = resource
        super().__init__(f"{resource} not found")


class ServingService:
    def __init__(self, repository: PostgresServingRepository) -> None:
        self.repository = repository

    def health(self) -> bool:
        return self.repository.health()

    def list_events(
        self,
        event_type: str | None = None,
        released_from: date | None = None,
        released_to: date | None = None,
    ) -> list[EventSummary]:
        return [
            self._event_view(record)
            for record in self.repository.list_events(event_type, released_from, released_to)
        ]

    def list_symbols(self, event_id: str) -> list[str]:
        if self.repository.get_event(event_id) is None:
            raise ServingNotFoundError("event")
        return self.repository.list_symbols(event_id)

    def get_bars(self, event_id: str, symbol: str, timeframe: str) -> list[BarView]:
        event = self.repository.get_event(event_id)
        if event is None:
            raise ServingNotFoundError("event")
        if symbol not in self.repository.list_symbols(event_id):
            raise ServingNotFoundError("symbol")
        records = self.repository.get_bars(event_id, symbol, timeframe)
        if not records:
            raise ServingNotFoundError("bars")
        return [BarView(**record.__dict__) for record in records]

    def get_strategy_summary(self) -> StrategySummaryView:
        record = self.repository.get_strategy_summary()
        positive_rate = None
        if record.eligible_count:
            positive_rate = (
                Decimal(record.positive_count) / Decimal(record.eligible_count) * 100
            )
        return StrategySummaryView(
            strategy_name=STRATEGY_NAME,
            strategy_version=STRATEGY_VERSION,
            total_count=record.total_count,
            eligible_count=record.eligible_count,
            mean_net_return_pct=record.mean_net_return_pct,
            positive_count=record.positive_count,
            positive_rate_pct=positive_rate,
            provenance=_provenance(),
        )

    def get_event_symbol_detail(self, event_id: str, symbol: str) -> EventSymbolDetail:
        event = self.repository.get_event(event_id)
        if event is None:
            raise ServingNotFoundError("event")
        if symbol not in self.repository.list_symbols(event_id):
            raise ServingNotFoundError("symbol")

        impact_records = self.repository.get_impacts(event_id, symbol)
        macro_records = self.repository.get_macro_context(event_id)
        strategy = self.repository.get_strategy_result(event_id, symbol)
        summary = self.repository.get_strategy_summary()
        simulation = None
        signal = "FLAT"
        if strategy is not None:
            signal = {1: "LONG", -1: "SHORT", 0: "FLAT"}[strategy.signal]
            simulation = SimulationView(
                entry_price=strategy.entry_price,
                exit_price=strategy.exit_price,
                gross_return_pct=strategy.gross_return_pct,
                transaction_cost_bps=strategy.transaction_cost_bps,
                net_return_pct=strategy.net_return_pct,
                coverage_status=strategy.coverage_status,
            )

        required_windows = {"PRE_60M", "POST_5M", "POST_30M", "POST_60M"}
        complete_windows = {
            impact.window_name
            for impact in impact_records
            if impact.return_pct is not None and impact.coverage_status == "COMPLETE"
        }
        readiness = evaluate_execution_readiness(
            ReadinessInput(
                market_data_ready=required_windows.issubset(complete_windows),
                strategy_result_ready=(
                    strategy is not None
                    and strategy.net_return_pct is not None
                    and strategy.coverage_status == "COMPLETE"
                ),
                strategy_mean_net_return_pct=summary.mean_net_return_pct,
                forecast=event.forecast,
                actual=event.actual,
                surprise=event.surprise,
                paper_execution_enabled=False,
                position_recovery_enabled=False,
                kill_switch_enabled=False,
            )
        )
        return EventSymbolDetail(
            event=self._event_view(event),
            symbol=symbol,
            impacts=[
                ImpactView(
                    window_name=record.window_name,
                    return_pct=record.return_pct,
                    market_return_pct=record.market_return_pct,
                    excess_return_pct=record.excess_return_pct,
                    volume=record.volume,
                    realized_volatility=record.realized_volatility,
                    coverage_status=record.coverage_status,
                )
                for record in impact_records
            ],
            macro_context=[MacroContextView(**record.__dict__) for record in macro_records],
            research_signal=signal,
            simulation=simulation,
            execution_readiness=readiness,
            provenance=_provenance(),
        )

    def get_historical_comparison(
        self, event_type: str, symbol: str, window_name: str
    ) -> HistoricalComparisonView:
        records = self.repository.get_historical_impacts(
            event_type, symbol, window_name
        )
        return HistoricalComparisonView(
            event_type=event_type,
            symbol=symbol,
            window_name=window_name,
            points=[HistoricalImpactPoint(**record.__dict__) for record in records],
            provenance=_provenance(),
        )

    def get_cross_asset_comparison(
        self, event_id: str, window_name: str
    ) -> CrossAssetComparisonView:
        if self.repository.get_event(event_id) is None:
            raise ServingNotFoundError("event")
        records = self.repository.get_cross_asset_impacts(event_id, window_name)
        return CrossAssetComparisonView(
            event_id=event_id,
            window_name=window_name,
            points=[CrossAssetImpactPoint(**record.__dict__) for record in records],
            provenance=_provenance(),
        )

    def get_overview(self, latest_pipeline: dict | None) -> PlatformOverviewView:
        record = self.repository.get_overview_metrics()
        metric_specs = (
            ("releases", "Official releases", record.releases, "releases"),
            ("symbols", "Supported assets", record.symbols, "symbols"),
            ("event_symbol_intervals", "Event-asset intervals", record.event_symbol_intervals, "intervals"),
            ("stored_1m_bars", "Stored SIP 1m bars", record.stored_1m_bars, "bar rows"),
            ("derived_3m_bars", "Derived 3m bars", record.derived_3m_bars, "bar rows"),
            ("derived_5m_bars", "Derived 5m bars", record.derived_5m_bars, "bar rows"),
            ("pit_macro_contexts", "Point-in-time macro contexts", record.pit_macro_contexts, "context rows"),
            ("impact_rows", "Event impact results", record.impact_rows, "analysis rows"),
        )
        return PlatformOverviewView(
            product_name="U.S. Economic Event Market Intelligence Platform",
            description="A reproducible point-in-time data platform for U.S. economic-event market analysis.",
            metrics=[DatasetMetricView(key=k, label=l, value=v, unit=u) for k,l,v,u in metric_specs],
            event_type_counts=self.repository.get_event_type_counts(),
            supported_symbols=self.repository.list_supported_symbols(),
            recent_events=[self._event_view(item) for item in self.repository.list_events()[:6]],
            baseline=self.get_strategy_summary(),
            latest_pipeline=latest_pipeline,
            limitations=[
                "탐색용 기준 전략의 전체 평균은 음수이며 투자 권유가 아닙니다.",
                "수집이 정상 완료돼도 실제 제공자 가격 봉은 일부 비어 있을 수 있습니다.",
                "모의주문은 분석 신호와 분리되어 자동으로 주문되지 않습니다.",
            ],
        )

    @staticmethod
    def _event_view(record: EventRecord) -> EventSummary:
        return EventSummary(
            event_id=record.event_id,
            event_type=record.event_type,
            reference_period=record.reference_period,
            released_at=record.released_at,
            source=record.source,
            quality_status=record.quality_status,
        )
