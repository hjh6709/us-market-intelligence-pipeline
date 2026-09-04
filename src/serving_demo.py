import re
from collections.abc import Callable

import psycopg
from pydantic import BaseModel

from src.event_strategy_backtest import calculate_and_store as calculate_strategy
from src.macro_event_impact import calculate_and_store as calculate_impacts
from src.serving_repository import PostgresServingRepository
from src.serving_service import ServingService


class DemoInput(BaseModel):
    event_id: str
    symbol: str


class DemoProcessing(BaseModel):
    events: int
    symbols: int
    impact_rows_upserted: int


class DemoStorage(BaseModel):
    strategy_rows_upserted: int
    duplicate_business_keys: int


class DemoRead(BaseModel):
    impact_rows: int
    bar_timeframes: list[str]


class DemoDecision(BaseModel):
    stage: str
    order_action: str


class ServingDemoResult(BaseModel):
    input: DemoInput
    processing: DemoProcessing
    storage: DemoStorage
    read: DemoRead
    result: DemoDecision


def count_duplicate_business_keys(database_url: str) -> int:
    query = """
        SELECT count(*)
        FROM (
            SELECT 1
            FROM macro_event_impacts
            GROUP BY economic_event_id, symbol, source, feed, window_name, analysis_version
            HAVING count(*) > 1
            UNION ALL
            SELECT 1
            FROM event_strategy_results
            GROUP BY economic_event_id, symbol, strategy_name, strategy_version
            HAVING count(*) > 1
        ) AS duplicate_keys
    """
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        row = connection.execute(query).fetchone()
        return int(row[0]) if row is not None else 0


def run_serving_demo(
    database_url: str,
    event_id: str,
    symbol: str,
    *,
    impact_runner: Callable[..., tuple[int, int]] = calculate_impacts,
    strategy_runner: Callable[..., dict[str, object]] = calculate_strategy,
    service: ServingService | None = None,
    duplicate_counter: Callable[[str], int] = count_duplicate_business_keys,
) -> ServingDemoResult:
    if not event_id.strip():
        raise ValueError("event_id must not be empty")
    if not re.fullmatch(r"[A-Z][A-Z0-9.]{0,9}", symbol):
        raise ValueError("symbol must be an uppercase ticker")

    event_count, impact_count = impact_runner(
        database_url,
        event_ids=[event_id],
        symbols=[symbol],
    )
    strategy_summary = strategy_runner(
        database_url,
        event_ids=[event_id],
        symbols=[symbol],
    )
    serving_service = service or ServingService(PostgresServingRepository(database_url))
    detail = serving_service.get_event_symbol_detail(event_id, symbol)
    available_timeframes = []
    for timeframe in ("1m", "3m", "5m"):
        if serving_service.get_bars(event_id, symbol, timeframe):
            available_timeframes.append(timeframe)

    return ServingDemoResult(
        input=DemoInput(event_id=event_id, symbol=symbol),
        processing=DemoProcessing(
            events=event_count,
            symbols=1,
            impact_rows_upserted=impact_count,
        ),
        storage=DemoStorage(
            strategy_rows_upserted=int(strategy_summary["rows"]),
            duplicate_business_keys=duplicate_counter(database_url),
        ),
        read=DemoRead(
            impact_rows=len(detail.impacts),
            bar_timeframes=available_timeframes,
        ),
        result=DemoDecision(
            stage=detail.execution_readiness.stage,
            order_action=detail.execution_readiness.order_action,
        ),
    )
