from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg


STRATEGY_NAME = "pre60_momentum_post60"
STRATEGY_VERSION = "v1"
ANALYSIS_VERSION = "multi_event_sip_v1"
ALLOWED_TIMEFRAMES = frozenset({"1m", "3m", "5m"})


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    event_type: str
    reference_period: str
    released_at: datetime
    source: str
    quality_status: str
    forecast: Decimal | None
    actual: Decimal | None
    surprise: Decimal | None


@dataclass(frozen=True)
class ImpactRecord:
    window_name: str
    return_pct: Decimal | None
    market_return_pct: Decimal | None
    excess_return_pct: Decimal | None
    volume: int | None
    realized_volatility: Decimal | None
    coverage_status: str


@dataclass(frozen=True)
class MacroContextRecord:
    series_id: str
    series_name: str | None
    observation_date: date
    value: Decimal
    vintage_date: date | None


@dataclass(frozen=True)
class StrategyRecord:
    signal: int
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_return_pct: Decimal | None
    transaction_cost_bps: Decimal
    net_return_pct: Decimal | None
    coverage_status: str


@dataclass(frozen=True)
class StrategySummaryRecord:
    total_count: int
    eligible_count: int
    mean_net_return_pct: Decimal | None
    positive_count: int


@dataclass(frozen=True)
class BarRecord:
    symbol: str
    timeframe: str
    window_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    source_bar_count: int | None
    expected_bar_count: int | None
    coverage_status: str | None


class PostgresServingRepository:
    def __init__(
        self,
        database_url: str,
        *,
        connect: Callable[..., object] = psycopg.connect,
    ) -> None:
        self.database_url = database_url
        self._connect = connect

    def _connection(self):
        return self._connect(self.database_url, connect_timeout=5)

    def health(self) -> bool:
        try:
            with self._connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone() == (1,)
        except psycopg.Error:
            return False

    def list_events(
        self,
        event_type: str | None = None,
        released_from: date | None = None,
        released_to: date | None = None,
    ) -> list[EventRecord]:
        filters: list[str] = []
        params: list[object] = []
        if event_type is not None:
            filters.append("event_type = %s")
            params.append(event_type)
        if released_from is not None:
            filters.append("released_at::date >= %s")
            params.append(released_from)
        if released_to is not None:
            filters.append("released_at::date <= %s")
            params.append(released_to)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        sql = f"""
            SELECT economic_event_id, event_type, reference_period, released_at,
                   release_source, quality_status, forecast, actual, surprise
            FROM economic_events
            {where}
            ORDER BY released_at DESC, event_type, reference_period
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return [EventRecord(*row) for row in cursor.fetchall()]

    def get_event(self, event_id: str) -> EventRecord | None:
        sql = """
            SELECT economic_event_id, event_type, reference_period, released_at,
                   release_source, quality_status, forecast, actual, surprise
            FROM economic_events
            WHERE economic_event_id = %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (event_id,))
            row = cursor.fetchone()
            return EventRecord(*row) if row is not None else None

    def list_symbols(self, event_id: str) -> list[str]:
        sql = """
            SELECT DISTINCT symbol
            FROM macro_event_impacts
            WHERE economic_event_id = %s AND analysis_version = %s
            ORDER BY symbol
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (event_id, ANALYSIS_VERSION))
            return [row[0] for row in cursor.fetchall()]

    def get_impacts(self, event_id: str, symbol: str) -> list[ImpactRecord]:
        sql = """
            SELECT window_name, return_pct, benchmark_return_pct,
                   market_relative_return_pct, volume, realized_volatility,
                   coverage_status
            FROM macro_event_impacts
            WHERE economic_event_id = %s AND symbol = %s
              AND analysis_version = %s
            ORDER BY window_start
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (event_id, symbol, ANALYSIS_VERSION))
            return [ImpactRecord(*row) for row in cursor.fetchall()]

    def get_macro_context(self, event_id: str) -> list[MacroContextRecord]:
        sql = """
            SELECT context.series_id, series.title, context.observation_date,
                   context.value, context.realtime_start
            FROM macro_event_contexts AS context
            JOIN macro_series AS series USING (series_id)
            WHERE context.economic_event_id = %s
            ORDER BY context.series_id
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (event_id,))
            return [MacroContextRecord(*row) for row in cursor.fetchall()]

    def get_strategy_result(self, event_id: str, symbol: str) -> StrategyRecord | None:
        sql = """
            SELECT signal, entry_price, exit_price, gross_return_pct,
                   transaction_cost_bps, net_return_pct, coverage_status
            FROM event_strategy_results
            WHERE economic_event_id = %s AND symbol = %s
              AND strategy_name = %s AND strategy_version = %s
        """
        params = (event_id, symbol, STRATEGY_NAME, STRATEGY_VERSION)
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return StrategyRecord(*row) if row is not None else None

    def get_strategy_summary(self) -> StrategySummaryRecord:
        sql = """
            SELECT count(*), count(net_return_pct), avg(net_return_pct),
                   count(*) FILTER (WHERE net_return_pct > 0)
            FROM event_strategy_results
            WHERE strategy_name = %s AND strategy_version = %s
        """
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (STRATEGY_NAME, STRATEGY_VERSION))
            row = cursor.fetchone()
            if row is None:
                return StrategySummaryRecord(0, 0, None, 0)
            return StrategySummaryRecord(*row)

    def get_bars(self, event_id: str, symbol: str, timeframe: str) -> list[BarRecord]:
        if timeframe not in ALLOWED_TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {timeframe}")

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT released_at FROM economic_events WHERE economic_event_id = %s",
                (event_id,),
            )
            event_row = cursor.fetchone()
            if event_row is None:
                return []
            released_at = event_row[0]
            start = released_at - timedelta(minutes=60)
            end = released_at + timedelta(minutes=120)
            cursor.execute(
                """
                SELECT symbol, timeframe, bar_start, open, high, low, close,
                       volume, source_bar_count, expected_bar_count, coverage_status
                FROM market_bars
                WHERE symbol = %s AND timeframe = %s
                  AND source = %s AND feed = %s
                  AND bar_start >= %s AND bar_start < %s
                ORDER BY bar_start
                """,
                (symbol, timeframe, "alpaca", "sip", start, end),
            )
            return [BarRecord(*row) for row in cursor.fetchall()]
