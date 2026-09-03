"""Derive coverage-aware 3-minute and 5-minute bars from stored 1-minute bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import psycopg

from src.historical_bars import HistoricalBar


@dataclass(frozen=True)
class DerivedBar:
    symbol: str
    bar_start: datetime
    timeframe: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    vwap: Decimal | None
    source_bar_count: int
    expected_bar_count: int

    @property
    def coverage_status(self) -> str:
        return (
            "COMPLETE"
            if self.source_bar_count == self.expected_bar_count
            else "PARTIAL"
        )


def aggregate_derived_bars(
    minute_bars: Sequence[HistoricalBar],
    minutes: int,
) -> list[DerivedBar]:
    if minutes not in (3, 5):
        raise ValueError("derived timeframe must be 3 or 5 minutes")

    grouped: dict[tuple[str, datetime], list[HistoricalBar]] = defaultdict(list)
    seen_minutes = set()
    for bar in minute_bars:
        if bar.bar_start.tzinfo is None:
            raise ValueError("minute bar timestamp must include a timezone")
        minute_key = (bar.symbol, bar.bar_start)
        if minute_key in seen_minutes:
            raise ValueError(f"duplicate minute bar: {bar.symbol} {bar.bar_start}")
        seen_minutes.add(minute_key)
        bucket_start = bar.bar_start.replace(
            minute=(bar.bar_start.minute // minutes) * minutes,
            second=0,
            microsecond=0,
        )
        grouped[(bar.symbol, bucket_start)].append(bar)

    derived = []
    for (symbol, bucket_start), bars in sorted(grouped.items()):
        ordered = sorted(bars, key=lambda item: item.bar_start)
        weighted = [
            (bar.vwap * bar.volume, bar.volume)
            for bar in ordered
            if bar.vwap is not None and bar.volume > 0
        ]
        vwap_volume = sum((volume for _, volume in weighted), 0)
        vwap = (
            sum((notional for notional, _ in weighted), Decimal("0")) / vwap_volume
            if vwap_volume
            else None
        )
        derived.append(
            DerivedBar(
                symbol=symbol,
                bar_start=bucket_start,
                timeframe=f"{minutes}m",
                open=ordered[0].open,
                high=max(bar.high for bar in ordered),
                low=min(bar.low for bar in ordered),
                close=ordered[-1].close,
                volume=sum(bar.volume for bar in ordered),
                trade_count=sum(bar.trade_count for bar in ordered),
                vwap=vwap,
                source_bar_count=len(ordered),
                expected_bar_count=minutes,
            )
        )
    return derived


def upsert_derived_bars(
    bars: Sequence[DerivedBar],
    *,
    database_url: str,
    source: str = "alpaca",
    feed: str = "sip",
) -> int:
    if not bars:
        return 0
    rows = [
        (
            bar.symbol,
            bar.bar_start,
            bar.timeframe,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.trade_count,
            bar.vwap,
            source,
            feed,
            True,
            "provider_aggregated_from_1m_v1",
            -1,
            bar.source_bar_count,
            bar.expected_bar_count,
            bar.coverage_status,
        )
        for bar in bars
    ]
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.executemany(
                """
                INSERT INTO market_bars (
                    symbol, bar_start, timeframe, open, high, low, close,
                    volume, trade_count, vwap, source, feed, is_final,
                    condition_policy, spark_batch_id, source_bar_count,
                    expected_bar_count, coverage_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (symbol, bar_start, timeframe, source, feed)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trade_count = EXCLUDED.trade_count,
                    vwap = EXCLUDED.vwap,
                    source_bar_count = EXCLUDED.source_bar_count,
                    expected_bar_count = EXCLUDED.expected_bar_count,
                    coverage_status = EXCLUDED.coverage_status,
                    condition_policy = EXCLUDED.condition_policy,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
    return len(rows)
