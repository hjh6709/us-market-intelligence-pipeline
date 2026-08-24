"""Calculate reproducible CPI event-window metrics from stored SIP minute bars."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file


ANALYSIS_VERSION = "cpi_sip_v1"
SYMBOLS = ("SPY", "QQQ", "SMH", "NVDA")
WINDOWS = (
    ("PRE_60M", -60, 0),
    ("POST_5M", 0, 5),
    ("POST_30M", 0, 30),
    ("POST_60M", 0, 60),
)
HUNDRED = Decimal("100")


@dataclass(frozen=True)
class BarPoint:
    bar_start: datetime
    open: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class ImpactMetric:
    economic_event_id: str
    symbol: str
    window_name: str
    window_start: datetime
    window_end: datetime
    open_price: Decimal | None
    close_price: Decimal | None
    return_pct: Decimal | None
    volume: int | None
    realized_volatility: Decimal | None
    benchmark_return_pct: Decimal | None
    market_relative_return_pct: Decimal | None
    coverage_status: str
    coverage_reason: str

    @property
    def impact_id(self) -> str:
        key = "|".join(
            (
                self.economic_event_id,
                self.symbol,
                "alpaca",
                "sip",
                self.window_name,
                ANALYSIS_VERSION,
            )
        )
        return "sha256:" + hashlib.sha256(key.encode("utf-8")).hexdigest()


def calculate_metric(
    *,
    economic_event_id: str,
    symbol: str,
    window_name: str,
    window_start: datetime,
    window_end: datetime,
    bars: Sequence[BarPoint],
    opening_price: Decimal | None = None,
) -> ImpactMetric:
    selected = sorted(
        (
            bar
            for bar in bars
            if window_start <= bar.bar_start < window_end
        ),
        key=lambda bar: bar.bar_start,
    )
    expected_minutes = int((window_end - window_start).total_seconds() / 60)
    if not selected:
        return ImpactMetric(
            economic_event_id,
            symbol,
            window_name,
            window_start,
            window_end,
            opening_price,
            None,
            None,
            None,
            None,
            None,
            None,
            "NO_MARKET_DATA",
            f"bars=0/{expected_minutes}",
        )
    if opening_price is None:
        opening_price = selected[0].open
    if opening_price <= 0:
        raise ValueError("opening_price must be positive")

    close_price = selected[-1].close
    return_pct = (close_price / opening_price - Decimal(1)) * HUNDRED
    returns = []
    previous_price = opening_price
    for bar in selected:
        returns.append((bar.close / previous_price - Decimal(1)) * HUNDRED)
        previous_price = bar.close
    volatility = _population_stddev(returns)
    endpoint_lag = max(
        0,
        int(
            (
                window_end - (selected[-1].bar_start + timedelta(minutes=1))
            ).total_seconds()
            / 60
        ),
    )
    coverage_ratio = Decimal(len(selected)) / Decimal(expected_minutes)
    coverage_status = (
        "COMPLETE"
        if coverage_ratio >= Decimal("0.90") and endpoint_lag <= 1
        else "PARTIAL_MARKET_COVERAGE"
    )
    coverage_reason = (
        f"bars={len(selected)}/{expected_minutes};"
        f"endpoint_lag_minutes={endpoint_lag}"
    )
    return ImpactMetric(
        economic_event_id,
        symbol,
        window_name,
        window_start,
        window_end,
        opening_price,
        close_price,
        return_pct,
        sum(bar.volume for bar in selected),
        volatility,
        None,
        None,
        coverage_status,
        coverage_reason,
    )


def calculate_event_impacts(
    economic_event_id: str,
    released_at: datetime,
    bars_by_symbol: Mapping[str, Sequence[BarPoint]],
) -> list[ImpactMetric]:
    metrics = []
    for symbol in SYMBOLS:
        bars = bars_by_symbol.get(symbol, ())
        pre_metric = calculate_metric(
            economic_event_id=economic_event_id,
            symbol=symbol,
            window_name="PRE_60M",
            window_start=released_at - timedelta(minutes=60),
            window_end=released_at,
            bars=bars,
        )
        metrics.append(pre_metric)
        for window_name, _, end_minutes in WINDOWS[1:]:
            metric = calculate_metric(
                economic_event_id=economic_event_id,
                symbol=symbol,
                window_name=window_name,
                window_start=released_at,
                window_end=released_at + timedelta(minutes=end_minutes),
                bars=bars,
                opening_price=pre_metric.close_price,
            )
            if pre_metric.close_price is None and metric.coverage_status != "NO_MARKET_DATA":
                metric = replace(
                    metric,
                    open_price=None,
                    return_pct=None,
                    coverage_status="MISSING_PRE_RELEASE_BASELINE",
                    coverage_reason=metric.coverage_reason + ";pre_close=missing",
                )
            metrics.append(metric)

    benchmark_by_window = {
        metric.window_name: metric.return_pct
        for metric in metrics
        if metric.symbol == "SPY"
    }
    return [
        replace(
            metric,
            benchmark_return_pct=benchmark_by_window.get(metric.window_name),
            market_relative_return_pct=(
                metric.return_pct - benchmark_by_window[metric.window_name]
                if metric.return_pct is not None
                and benchmark_by_window.get(metric.window_name) is not None
                else None
            ),
        )
        for metric in metrics
    ]


def calculate_and_store(database_url: str) -> tuple[int, int]:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        events = connection.execute(
            """
            SELECT economic_event_id, released_at
            FROM economic_events
            WHERE event_type = 'CPI' AND quality_status = 'READY'
            ORDER BY released_at
            """
        ).fetchall()
        all_metrics = []
        for economic_event_id, released_at in events:
            rows = connection.execute(
                """
                SELECT symbol, bar_start, open, close, volume
                FROM market_bars
                WHERE source = 'alpaca'
                  AND feed = 'sip'
                  AND timeframe = '1m'
                  AND symbol = ANY(%s)
                  AND bar_start >= %s - INTERVAL '60 minutes'
                  AND bar_start < %s + INTERVAL '60 minutes'
                ORDER BY symbol, bar_start
                """,
                (list(SYMBOLS), released_at, released_at),
            ).fetchall()
            bars_by_symbol = {symbol: [] for symbol in SYMBOLS}
            for symbol, bar_start, open_price, close_price, volume in rows:
                bars_by_symbol[symbol].append(
                    BarPoint(bar_start, open_price, close_price, volume)
                )
            all_metrics.extend(
                calculate_event_impacts(
                    economic_event_id,
                    released_at,
                    bars_by_symbol,
                )
            )

        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO macro_event_impacts (
                    impact_id, economic_event_id, symbol, source, feed,
                    session_scope, window_name, window_start, window_end,
                    open_price, close_price, return_pct, volume,
                    realized_volatility, benchmark_symbol,
                    benchmark_return_pct, market_relative_return_pct,
                    coverage_status, coverage_reason, analysis_version
                ) VALUES (
                    %s, %s, %s, 'alpaca', 'sip', 'EXTENDED_HOURS',
                    %s, %s, %s, %s, %s, %s, %s, %s, 'SPY', %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    economic_event_id, symbol, source, feed,
                    window_name, analysis_version
                ) DO UPDATE SET
                    impact_id = EXCLUDED.impact_id,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    return_pct = EXCLUDED.return_pct,
                    volume = EXCLUDED.volume,
                    realized_volatility = EXCLUDED.realized_volatility,
                    benchmark_return_pct = EXCLUDED.benchmark_return_pct,
                    market_relative_return_pct = EXCLUDED.market_relative_return_pct,
                    coverage_status = EXCLUDED.coverage_status,
                    coverage_reason = EXCLUDED.coverage_reason,
                    created_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        metric.impact_id,
                        metric.economic_event_id,
                        metric.symbol,
                        metric.window_name,
                        metric.window_start,
                        metric.window_end,
                        metric.open_price,
                        metric.close_price,
                        metric.return_pct,
                        metric.volume,
                        metric.realized_volatility,
                        metric.benchmark_return_pct,
                        metric.market_relative_return_pct,
                        metric.coverage_status,
                        metric.coverage_reason,
                        ANALYSIS_VERSION,
                    )
                    for metric in all_metrics
                ],
            )
    return len(events), len(all_metrics)


def _population_stddev(values: Sequence[Decimal]) -> Decimal | None:
    if len(values) < 2:
        return None
    mean = sum(values, Decimal(0)) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    return variance.sqrt()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args(argv)
    file_values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or file_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    events, impacts = calculate_and_store(database_url)
    print(json.dumps({"economic_events": events, "event_impacts_upserted": impacts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
