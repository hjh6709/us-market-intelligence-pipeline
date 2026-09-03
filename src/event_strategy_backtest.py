"""Run a point-in-time-safe exploratory strategy on stored event metrics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import psycopg

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file
from src.macro_event_impact import ANALYSIS_VERSION


STRATEGY_NAME = "pre60_momentum_post60"
STRATEGY_VERSION = "v1"
DEFAULT_TRANSACTION_COST_BPS = Decimal("10")


@dataclass(frozen=True)
class StrategyResult:
    economic_event_id: str
    symbol: str
    signal: int
    entry_at: datetime
    exit_at: datetime
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_return_pct: Decimal | None
    transaction_cost_bps: Decimal
    net_return_pct: Decimal | None
    benchmark_return_pct: Decimal | None
    coverage_status: str

    @property
    def strategy_result_id(self) -> str:
        value = "|".join(
            (
                self.economic_event_id,
                self.symbol,
                STRATEGY_NAME,
                STRATEGY_VERSION,
            )
        )
        return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def calculate_strategy_result(
    *,
    economic_event_id: str,
    symbol: str,
    entry_at: datetime,
    exit_at: datetime,
    entry_price: Decimal | None,
    exit_price: Decimal | None,
    pre_return_pct: Decimal | None,
    post_return_pct: Decimal | None,
    benchmark_return_pct: Decimal | None,
    pre_coverage: str,
    post_coverage: str,
    transaction_cost_bps: Decimal = DEFAULT_TRANSACTION_COST_BPS,
) -> StrategyResult:
    if transaction_cost_bps < 0:
        raise ValueError("transaction_cost_bps must not be negative")
    eligible = (
        pre_return_pct is not None
        and post_return_pct is not None
        and entry_price is not None
        and exit_price is not None
    )
    signal = 0
    gross_return = None
    net_return = None
    if eligible:
        signal = 1 if pre_return_pct > 0 else -1 if pre_return_pct < 0 else 0
        gross_return = Decimal(signal) * post_return_pct
        round_trip_cost_pct = (
            transaction_cost_bps / Decimal("100") if signal else Decimal(0)
        )
        net_return = gross_return - round_trip_cost_pct
    coverage_status = (
        "NOT_ELIGIBLE"
        if not eligible
        else "COMPLETE"
        if pre_coverage == post_coverage == "COMPLETE"
        else "PARTIAL_MARKET_COVERAGE"
    )
    return StrategyResult(
        economic_event_id=economic_event_id,
        symbol=symbol,
        signal=signal,
        entry_at=entry_at,
        exit_at=exit_at,
        entry_price=entry_price,
        exit_price=exit_price,
        gross_return_pct=gross_return,
        transaction_cost_bps=transaction_cost_bps,
        net_return_pct=net_return,
        benchmark_return_pct=benchmark_return_pct,
        coverage_status=coverage_status,
    )


def calculate_and_store(
    database_url: str,
    *,
    transaction_cost_bps: Decimal = DEFAULT_TRANSACTION_COST_BPS,
) -> dict[str, object]:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        rows = connection.execute(
            """
            SELECT
                pre.economic_event_id,
                pre.symbol,
                post.window_start,
                post.window_end,
                post.open_price,
                post.close_price,
                pre.return_pct,
                post.return_pct,
                post.benchmark_return_pct,
                pre.coverage_status,
                post.coverage_status
            FROM macro_event_impacts pre
            JOIN macro_event_impacts post
              ON post.economic_event_id = pre.economic_event_id
             AND post.symbol = pre.symbol
             AND post.analysis_version = pre.analysis_version
            WHERE pre.analysis_version = %s
              AND pre.window_name = 'PRE_60M'
              AND post.window_name = 'POST_60M'
            ORDER BY pre.economic_event_id, pre.symbol
            """,
            (ANALYSIS_VERSION,),
        ).fetchall()
        results = [
            calculate_strategy_result(
                economic_event_id=row[0],
                symbol=row[1],
                entry_at=row[2],
                exit_at=row[3],
                entry_price=row[4],
                exit_price=row[5],
                pre_return_pct=row[6],
                post_return_pct=row[7],
                benchmark_return_pct=row[8],
                pre_coverage=row[9],
                post_coverage=row[10],
                transaction_cost_bps=transaction_cost_bps,
            )
            for row in rows
        ]
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO event_strategy_results (
                    strategy_result_id, economic_event_id, symbol, strategy_name,
                    signal, entry_at, exit_at, entry_price, exit_price,
                    gross_return_pct, transaction_cost_bps, net_return_pct,
                    benchmark_return_pct, coverage_status, strategy_version
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    economic_event_id, symbol, strategy_name, strategy_version
                ) DO UPDATE SET
                    strategy_result_id = EXCLUDED.strategy_result_id,
                    signal = EXCLUDED.signal,
                    entry_at = EXCLUDED.entry_at,
                    exit_at = EXCLUDED.exit_at,
                    entry_price = EXCLUDED.entry_price,
                    exit_price = EXCLUDED.exit_price,
                    gross_return_pct = EXCLUDED.gross_return_pct,
                    transaction_cost_bps = EXCLUDED.transaction_cost_bps,
                    net_return_pct = EXCLUDED.net_return_pct,
                    benchmark_return_pct = EXCLUDED.benchmark_return_pct,
                    coverage_status = EXCLUDED.coverage_status,
                    created_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        result.strategy_result_id,
                        result.economic_event_id,
                        result.symbol,
                        STRATEGY_NAME,
                        result.signal,
                        result.entry_at,
                        result.exit_at,
                        result.entry_price,
                        result.exit_price,
                        result.gross_return_pct,
                        result.transaction_cost_bps,
                        result.net_return_pct,
                        result.benchmark_return_pct,
                        result.coverage_status,
                        STRATEGY_VERSION,
                    )
                    for result in results
                ],
            )
        eligible = [item for item in results if item.net_return_pct is not None]
        mean_net = (
            sum((item.net_return_pct for item in eligible), Decimal(0))
            / Decimal(len(eligible))
            if eligible
            else None
        )
        positive = sum(item.net_return_pct > 0 for item in eligible)
        return {
            "strategy": STRATEGY_NAME,
            "analysis_version": ANALYSIS_VERSION,
            "transaction_cost_bps_round_trip": str(transaction_cost_bps),
            "rows": len(results),
            "eligible_rows": len(eligible),
            "complete_rows": sum(item.coverage_status == "COMPLETE" for item in results),
            "mean_net_return_pct": str(mean_net) if mean_net is not None else None,
            "positive_net_rows": positive,
            "positive_net_ratio": positive / len(eligible) if eligible else None,
        }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--transaction-cost-bps",
        type=Decimal,
        default=DEFAULT_TRANSACTION_COST_BPS,
    )
    args = parser.parse_args(argv)
    file_values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or file_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    print(
        json.dumps(
            calculate_and_store(
                database_url,
                transaction_cost_bps=args.transaction_cost_bps,
            )
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
