"""Export public, aggregate evidence for the multi-event analysis and backtest."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.event_strategy_backtest import STRATEGY_NAME, STRATEGY_VERSION
from src.live_market_smoke import _read_env_file
from src.macro_event_impact import ANALYSIS_VERSION


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _rows(cursor: psycopg.Cursor, query: str, params: tuple = ()) -> list[dict]:
    cursor.execute(query, params)
    columns = [description.name for description in cursor.description]
    return [
        {column: _json_value(value) for column, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def build_summary(database_url: str) -> dict[str, object]:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            impact_coverage = _rows(
                cursor,
                """
                SELECT e.event_type, i.window_name, i.coverage_status,
                       COUNT(*) AS rows
                FROM macro_event_impacts i
                JOIN economic_events e USING (economic_event_id)
                WHERE i.analysis_version = %s
                GROUP BY e.event_type, i.window_name, i.coverage_status
                ORDER BY e.event_type, i.window_name, i.coverage_status
                """,
                (ANALYSIS_VERSION,),
            )
            event_returns = _rows(
                cursor,
                """
                SELECT e.event_type, i.symbol, i.window_name,
                       COUNT(i.return_pct) AS usable_rows,
                       ROUND(AVG(i.return_pct), 6) AS mean_return_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY i.return_pct
                       )::numeric, 6) AS median_return_pct,
                       ROUND(STDDEV_SAMP(i.return_pct), 6) AS sample_stddev_pct,
                       ROUND((AVG(i.return_pct) - 1.96 * STDDEV_SAMP(i.return_pct)
                           / SQRT(COUNT(i.return_pct)))::numeric, 6)
                           AS mean_95pct_ci_lower,
                       ROUND((AVG(i.return_pct) + 1.96 * STDDEV_SAMP(i.return_pct)
                           / SQRT(COUNT(i.return_pct)))::numeric, 6)
                           AS mean_95pct_ci_upper,
                       ROUND(AVG((i.return_pct > 0)::int)::numeric, 6)
                           AS positive_return_ratio
                FROM macro_event_impacts i
                JOIN economic_events e USING (economic_event_id)
                WHERE i.analysis_version = %s
                  AND i.window_name IN ('POST_5M', 'POST_30M', 'POST_60M')
                GROUP BY e.event_type, i.symbol, i.window_name
                ORDER BY e.event_type, i.symbol, i.window_name
                """,
                (ANALYSIS_VERSION,),
            )
            strategy = _rows(
                cursor,
                """
                SELECT e.event_type, r.symbol,
                       COUNT(*) AS rows,
                       COUNT(r.net_return_pct) AS eligible_rows,
                       COUNT(*) FILTER (
                           WHERE r.coverage_status = 'COMPLETE'
                       ) AS complete_rows,
                       ROUND(AVG(r.net_return_pct), 6) AS mean_net_return_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY r.net_return_pct
                       )::numeric, 6) AS median_net_return_pct,
                       ROUND(AVG((r.net_return_pct > 0)::int)::numeric, 6)
                           AS positive_net_ratio
                FROM event_strategy_results r
                JOIN economic_events e USING (economic_event_id)
                WHERE r.strategy_name = %s AND r.strategy_version = %s
                GROUP BY e.event_type, r.symbol
                ORDER BY e.event_type, r.symbol
                """,
                (STRATEGY_NAME, STRATEGY_VERSION),
            )
            overall = _rows(
                cursor,
                """
                SELECT COUNT(*) AS rows,
                       COUNT(net_return_pct) AS eligible_rows,
                       COUNT(*) FILTER (
                           WHERE coverage_status = 'COMPLETE'
                       ) AS complete_rows,
                       ROUND(AVG(net_return_pct), 6) AS mean_net_return_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY net_return_pct
                       )::numeric, 6) AS median_net_return_pct,
                       ROUND(AVG((net_return_pct > 0)::int)::numeric, 6)
                           AS positive_net_ratio
                FROM event_strategy_results
                WHERE strategy_name = %s AND strategy_version = %s
                """,
                (STRATEGY_NAME, STRATEGY_VERSION),
            )[0]
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "analysis_version": ANALYSIS_VERSION,
        "scope": {
            "event_types": ["CPI", "EMPLOYMENT", "PCE", "FOMC"],
            "events": 202,
            "symbols": 10,
            "event_symbol_pairs": 2020,
            "windows": ["PRE_60M", "POST_5M", "POST_30M", "POST_60M"],
        },
        "impact_coverage": impact_coverage,
        "event_return_summary": event_returns,
        "exploratory_strategy": {
            "name": STRATEGY_NAME,
            "version": STRATEGY_VERSION,
            "rule": "PRE_60M return > 0: long; < 0: short; exit after 60m",
            "round_trip_transaction_cost_bps": 10,
            "overall": overall,
            "by_event_type_and_symbol": strategy,
        },
        "interpretation_limits": [
            "The strategy uses only pre-release price direction, not consensus or surprise.",
            "Partial pre-market coverage is retained and labelled; complete-only count is separate.",
            "This is an exploratory baseline, not evidence of causality or expected profit.",
            "The 95% intervals are normal-approximation intervals and do not control overlapping events or multiple tests.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/evidence/multi-event-expansion/event-analysis.json"),
    )
    args = parser.parse_args()
    values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    summary = build_summary(database_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary["exploratory_strategy"]["overall"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
