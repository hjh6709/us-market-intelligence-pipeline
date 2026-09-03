"""Collect session 1-minute bars and ±7-session daily context for market events."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.derived_bars import aggregate_derived_bars, upsert_derived_bars
from src.economic_event_schedule import event_counts, load_event_catalog
from src.historical_bars import (
    AlpacaHistoricalBarsClient,
    fetch_all_bars,
    upsert_historical_bars,
)
from src.live_market_smoke import _read_env_file, load_credentials
from src.market_event_context import (
    build_context_requests,
    available_request_end,
    select_daily_context,
    select_session_context,
)
from src.market_universe import load_market_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path("config/market_event_catalog.json"))
    parser.add_argument("--universe", type=Path, default=Path("config/market_universe.json"))
    parser.add_argument("--event-types", nargs="+", default=["CPI", "EMPLOYMENT", "PCE", "FOMC"])
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--release-from", default="2022-01-01")
    parser.add_argument("--release-to", default="2026-08-26")
    parser.add_argument("--feed", choices=["sip", "iex"], default="sip")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, default=Path("data/archive/market-event-context-manifest.json"))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _selection(args: argparse.Namespace):
    requested_types = {item.strip().upper() for item in args.event_types}
    catalog_releases = load_event_catalog(args.catalog)
    available_types = {item.event_type for item in catalog_releases}
    unknown = requested_types - available_types
    if unknown:
        raise ValueError(f"event-types are not in the catalog: {sorted(unknown)}")
    releases = [
        item
        for item in catalog_releases
        if item.event_type in requested_types
        and args.release_from <= item.release_date.isoformat() <= args.release_to
    ]
    if not releases:
        raise ValueError("no confirmed release falls inside the requested selection")
    symbols = (
        [item.strip().upper() for item in args.symbols]
        if args.symbols
        else [item.symbol for item in load_market_universe(args.universe)]
    )
    return releases, symbols, build_context_requests(releases, symbols)


def _planned_summary(args, releases, symbols, requests) -> dict:
    return {
        "schema_version": 1,
        "status": "PLANNED" if args.dry_run else "COLLECTED",
        "release_from": args.release_from,
        "release_to": args.release_to,
        "event_counts": event_counts(releases),
        "release_count": len(releases),
        "symbols": symbols,
        "symbol_count": len(symbols),
        "api_request_count_before_pagination": len(requests),
        "layers": {
            "SESSION_1MIN": {
                "window": "T-60m through T+120m inclusive",
                "expected_buckets_per_event_symbol": 181,
                "planned_max_rows": len(releases) * len(symbols) * 181,
                "derived_3m_max_rows": len(releases) * len(symbols) * 61,
                "derived_5m_max_rows": len(releases) * len(symbols) * 37,
            },
            "DAILY_15_SESSIONS": {
                "window": "7 observed sessions before + event session + 7 after",
                "expected_buckets_per_event_symbol": 15,
                "planned_max_rows": len(releases) * len(symbols) * 15,
            },
        },
        "feed": args.feed,
    }


def main() -> int:
    args = parse_args()
    releases, symbols, requests = _selection(args)
    summary = _planned_summary(args, releases, symbols, requests)
    if args.dry_run:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    key_id, secret_key = load_credentials(env_path=args.env_file)
    env_values = _read_env_file(args.env_file)
    database_url = os.environ.get("DATABASE_URL") or env_values.get("DATABASE_URL") or DEFAULT_DATABASE_URL
    client = AlpacaHistoricalBarsClient(key_id, secret_key, timeout_seconds=30.0)
    release_by_id = {release.event_id: release for release in releases}
    fetched_counts = Counter()
    upserted_counts = Counter()
    derived_counts = Counter()
    derived_partial_counts = Counter()
    total_pages = 0
    daily_coverage = []
    provider_available_until = datetime.now(UTC) - timedelta(minutes=20)

    for request in requests:
        request_end = available_request_end(request, provider_available_until)
        bars, pages = fetch_all_bars(
            client,
            symbols=request.symbols,
            start=request.start,
            end=request_end,
            feed=args.feed,
            timeframe=request.timeframe,
            max_pages=20,
        )
        total_pages += pages
        if request.layer == "SESSION_1MIN":
            selected_bars = select_session_context(bars, request)
        else:
            selected_bars = []
            release = release_by_id[request.event_id]
            for symbol in request.symbols:
                selection = select_daily_context(bars, release, symbol)
                selected_bars.extend(selection.bars)
                daily_coverage.append(
                    {
                        "event_id": request.event_id,
                        "symbol": symbol,
                        "before": selection.sessions_before,
                        "event": selection.event_session,
                        "after": selection.sessions_after,
                        "complete": selection.complete,
                    }
                )
        fetched_counts[request.layer] += len(selected_bars)
        upserted_counts[request.layer] += upsert_historical_bars(
            selected_bars,
            database_url=database_url,
            feed=args.feed,
            timeframe=request.timeframe,
        )
        if request.layer == "SESSION_1MIN":
            for minutes in (3, 5):
                derived = aggregate_derived_bars(selected_bars, minutes)
                upsert_derived_bars(
                    derived,
                    database_url=database_url,
                    feed=args.feed,
                )
                timeframe = f"{minutes}m"
                derived_counts[timeframe] += len(derived)
                derived_partial_counts[timeframe] += sum(
                    bar.coverage_status == "PARTIAL" for bar in derived
                )
        print(
            json.dumps(
                {
                    "step": "event_context",
                    "event_type": request.event_type,
                    "release_date": request.release_date,
                    "layer": request.layer,
                    "selected_bars": len(selected_bars),
                    "pages": pages,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    result = {
        **summary,
        "pages": total_pages,
        "selected_bar_counts": dict(sorted(fetched_counts.items())),
        "upsert_attempt_counts": dict(sorted(upserted_counts.items())),
        "derived_bar_counts": dict(sorted(derived_counts.items())),
        "derived_partial_counts": dict(sorted(derived_partial_counts.items())),
        "provider_available_until": provider_available_until.isoformat().replace(
            "+00:00", "Z"
        ),
        "daily_context_complete": sum(item["complete"] for item in daily_coverage),
        "daily_context_incomplete": sum(not item["complete"] for item in daily_coverage),
        "daily_coverage": daily_coverage,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps({"step": "context_summary", **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
