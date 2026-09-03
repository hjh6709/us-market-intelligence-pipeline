"""Collect verified SIP trades for official economic events and a symbol universe."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from src.economic_event_schedule import event_counts, load_event_catalog
from src.historical_market_replay import AlpacaHistoricalClient
from src.live_market_smoke import load_credentials
from src.market_event_archive_plan import build_archive_plan
from src.market_trade_archive import collect_archive_partition
from src.market_universe import load_market_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path("config/market_event_catalog.json"))
    parser.add_argument("--universe", type=Path, default=Path("config/market_universe.json"))
    parser.add_argument("--event-types", nargs="+", default=["CPI", "EMPLOYMENT", "PCE", "FOMC"])
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--release-from", default="2022-01-01")
    parser.add_argument("--release-to", default="2026-08-26")
    parser.add_argument("--minutes-before", type=int, default=60)
    parser.add_argument("--minutes-after", type=int, default=60)
    parser.add_argument("--feed", choices=["sip", "iex"], default="sip")
    parser.add_argument("--archive-root", type=Path, default=Path("data/archive"))
    parser.add_argument("--output", type=Path, default=Path("data/archive/market-event-dataset-manifest.json"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--max-pages", type=int, default=1_000)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate the plan and print counts without calling Alpaca",
    )
    return parser.parse_args()


def _selection(args: argparse.Namespace):
    requested_types = {item.strip().upper() for item in args.event_types}
    if not requested_types:
        raise ValueError("event-types must not be empty")
    catalog_releases = load_event_catalog(args.catalog)
    unknown = requested_types - {item.event_type for item in catalog_releases}
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
    plan = build_archive_plan(
        releases,
        symbols,
        minutes_before=args.minutes_before,
        minutes_after=args.minutes_after,
        feed=args.feed,
    )
    return releases, symbols, plan


def _summary(args: argparse.Namespace, releases, symbols, plan) -> dict:
    return {
        "schema_version": 1,
        "status": "PLANNED" if args.dry_run else "COLLECTED",
        "release_from": args.release_from,
        "release_to": args.release_to,
        "event_counts": event_counts(releases),
        "release_count": len(releases),
        "symbols": symbols,
        "symbol_count": len(symbols),
        "partition_count": len(plan),
        "window_minutes": {
            "before": args.minutes_before,
            "after": args.minutes_after,
            "expected_minute_buckets": args.minutes_before + args.minutes_after + 1,
        },
        "feed": args.feed,
    }


def main() -> int:
    args = parse_args()
    releases, symbols, plan = _selection(args)
    summary = _summary(args, releases, symbols, plan)
    if args.dry_run:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    key_id, secret_key = load_credentials(env_path=args.env_file)
    client = AlpacaHistoricalClient(key_id, secret_key, timeout_seconds=30.0)
    manifests = []
    for partition in plan:
        manifest = collect_archive_partition(
            client,
            partition,
            archive_root=args.archive_root,
            limit=args.limit,
            max_pages=args.max_pages,
        )
        manifests.append(manifest)
        print(
            json.dumps(
                {
                    "step": "archive_partition",
                    "event_type": partition.event_type,
                    "release_date": partition.release_date,
                    "symbol": partition.symbol,
                    "rows": manifest.row_count,
                    "pages": manifest.page_count,
                    "sha256": manifest.sha256,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    collected = {
        **summary,
        "raw_trade_count": sum(item.row_count for item in manifests),
        "collected_partition_counts": dict(
            sorted(Counter(item.partition.event_type for item in manifests).items())
        ),
        "partitions": [
            {
                "manifest": str(item.manifest_path.relative_to(args.archive_root)),
                "rows": item.row_count,
                "sha256": item.sha256,
            }
            for item in manifests
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(collected, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    print(json.dumps({"step": "archive_summary", **collected}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
