"""Backfill Alpaca SIP minute bars around configured CPI release times."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from src.cpi_ingestion import DEFAULT_DATABASE_URL, load_cpi_releases
from src.historical_bars import (
    AlpacaHistoricalBarsClient,
    fetch_all_bars,
    upsert_historical_bars,
)
from src.live_market_smoke import _read_env_file, load_credentials


DEFAULT_SYMBOLS = ("SPY", "QQQ", "SMH", "NVDA")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args(argv)
    if args.window_minutes <= 0:
        parser.error("--window-minutes must be positive")

    key_id, secret_key = load_credentials(env_path=args.env_file)
    file_values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or file_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    client = AlpacaHistoricalBarsClient(key_id, secret_key)
    all_bars = []
    total_pages = 0
    for release in load_cpi_releases():
        bars, pages = fetch_all_bars(
            client,
            symbols=args.symbols,
            start=release.released_at - timedelta(minutes=args.window_minutes),
            end=release.released_at + timedelta(minutes=args.window_minutes),
            feed="sip",
        )
        all_bars.extend(bars)
        total_pages += pages

    stored = upsert_historical_bars(
        all_bars,
        database_url=database_url,
        feed="sip",
    )
    counts = Counter(bar.symbol for bar in all_bars)
    print(
        json.dumps(
            {
                "events": 12,
                "symbols": list(args.symbols),
                "window_minutes_each_side": args.window_minutes,
                "pages": total_pages,
                "fetched_bars": len(all_bars),
                "stored_bars": stored,
                "bars_by_symbol": dict(sorted(counts.items())),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
