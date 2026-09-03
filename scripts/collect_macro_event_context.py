from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from src.cpi_ingestion import _settings
from src.economic_event_ingestion import upsert_economic_events
from src.economic_event_schedule import load_event_catalog
from src.fred_client import FredClient
from src.macro_context_ingestion import (
    MACRO_SERIES,
    fetch_event_macro_context,
    upsert_event_macro_context,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-from", default="2022-01-01")
    parser.add_argument("--release-to", default="2026-08-26")
    parser.add_argument(
        "--event-types",
        nargs="+",
        choices=["CPI", "EMPLOYMENT", "PCE", "FOMC"],
        default=["CPI", "EMPLOYMENT", "PCE", "FOMC"],
    )
    parser.add_argument("--request-interval", type=float, default=0.55)
    parser.add_argument(
        "--series",
        nargs="+",
        choices=sorted(MACRO_SERIES),
        default=sorted(MACRO_SERIES),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fred_api_key, database_url = _settings(os.environ, args.env_file)
    requested_types = set(args.event_types)
    releases = [
        item
        for item in load_event_catalog()
        if item.event_type in requested_types
        and args.release_from <= item.release_date.isoformat() <= args.release_to
    ]
    if not releases:
        raise ValueError("no confirmed economic release falls inside the requested range")

    client = FredClient(fred_api_key, timeout_seconds=30.0)
    selected_series = {series_id: MACRO_SERIES[series_id] for series_id in args.series}
    contexts = []
    for release in releases:
        event_contexts = fetch_event_macro_context(
            client,
            [release],
            series=selected_series,
            request_interval_seconds=args.request_interval,
        )
        contexts.extend(event_contexts)
        print(
            json.dumps(
                {
                    "step": "macro_event_context",
                    "event_type": release.event_type,
                    "release_date": release.release_date.isoformat(),
                    "series_count": len(event_contexts),
                }
            ),
            flush=True,
        )

    events = upsert_economic_events(releases, database_url=database_url)
    stored_contexts = upsert_event_macro_context(contexts, database_url=database_url)
    print(
        json.dumps(
            {
                "step": "macro_summary",
                "economic_events": events,
                "macro_event_contexts": stored_contexts,
                "series_count": len(selected_series),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
