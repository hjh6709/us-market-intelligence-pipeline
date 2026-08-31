from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from src.cpi_ingestion import load_cpi_releases, upsert_cpi_data, _settings
from src.fred_client import FredClient, MacroObservation
from src.macro_context_ingestion import (
    fetch_event_macro_context,
    upsert_event_macro_context,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-from", default="2022-01-01")
    parser.add_argument("--release-to", default="2026-08-12")
    parser.add_argument("--request-interval", type=float, default=0.55)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fred_api_key, database_url = _settings(os.environ, args.env_file)
    releases = [
        item
        for item in load_cpi_releases()
        if args.release_from <= item.release_date.isoformat() <= args.release_to
    ]
    if not releases:
        raise ValueError("no confirmed CPI release falls inside the requested range")

    client = FredClient(fred_api_key, timeout_seconds=30.0)
    contexts = []
    for release in releases:
        event_contexts = fetch_event_macro_context(
            client,
            [release],
            request_interval_seconds=args.request_interval,
        )
        contexts.extend(event_contexts)
        print(
            json.dumps(
                {
                    "step": "macro_event_context",
                    "release_date": release.release_date.isoformat(),
                    "series_count": len(event_contexts),
                }
            ),
            flush=True,
        )

    release_by_event = {item.event_id: item for item in releases}
    cpi_observations = [
        (
            release_by_event[item.economic_event_id],
            MacroObservation(
                series_id=item.series_id,
                observation_date=item.observation_date,
                realtime_start=item.realtime_start,
                realtime_end=item.realtime_end,
                value=item.value,
            ),
        )
        for item in contexts
        if item.series_id in {"CPIAUCSL", "CPILFESL"}
    ]
    events, observations = upsert_cpi_data(
        releases, cpi_observations, database_url=database_url
    )
    stored_contexts = upsert_event_macro_context(contexts, database_url=database_url)
    print(
        json.dumps(
            {
                "step": "macro_summary",
                "economic_events": events,
                "macro_observations": observations,
                "macro_event_contexts": stored_contexts,
                "series_count": 10,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
