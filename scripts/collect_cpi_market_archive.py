from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path

from src.cpi_ingestion import load_cpi_releases
from src.historical_market_replay import AlpacaHistoricalClient
from src.live_market_smoke import load_credentials
from src.market_trade_archive import ArchivePartition, collect_archive_partition


def rfc3339(value) -> str:
    return value.isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "SMH", "NVDA"])
    parser.add_argument("--release-from", default="2022-01-01")
    parser.add_argument("--release-to", default="2026-08-12")
    parser.add_argument("--archive-root", type=Path, default=Path("data/archive"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--max-pages", type=int, default=1_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [item.strip().upper() for item in args.symbols]
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("symbols must be a non-empty unique list")
    releases = [
        item
        for item in load_cpi_releases()
        if args.release_from <= item.release_date.isoformat() <= args.release_to
    ]
    if not releases:
        raise ValueError("no confirmed CPI release falls inside the requested range")

    key_id, secret_key = load_credentials(env_path=args.env_file)
    client = AlpacaHistoricalClient(key_id, secret_key, timeout_seconds=30.0)
    manifests = []
    for release in releases:
        for symbol in symbols:
            partition = ArchivePartition(
                event_type="CPI",
                release_date=release.release_date.isoformat(),
                symbol=symbol,
                start=rfc3339(release.released_at - timedelta(minutes=60)),
                end=rfc3339(release.released_at + timedelta(minutes=61)),
                feed="sip",
            )
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
                        "release_date": partition.release_date,
                        "symbol": symbol,
                        "rows": manifest.row_count,
                        "pages": manifest.page_count,
                        "sha256": manifest.sha256,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    result = {
        "schema_version": 1,
        "dataset_id": f"cpi-sip-{args.release_from}-{args.release_to}",
        "release_from": args.release_from,
        "release_to": args.release_to,
        "release_count": len(releases),
        "symbols": symbols,
        "partition_count": len(manifests),
        "raw_trade_count": sum(item.row_count for item in manifests),
        "partitions": [
            {
                "manifest": str(item.manifest_path.relative_to(args.archive_root)),
                "rows": item.row_count,
                "sha256": item.sha256,
            }
            for item in manifests
        ],
    }
    output = args.archive_root / "dataset-manifest.json"
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(output)
    print(json.dumps({"step": "archive_summary", **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
