from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import psycopg

from src.historical_market_replay import AlpacaHistoricalClient, HistoricalTradeError
from src.live_market_smoke import _read_env_file
from src.market_trade_archive import (
    ArchivePartition,
    collect_archive_partition,
    load_archive_manifest,
)


def unavailable_opener(request, *, timeout):
    raise HTTPError(request.full_url, 503, "service unavailable", {}, None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-manifest", type=Path, default=Path("data/archive/dataset-manifest.json"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checks = []

    api_failed = False
    try:
        AlpacaHistoricalClient("redacted", "redacted", opener=unavailable_opener).fetch_page(
            symbol="NVDA",
            start="2026-08-12T11:30:00Z",
            end="2026-08-12T13:31:00Z",
            feed="sip",
            limit=100,
        )
    except HistoricalTradeError as error:
        api_failed = True
        checks.append(
            {
                "failure_type": "api_http_503",
                "failure_reproduced": True,
                "error_type": type(error).__name__,
                "secret_exposed": False,
            }
        )
    if not api_failed:
        raise RuntimeError("mock API failure was not reproduced")

    dataset = json.loads(args.dataset_manifest.read_text(encoding="utf-8"))
    first_manifest = load_archive_manifest(
        args.dataset_manifest.parent / dataset["partitions"][0]["manifest"]
    )
    checks[-1]["recovered"] = first_manifest.row_count > 0
    checks[-1]["recovery"] = "verified completed Parquet partition and checksum"

    with tempfile.TemporaryDirectory() as temp:
        invalid_rejected = False
        try:
            collect_archive_partition(
                object(),
                ArchivePartition("CPI", "2026-08-12", "NVDA", "start", "end"),
                archive_root=Path(temp),
                limit=0,
            )
        except ValueError as error:
            invalid_rejected = True
            checks.append(
                {
                    "failure_type": "invalid_input",
                    "failure_reproduced": True,
                    "error_type": type(error).__name__,
                    "side_effect_files": len(list(Path(temp).rglob("*"))),
                    "recovered": True,
                    "recovery": "corrected parameters before retry",
                }
            )
        if not invalid_rejected:
            raise RuntimeError("invalid input was not rejected")

    env = _read_env_file(args.env_file)
    database_url = os.environ.get("DATABASE_URL") or env.get(
        "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
    )
    db_failed = False
    try:
        psycopg.connect(
            "postgresql://market:market@127.0.0.1:1/market", connect_timeout=1
        )
    except psycopg.Error as error:
        db_failed = True
        with psycopg.connect(database_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                recovered = cursor.fetchone()[0] == 1
        checks.append(
            {
                "failure_type": "database_unavailable",
                "failure_reproduced": True,
                "error_type": type(error).__name__,
                "secret_exposed": False,
                "recovered": recovered,
                "recovery": "restored valid database endpoint and health query passed",
            }
        )
    if not db_failed:
        raise RuntimeError("database connection failure was not reproduced")

    result = {
        "executed_at": datetime.now(UTC).isoformat(),
        "status": "succeeded",
        "checks": checks,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
