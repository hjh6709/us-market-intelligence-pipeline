"""Export verified market bars to a local-only CSV and print a safe manifest."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path

import psycopg

from src.live_market_smoke import _read_env_file


DEFAULT_DATABASE_URL = "postgresql://market:market@localhost:55432/market"
LOCAL_DATA_ROOT = Path("data/local")
EXPECTED_RESULT = Path("docs/evidence/actual-ingestion/result.json")
CSV_FIELDS = (
    "symbol",
    "bar_start",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "source",
    "feed",
    "is_final",
)


def local_output_path(value: str) -> Path:
    output = Path(value)
    resolved_root = LOCAL_DATA_ROOT.resolve()
    if not output.resolve().is_relative_to(resolved_root):
        raise argparse.ArgumentTypeError(
            f"actual market data must stay under {LOCAL_DATA_ROOT}/"
        )
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export actual market bars locally without publishing market values."
    )
    parser.add_argument("--symbol", default="SMH")
    parser.add_argument("--start", default="2026-08-19T19:50:00Z")
    parser.add_argument("--end", default="2026-08-19T19:56:00Z")
    parser.add_argument("--source", default="alpaca")
    parser.add_argument("--feed", default="iex")
    parser.add_argument(
        "--output",
        type=local_output_path,
        default="data/local/actual_market_bars.csv",
    )
    return parser.parse_args()


def canonical_row(row: dict[str, object]) -> str:
    values = []
    for field in CSV_FIELDS:
        value = row[field]
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        values.append(str(value).lower() if isinstance(value, bool) else str(value))
    return ",".join(values)


def write_export(
    rows: list[dict[str, object]], output: Path, expected_sha256: str
) -> dict[str, object]:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    digest_input = "\n".join(canonical_row(row) for row in rows).encode("utf-8")
    actual_sha256 = hashlib.sha256(digest_input).hexdigest()
    return {
        "status": "exported_local_only",
        "row_count": len(rows),
        "first_bar_start": rows[0]["bar_start"].isoformat(),
        "last_bar_start": rows[-1]["bar_start"].isoformat(),
        "sha256": actual_sha256,
        "output": str(output),
        "git_ignored": True,
        "expected_sha256": expected_sha256,
        "hash_matches_expected": actual_sha256 == expected_sha256,
    }


def export_rows(args: argparse.Namespace) -> dict[str, object]:
    query = """
        SELECT symbol, bar_start, timeframe, open, high, low, close,
               volume, trade_count, vwap, source, feed, is_final
        FROM market_bars
        WHERE symbol = %s
          AND source = %s
          AND feed = %s
          AND bar_start >= %s::timestamptz
          AND bar_start < %s::timestamptz
        ORDER BY bar_start
    """
    database_url = (
        os.environ.get("DATABASE_URL")
        or _read_env_file(Path(".env")).get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute(
                query,
                (args.symbol, args.source, args.feed, args.start, args.end),
            )
            rows = cursor.fetchall()

    if not rows:
        raise RuntimeError("No market bars matched the requested evidence window.")

    expected = json.loads(EXPECTED_RESULT.read_text(encoding="utf-8"))
    return write_export(
        rows,
        args.output,
        expected["market_bars_sha256"],
    )


def main() -> int:
    args = parse_args()
    manifest = export_rows(args)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0 if manifest["hash_matches_expected"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
