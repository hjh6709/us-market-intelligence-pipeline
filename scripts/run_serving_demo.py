#!/usr/bin/env python3
import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file
from src.serving_demo import run_serving_demo


def redact_error(value: str) -> str:
    return re.sub(r"postgres(?:ql)?://[^\s]+", "[database-url-redacted]", value)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recalculate, store, and read one stored event-symbol result."
    )
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    env_values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or env_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    try:
        result = run_serving_demo(database_url, args.event_id, args.symbol)
        payload = result.model_dump_json(indent=2) + "\n"
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=args.output.parent,
                prefix=f".{args.output.name}.",
                delete=False,
            ) as handle:
                handle.write(payload)
                temporary_path = Path(handle.name)
            temporary_path.replace(args.output)
        print(payload, end="")
        return 0
    except Exception as error:
        print(f"serving demo failed: {redact_error(str(error))}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
