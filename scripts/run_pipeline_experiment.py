from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

from src.live_market_smoke import _read_env_file
from src.market_trade_archive import load_archive_manifest
from src.pipeline_experiment import run_experiment, write_result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-manifest", type=Path, default=Path("data/archive/dataset-manifest.json"))
    parser.add_argument("--release-from", required=True)
    parser.add_argument("--release-to", required=True)
    parser.add_argument("--environment", choices=("local", "gcp"), required=True)
    parser.add_argument("--topic", default="raw.market-sip.load.v1")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = json.loads(args.dataset_manifest.read_text(encoding="utf-8"))
    archive_root = args.dataset_manifest.parent
    manifests = []
    for item in dataset["partitions"]:
        manifest = load_archive_manifest(archive_root / item["manifest"])
        release_date = manifest.partition.release_date
        if args.release_from <= release_date <= args.release_to:
            manifests.append(manifest)
    if not manifests:
        raise ValueError("the selected release range contains no archived partitions")

    env = _read_env_file(args.env_file)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or env.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    database_url = os.environ.get("DATABASE_URL") or env.get(
        "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
    )
    result = run_experiment(
        manifests,
        dataset_id=f"{dataset['dataset_id']}:{args.release_from}:{args.release_to}",
        environment=args.environment,
        bootstrap_servers=bootstrap,
        topic=args.topic,
        database_url=database_url,
        experiment_run_id=args.run_id,
    )
    write_result(result, args.output)
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
