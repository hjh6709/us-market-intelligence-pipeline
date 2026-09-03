"""Export a public, price-free summary of the multi-event expansion run."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
import os
from pathlib import Path

import psycopg

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--context-manifest",
        type=Path,
        default=Path("data/archive/market-event-context-manifest-v2.json"),
    )
    parser.add_argument(
        "--partition-result",
        type=Path,
        default=Path("docs/evidence/load-recovery/v2-partition-routing.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/evidence/multi-event-expansion/full-expansion-summary.json"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def _database_url(env_file: Path) -> str:
    values = _read_env_file(env_file)
    return os.environ.get("DATABASE_URL") or values.get("DATABASE_URL") or DEFAULT_DATABASE_URL


def main() -> int:
    args = parse_args()
    context = json.loads(args.context_manifest.read_text(encoding="utf-8"))
    partition = json.loads(args.partition_result.read_text(encoding="utf-8"))
    if context.get("status") != "COLLECTED":
        raise ValueError("market context manifest is not a completed collection")
    if partition.get("status") != "succeeded":
        raise ValueError("Kafka v2 partition experiment did not succeed")

    database_counts: dict[str, object] = {}
    with psycopg.connect(_database_url(args.env_file), connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT event_type, COUNT(*) FROM economic_events GROUP BY event_type ORDER BY event_type"
            )
            database_counts["economic_events_by_type"] = dict(cursor.fetchall())
            cursor.execute(
                """
                SELECT event.event_type, COUNT(context.series_id)
                FROM economic_events AS event
                LEFT JOIN macro_event_contexts AS context
                  USING (economic_event_id)
                GROUP BY event.event_type
                ORDER BY event.event_type
                """
            )
            database_counts["macro_contexts_by_event_type"] = dict(cursor.fetchall())
            cursor.execute(
                """
                SELECT timeframe, COUNT(*)
                FROM market_bars
                WHERE source = 'alpaca' AND feed = 'sip'
                GROUP BY timeframe
                ORDER BY timeframe
                """
            )
            database_counts["alpaca_sip_market_bars_by_timeframe"] = dict(
                cursor.fetchall()
            )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT symbol, bar_start, timeframe, source, feed
                    FROM market_bars
                    GROUP BY symbol, bar_start, timeframe, source, feed
                    HAVING COUNT(*) > 1
                ) duplicated
                """
            )
            database_counts["market_bar_business_key_duplicates"] = int(
                cursor.fetchone()[0]
            )

    coverage = Counter(item["coverage_status"] for item in context["daily_coverage"])
    summary = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "contains_prices": False,
        "market_context": {
            key: context[key]
            for key in (
                "release_from",
                "release_to",
                "event_counts",
                "release_count",
                "symbols",
                "symbol_count",
                "work_item_count",
                "pages",
                "selected_bar_counts",
                "derived_bar_counts",
                "derived_partial_counts",
            )
        },
        "coverage_status_counts": dict(sorted(coverage.items())),
        "kafka_v2_spark_validation": {
            key: partition[key]
            for key in (
                "experiment_run_id",
                "raw_input_trades",
                "kafka_published",
                "kafka_consumed",
                "spark_input",
                "spark_invalid",
                "spark_duplicates",
                "spark_output_bars",
                "postgres_stored_bars",
                "postgres_business_key_duplicates",
                "duration_seconds",
                "events_per_second",
                "kafka_partition_counts",
                "kafka_max_partition_share",
            )
        },
        "database": database_counts,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
