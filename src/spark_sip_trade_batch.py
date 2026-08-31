"""Batch-process one traced SIP trade replay from Kafka into minute bars."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from pyspark.sql import functions as F
from pyspark import StorageLevel

from src.postgres import upsert_market_bars
from src.preprocess import (
    aggregate_minute_bars,
    apply_minute_bar_condition_policy,
    parse_market_events,
    split_valid_invalid,
    validate_market_trades,
)
from src.spark_market_processor import _load_setting, create_market_spark


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-id", required=True)
    parser.add_argument("--topic", default="raw.market-sip.v1")
    parser.add_argument("--symbols", nargs="+", default=["NVDA"])
    parser.add_argument("--offset-ranges", required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default=_load_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.set_defaults(
        database_url=_load_setting(
            "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
        )
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, int | str]:
    offset_ranges = (
        json.loads(args.offset_ranges)
        if isinstance(args.offset_ranges, str)
        else args.offset_ranges
    )
    assignment, starting_offsets, ending_offsets = _spark_offset_json(
        args.topic, offset_ranges
    )
    spark = create_market_spark("cpi-sip-trade-batch")
    try:
        kafka = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("assign", assignment)
            .option("startingOffsets", starting_offsets)
            .option("endingOffsets", ending_offsets)
            .option("failOnDataLoss", "true")
            .load()
            .select("value", "topic", "partition", "offset", "timestamp")
        )
        traced = parse_market_events(kafka).filter(F.col("trace_id") == args.trace_id)
        validated = validate_market_trades(traced, args.symbols).persist(
            StorageLevel.DISK_ONLY
        )
        valid, invalid = split_valid_invalid(validated)
        input_count = validated.count()
        invalid_count = invalid.count()
        deduplicated = valid.dropDuplicates(["event_id"])
        deduplicated_count = deduplicated.count()
        policy_trades = apply_minute_bar_condition_policy(deduplicated).persist(
            StorageLevel.DISK_ONLY
        )
        unsupported_count = policy_trades.filter(
            F.size("unsupported_conditions") > 0
        ).count()
        volume_eligible_count = policy_trades.filter("updates_volume").count()
        price_eligible_count = policy_trades.filter("updates_ohlc").count()
        bars = aggregate_minute_bars(deduplicated).withColumn(
            "source", F.lit("alpaca_replay")
        )
        output_count = bars.count()
        stored_count = upsert_market_bars(bars, 0, database_url=args.database_url)
        return {
            "step": "spark_summary",
            "topic": args.topic,
            "trace_id": args.trace_id,
            "spark_input_trades": input_count,
            "spark_invalid_trades": invalid_count,
            "spark_valid_unique_trades": deduplicated_count,
            "spark_unsupported_condition_trades": unsupported_count,
            "spark_volume_eligible_trades": volume_eligible_count,
            "spark_price_eligible_trades": price_eligible_count,
            "spark_condition_excluded_from_trade_count": (
                deduplicated_count - volume_eligible_count
            ),
            "spark_output_bars": output_count,
            "postgres_upserted_bars": stored_count,
        }
    finally:
        try:
            spark.stop()
        except (ConnectionRefusedError, OSError):
            # The JVM may already be gone after a Java heap failure.
            pass


def _spark_offset_json(topic: str, offset_ranges) -> tuple[str, str, str]:
    starts = {}
    ends = {}
    partitions = []
    for item in offset_ranges:
        if item["topic"] != topic:
            raise ValueError("offset range topic does not match Spark topic")
        partition = str(int(item["partition"]))
        partitions.append(int(item["partition"]))
        starts[partition] = int(item["start"])
        ends[partition] = int(item["end"])
    if not starts:
        raise ValueError("offset_ranges must not be empty")
    return (
        json.dumps({topic: partitions}, separators=(",", ":")),
        json.dumps({topic: starts}, separators=(",", ":")),
        json.dumps({topic: ends}, separators=(",", ":")),
    )


def main(argv: Sequence[str] | None = None) -> int:
    print(json.dumps(run(parse_args(argv)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
