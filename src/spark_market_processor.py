"""Kafka-consuming Spark Structured Streaming market processor."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.live_market_smoke import _read_env_file
from src.preprocess import (
    aggregate_minute_bars,
    parse_market_events,
    prepare_streaming_trades,
    split_valid_invalid,
    validate_market_trades,
)
from src.postgres import postgres_bar_sink


KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def _load_setting(name: str, default: str, env_path: Path = Path(".env")) -> str:
    return os.environ.get(name) or _read_env_file(env_path).get(name) or default


def checkpoint_paths(root: Path) -> tuple[Path, Path]:
    return root / "bars", root / "invalid-metrics"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=_load_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic", default=_load_setting("KAFKA_TOPIC", "raw.market.v1")
    )
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "NVDA"])
    parser.add_argument("--starting-offsets", choices=("latest", "earliest"), default="latest")
    parser.add_argument("--watermark", default="2 minutes")
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path(
            _load_setting(
                "SPARK_CHECKPOINT_ROOT", ".spark-checkpoints/market-processor"
            )
        ),
    )
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument(
        "--bar-sink",
        choices=("postgres", "console"),
        default=_load_setting("BAR_SINK", "postgres"),
    )
    parser.set_defaults(
        database_url=_load_setting(
            "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
        )
    )
    parser.add_argument("--timeout", type=float, default=None)
    return parser.parse_args(argv)


def create_market_spark(app_name: str = "market-processor") -> SparkSession:
    driver_memory = _load_setting("SPARK_DRIVER_MEMORY", "2g")
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_streams(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
    symbols: Sequence[str],
    watermark: str,
) -> tuple[DataFrame, DataFrame]:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
        .select("value", "topic", "partition", "offset", "timestamp")
    )
    validated = validate_market_trades(parse_market_events(raw), symbols)
    valid, invalid = split_valid_invalid(validated)
    bars = aggregate_minute_bars(prepare_streaming_trades(valid, watermark))
    return bars, invalid


def summarize_invalid_reasons(invalid_df: DataFrame) -> DataFrame:
    """Count bounded invalid reasons without retaining raw payload state."""
    return (
        invalid_df.select(F.explode("reason_codes").alias("reason"))
        .groupBy("reason")
        .count()
    )


def _show_invalid_batch(batch_df: DataFrame, batch_id: int) -> None:
    summarize_invalid_reasons(batch_df).withColumn(
        "spark_batch_id", F.lit(batch_id)
    ).show(truncate=False)


def run_processor(args: argparse.Namespace) -> None:
    spark = create_market_spark()
    bars_checkpoint, invalid_checkpoint = checkpoint_paths(args.checkpoint_root)
    bars, invalid_rows = build_streams(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
        symbols=args.symbols,
        watermark=args.watermark,
    )
    queries = []
    try:
        bar_writer = (
            bars.writeStream.queryName("final-market-bars")
            .outputMode("append")
            .option("checkpointLocation", str(bars_checkpoint))
            .trigger(processingTime=args.trigger)
        )
        if args.bar_sink == "postgres":
            bar_query = bar_writer.foreachBatch(
                postgres_bar_sink(args.database_url)
            ).start()
        else:
            bar_query = bar_writer.format("console").option(
                "truncate", "false"
            ).start()
        queries.append(bar_query)
        queries.append(
            invalid_rows.writeStream.queryName("invalid-market-metrics")
            .foreachBatch(_show_invalid_batch)
            .outputMode("append")
            .option("checkpointLocation", str(invalid_checkpoint))
            .trigger(processingTime=args.trigger)
            .start()
        )
        if args.timeout is None:
            spark.streams.awaitAnyTermination()
        else:
            spark.streams.awaitAnyTermination(args.timeout)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


def main() -> int:
    run_processor(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
