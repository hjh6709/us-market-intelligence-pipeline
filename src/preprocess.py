"""Reusable Spark transformations for raw Alpaca market trades."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, TimestampType

from src.spark_schemas import MARKET_ENVELOPE_SCHEMA


def _optional_column(
    frame: DataFrame,
    name: str,
    data_type: StringType | IntegerType | LongType | TimestampType,
) -> Column:
    if name in frame.columns:
        return F.col(name).cast(data_type).alias(f"kafka_{name}")
    return F.lit(None).cast(data_type).alias(f"kafka_{name}")


def parse_market_events(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka values and normalize required Alpaca trade fields."""
    raw_value = F.col("value").cast("string")
    parsed = F.from_json(raw_value, MARKET_ENVELOPE_SCHEMA)
    base = kafka_df.select(
        raw_value.alias("raw_value"),
        _optional_column(kafka_df, "topic", StringType()),
        _optional_column(kafka_df, "partition", IntegerType()),
        _optional_column(kafka_df, "offset", LongType()),
        _optional_column(kafka_df, "timestamp", TimestampType()),
        parsed.alias("envelope"),
        F.get_json_object(raw_value, "$").isNull().alias("malformed_json"),
    )
    return base.select(
        "raw_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "malformed_json",
        F.col("envelope.event_id").alias("event_id"),
        F.col("envelope.event_type").alias("event_type"),
        F.col("envelope.schema_version").alias("schema_version"),
        F.col("envelope.source").alias("source"),
        F.col("envelope.feed").alias("feed"),
        F.col("envelope.source_event_id").alias("source_event_id"),
        F.col("envelope.event_timestamp").alias("envelope_timestamp_raw"),
        F.try_to_timestamp(F.col("envelope.event_timestamp")).alias(
            "envelope_timestamp"
        ),
        F.col("envelope.ingested_at").alias("ingested_at_raw"),
        F.col("envelope.trace_id").alias("trace_id"),
        F.col("envelope.payload.T").alias("raw_message_type"),
        F.col("envelope.payload.S").alias("symbol"),
        F.col("envelope.payload.i").alias("provider_trade_id"),
        F.col("envelope.payload.x").alias("exchange"),
        F.col("envelope.payload.p").alias("price"),
        F.col("envelope.payload.s").alias("size"),
        F.col("envelope.payload.c").alias("conditions"),
        F.col("envelope.payload.t").alias("payload_timestamp_raw"),
        F.try_to_timestamp(F.col("envelope.payload.t")).alias("event_timestamp"),
        F.col("envelope.payload.z").alias("tape"),
    )


def _reason_array(conditions: Sequence[tuple[Column, str]]) -> Column:
    candidates = F.array(
        *[F.when(condition, F.lit(reason)) for condition, reason in conditions]
    )
    return F.filter(candidates, lambda reason: reason.isNotNull())


def validate_market_trades(
    parsed_df: DataFrame,
    allowed_symbols: Sequence[str],
) -> DataFrame:
    """Attach all applicable bounded validation reasons to each row."""
    allowlist = [symbol.upper() for symbol in allowed_symbols]
    blank_event_id = F.col("event_id").isNull() | (F.trim("event_id") == "")
    blank_source_id = F.col("source_event_id").isNull() | (
        F.trim("source_event_id") == ""
    )
    timestamp_mismatch = (
        F.col("envelope_timestamp").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & (F.col("envelope_timestamp") != F.col("event_timestamp"))
    )
    reasons = _reason_array(
        [
            (F.col("malformed_json"), "MALFORMED_JSON"),
            (
                (F.col("event_type") != "market.trade.raw")
                | (F.col("raw_message_type") != "t")
                | F.col("event_type").isNull()
                | F.col("raw_message_type").isNull(),
                "INVALID_EVENT_TYPE",
            ),
            (
                F.col("schema_version").isNull() | (F.col("schema_version") != 1),
                "UNSUPPORTED_SCHEMA_VERSION",
            ),
            (blank_event_id, "MISSING_EVENT_ID"),
            (blank_source_id, "MISSING_SOURCE_EVENT_ID"),
            (
                F.col("symbol").isNull() | ~F.col("symbol").isin(allowlist),
                "SYMBOL_NOT_ALLOWED",
            ),
            (F.col("price").isNull() | (F.col("price") <= 0), "INVALID_PRICE"),
            (F.col("size").isNull() | (F.col("size") < 0), "INVALID_SIZE"),
            (
                F.col("envelope_timestamp").isNull()
                | F.col("event_timestamp").isNull(),
                "INVALID_TIMESTAMP",
            ),
            (timestamp_mismatch, "TIMESTAMP_MISMATCH"),
        ]
    )
    return parsed_df.withColumn("reason_codes", reasons)


def split_valid_invalid(validated_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split validated trades without collecting or logging raw payloads."""
    return (
        validated_df.filter(F.size("reason_codes") == 0),
        validated_df.filter(F.size("reason_codes") > 0),
    )
