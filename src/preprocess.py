"""Reusable Spark transformations for raw Alpaca market trades."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, TimestampType

from src.spark_schemas import MARKET_ENVELOPE_SCHEMA


MINUTE_BAR_CONDITION_POLICY = "alpaca_sip_minute_v1"

# Alpaca follows the CTA/UTP sale-condition matrix when constructing minute
# bars. An empty condition array represents an otherwise regular trade in the
# normalized API payload. Unknown condition/tape combinations are excluded
# instead of silently changing OHLCV semantics.
_SUPPORTED_CONDITIONS_BY_TAPE = {
    "A": [
        "", "B", "C", "E", "F", "H", "I", "K", "L", "M", "N", "O",
        "P", "Q", "R", "T", "U", "V", "X", "Z", "4", "5", "6", "7", "9",
    ],
    "B": [
        "", "B", "C", "E", "F", "H", "I", "K", "L", "M", "N", "O",
        "P", "Q", "R", "T", "U", "V", "X", "Z", "4", "5", "6", "7", "9",
    ],
    "C": [
        "", "@", "A", "B", "C", "D", "F", "G", "H", "I", "K", "L",
        "M", "N", "O", "P", "Q", "R", "T", "U", "V", "W", "X", "Y",
        "Z", "4", "5", "6", "7", "9",
    ],
    "O": ["", "@", "C", "I", "N", "P", "R", "T", "U", "W"],
}

_PRICE_ONLY_EXCLUSIONS_BY_TAPE = {
    "A": ["B", "C", "H", "I", "N", "P", "R", "U", "V", "Z", "4", "7"],
    "B": ["B", "C", "H", "I", "N", "P", "R", "U", "V", "Z", "4", "7"],
    "C": ["C", "G", "H", "I", "N", "P", "R", "U", "V", "W", "Z", "4", "7"],
    "O": ["C", "I", "N", "P", "R", "U", "W"],
}

_PRICE_AND_VOLUME_EXCLUSIONS = ["M", "Q", "9"]


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
    blank_source = F.col("source").isNull() | (F.trim("source") == "")
    blank_feed = F.col("feed").isNull() | (F.trim("feed") == "")
    source_id_mismatch = (
        F.col("source_event_id").isNotNull()
        & F.col("provider_trade_id").isNotNull()
        & (
            F.trim("source_event_id")
            != F.col("provider_trade_id").cast("string")
        )
    )
    timezone_suffix = r"(?i)(Z|[+-][0-9]{2}:[0-9]{2})$"
    invalid_timestamp = (
        F.col("envelope_timestamp").isNull()
        | F.col("event_timestamp").isNull()
        | ~F.col("envelope_timestamp_raw").rlike(timezone_suffix)
        | ~F.col("payload_timestamp_raw").rlike(timezone_suffix)
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
            (blank_source, "MISSING_SOURCE"),
            (blank_feed, "MISSING_FEED"),
            (source_id_mismatch, "SOURCE_EVENT_ID_MISMATCH"),
            (
                F.col("symbol").isNull() | ~F.col("symbol").isin(allowlist),
                "SYMBOL_NOT_ALLOWED",
            ),
            (
                F.col("provider_trade_id").isNull(),
                "INVALID_PROVIDER_TRADE_ID",
            ),
            (
                F.col("exchange").isNull() | (F.trim("exchange") == ""),
                "INVALID_EXCHANGE",
            ),
            (F.col("conditions").isNull(), "INVALID_CONDITIONS"),
            (
                F.col("tape").isNull() | (F.trim("tape") == ""),
                "INVALID_TAPE",
            ),
            (F.col("price").isNull() | (F.col("price") <= 0), "INVALID_PRICE"),
            (F.col("size").isNull() | (F.col("size") <= 0), "INVALID_SIZE"),
            (invalid_timestamp, "INVALID_TIMESTAMP"),
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


def prepare_streaming_trades(
    valid_df: DataFrame,
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    """Bound streaming deduplication state by the event-time watermark."""
    if not valid_df.isStreaming:
        raise ValueError("prepare_streaming_trades requires a streaming DataFrame")
    return valid_df.withWatermark(
        "event_timestamp", watermark_delay
    ).dropDuplicatesWithinWatermark(["event_id"])


def _condition_codes_for_tape(mapping: dict[str, list[str]]) -> Column:
    result = F.array().cast(ArrayType(StringType(), containsNull=False))
    for tape, codes in mapping.items():
        result = F.when(
            F.col("tape") == tape,
            F.array(*[F.lit(code) for code in codes]),
        ).otherwise(result)
    return result


def apply_minute_bar_condition_policy(trades_df: DataFrame) -> DataFrame:
    """Mark whether each trade updates minute-bar price and volume fields.

    The policy implements Alpaca's documented CTA/UTP condition matrix for
    minute bars. With multiple condition codes, the strictest rule wins.
    See: https://docs.alpaca.markets/us/docs/market-data-faq
    """
    normalized_conditions = F.transform(
        F.col("conditions"), lambda code: F.upper(F.trim(code))
    )
    supported_conditions = _condition_codes_for_tape(_SUPPORTED_CONDITIONS_BY_TAPE)
    price_only_exclusions = _condition_codes_for_tape(
        _PRICE_ONLY_EXCLUSIONS_BY_TAPE
    )
    unsupported_conditions = F.array_except(
        normalized_conditions, supported_conditions
    )
    fully_excluded = (
        F.col("tape").isin("A", "B", "C")
        & (
            F.size(
                F.array_intersect(
                    normalized_conditions,
                    F.array(
                        *[
                            F.lit(code)
                            for code in _PRICE_AND_VOLUME_EXCLUSIONS
                        ]
                    ),
                )
            )
            > 0
        )
    )
    price_excluded = (
        F.size(F.array_intersect(normalized_conditions, price_only_exclusions)) > 0
    )
    supported = F.size(unsupported_conditions) == 0

    return (
        trades_df.withColumn("normalized_conditions", normalized_conditions)
        .withColumn("unsupported_conditions", unsupported_conditions)
        .withColumn("updates_volume", supported & ~fully_excluded)
        .withColumn(
            "updates_ohlc",
            supported & ~fully_excluded & ~price_excluded,
        )
    )


def aggregate_minute_bars(trades_df: DataFrame) -> DataFrame:
    """Aggregate normalized trades using provider-compatible SIP semantics."""
    policy_trades = apply_minute_bar_condition_policy(trades_df)
    order_key = F.struct(F.col("event_timestamp"), F.col("event_id"))
    group_columns = [
        "symbol",
        "source",
        "feed",
        F.window("event_timestamp", "1 minute").alias("bar_window"),
    ]
    price_bars = policy_trades.filter("updates_ohlc").groupBy(*group_columns).agg(
        F.min_by("price", order_key).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order_key).alias("close"),
    )
    volume_bars = policy_trades.filter("updates_volume").groupBy(*group_columns).agg(
        F.sum("size").alias("volume"),
        F.count(F.lit(1)).alias("trade_count"),
        F.sum(
            F.when(F.col("updates_ohlc"), F.col("price") * F.col("size"))
            .otherwise(F.lit(0))
        ).alias("vwap_notional"),
        F.sum(
            F.when(F.col("updates_ohlc"), F.col("size")).otherwise(F.lit(0))
        ).alias("vwap_volume"),
    )
    grouped = price_bars.join(
        volume_bars,
        on=["symbol", "source", "feed", "bar_window"],
        how="inner",
    )
    return grouped.select(
        "symbol",
        F.col("bar_window.start").alias("bar_start"),
        F.lit("1m").alias("timeframe"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        F.when(
            F.col("vwap_volume") > 0,
            (F.col("vwap_notional") / F.col("vwap_volume")).cast(
                "decimal(18,6)"
            ),
        )
        .otherwise(F.lit(None).cast("decimal(18,6)"))
        .alias("vwap"),
        "source",
        "feed",
        F.lit(True).alias("is_final"),
        F.lit(MINUTE_BAR_CONDITION_POLICY).alias("condition_policy"),
    )
