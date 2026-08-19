"""Explicit Spark schemas for the canonical raw market envelope."""

from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)


ALPACA_TRADE_SCHEMA = StructType(
    [
        StructField("T", StringType(), True),
        StructField("S", StringType(), True),
        StructField("i", LongType(), True),
        StructField("x", StringType(), True),
        StructField("p", DecimalType(18, 6), True),
        StructField("s", LongType(), True),
        StructField("c", ArrayType(StringType(), containsNull=False), True),
        StructField("t", StringType(), True),
        StructField("z", StringType(), True),
    ]
)

MARKET_ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("schema_version", LongType(), True),
        StructField("source", StringType(), True),
        StructField("feed", StringType(), True),
        StructField("source_event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("trace_id", StringType(), True),
        StructField("payload", ALPACA_TRADE_SCHEMA, True),
    ]
)
