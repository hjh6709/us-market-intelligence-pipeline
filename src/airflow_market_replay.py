"""Shared contracts for the parameterized Airflow market replay DAG."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.live_market_smoke import _read_env_file


TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
ALLOWED_FEEDS = frozenset({"iex", "sip"})
TOPIC_BY_FEED = {
    "iex": "raw.market.v1",
    "sip": "raw.market-sip.v1",
}


@dataclass(frozen=True)
class MarketReplayConfig:
    ticker: str
    start: str
    end: str
    feed: str
    trace_id: str


def validate_run_config(
    params: Mapping[str, Any],
    *,
    run_id: str,
) -> MarketReplayConfig:
    """Validate one manual DAG run and derive its stable Kafka trace ID."""
    ticker = str(params.get("ticker", "")).strip()
    if not TICKER_PATTERN.fullmatch(ticker):
        raise ValueError("ticker must be a valid uppercase market symbol")

    feed = str(params.get("feed", "")).strip().lower()
    if feed not in ALLOWED_FEEDS:
        raise ValueError("feed must be either iex or sip")

    start = str(params.get("start", "")).strip()
    end = str(params.get("end", "")).strip()
    start_at = _parse_aware_timestamp(start)
    end_at = _parse_aware_timestamp(end)
    if start_at >= end_at:
        raise ValueError("start must be before end")
    if not run_id:
        raise ValueError("run_id is required")

    digest = hashlib.sha256(f"{run_id}|{ticker}".encode("utf-8")).hexdigest()[:16]
    return MarketReplayConfig(
        ticker=ticker,
        start=start,
        end=end,
        feed=feed,
        trace_id=f"airflow-market-replay-{digest}",
    )


def _parse_aware_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("start and end must be ISO 8601 timestamps") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("start and end must include a timezone")
    return parsed


def build_replay_args(
    config: MarketReplayConfig,
    *,
    env_file: Path = Path(".env"),
) -> argparse.Namespace:
    return argparse.Namespace(
        symbol=config.ticker,
        start=config.start,
        end=config.end,
        feed=config.feed,
        topic=TOPIC_BY_FEED[config.feed],
        limit=10_000,
        max_pages=100,
        trace_id=config.trace_id,
        speed_multiplier=None,
        env_file=env_file,
    )


def build_consumer_args(
    replay_summary: Mapping[str, Any],
    *,
    env_file: Path = Path(".env"),
    timeout_seconds: float = 120.0,
) -> argparse.Namespace:
    published = int(replay_summary["published_trades"])
    if published < 1:
        raise ValueError("published_trades must be positive")
    return argparse.Namespace(
        trace_id=str(replay_summary["trace_id"]),
        expected_count=published,
        topic=str(replay_summary["topic"]),
        offset_ranges=replay_summary["offset_ranges"],
        timeout=timeout_seconds,
        env_file=env_file,
    )


def build_spark_args(
    config: MarketReplayConfig,
    replay_summary: Mapping[str, Any],
    *,
    env_file: Path = Path(".env"),
) -> argparse.Namespace:
    env_values = _read_env_file(env_file)
    return argparse.Namespace(
        trace_id=str(replay_summary["trace_id"]),
        topic=str(replay_summary["topic"]),
        symbols=[config.ticker],
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or env_values.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        database_url=os.environ.get("DATABASE_URL")
        or env_values.get(
            "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
        ),
        offset_ranges=replay_summary["offset_ranges"],
    )


def verify_spark_result(summary: Mapping[str, Any]) -> None:
    """Fail a DAG run when records diverge between Kafka, Spark, and storage."""
    published = int(summary["published_trades"])
    received = int(summary["consumer_received"])
    spark_input = int(summary["spark_input_trades"])
    invalid = int(summary["spark_invalid_trades"])
    unique = int(summary["spark_valid_unique_trades"])
    output_bars = int(summary["spark_output_bars"])
    stored_bars = int(summary["postgres_upserted_bars"])
    if not (
        published == received == spark_input == unique
        and invalid == 0
        and output_bars == stored_bars
    ):
        raise RuntimeError("pipeline integrity counts did not match")


def verify_stored_result(
    config: MarketReplayConfig,
    spark_summary: Mapping[str, Any],
    *,
    database_url: str,
    connector=None,
) -> dict[str, int | str]:
    """Verify the Spark output exists in PostgreSQL without exposing its DSN."""
    if connector is None:
        import psycopg

        connector = psycopg.connect

    query = """
        SELECT MIN(symbol), COUNT(*)
        FROM market_bars
        WHERE symbol = %s
          AND bar_start >= %s
          AND bar_start < %s
          AND timeframe = '1m'
          AND source = 'alpaca_replay'
          AND feed = %s
    """
    with connector(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (config.ticker, config.start, config.end, config.feed),
            )
            row = cursor.fetchone()

    stored_symbol = row[0] if row else None
    stored_count = int(row[1]) if row else 0
    expected_count = int(spark_summary["spark_output_bars"])
    if stored_symbol != config.ticker:
        raise RuntimeError(
            f"PostgreSQL symbol mismatch: expected {config.ticker}, got {stored_symbol}"
        )
    if stored_count != expected_count:
        raise RuntimeError(
            f"PostgreSQL stored bar count mismatch: expected {expected_count}, got {stored_count}"
        )
    return {
        "step": "postgres_verification",
        "symbol": config.ticker,
        "start": config.start,
        "end": config.end,
        "feed": config.feed,
        "source": "alpaca_replay",
        "expected_bars": expected_count,
        "stored_bars": stored_count,
    }
