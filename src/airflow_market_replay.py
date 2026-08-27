"""Shared contracts for the parameterized Airflow market replay DAG."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
ALLOWED_FEEDS = frozenset({"iex", "sip"})


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
    ticker = str(params.get("ticker", "")).strip().upper()
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
