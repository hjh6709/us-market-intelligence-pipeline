"""Canonical envelope construction for raw Alpaca market events."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


def build_market_envelope(
    payload: Mapping[str, Any],
    feed: str,
    ingested_at: datetime,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """Wrap an unchanged provider trade in the project's raw-event contract."""
    for name in ("T", "S", "i", "t"):
        if name not in payload:
            raise ValueError(f"Missing routing field: {name}")

    identity = [
        "alpaca",
        feed,
        "market.trade.raw",
        payload["S"],
        str(payload["i"]),
        payload["t"],
    ]
    digest = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()

    return {
        "event_id": f"sha256:{digest}",
        "event_type": "market.trade.raw",
        "schema_version": 1,
        "source": "alpaca",
        "feed": feed,
        "source_event_id": str(payload["i"]),
        "event_timestamp": payload["t"],
        "ingested_at": ingested_at.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "trace_id": trace_id,
        "payload": dict(payload),
    }
