"""Replay verified Parquet trade archives through the canonical Kafka contract."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Any

from src.market_event import build_market_envelope
from src.market_trade_archive import ArchiveManifest, read_archive_records


@dataclass(frozen=True)
class ArchiveReplayResult:
    trace_id: str
    partition_count: int
    expected_trades: int
    published_trades: int
    duration_seconds: float
    events_per_second: float


def replay_archive(
    manifests: Sequence[ArchiveManifest],
    *,
    publisher: Any,
    trace_id: str,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ArchiveReplayResult:
    if not manifests:
        raise ValueError("at least one verified archive manifest is required")
    expected = sum(item.row_count for item in manifests)
    started = monotonic()
    published = 0
    for manifest in manifests:
        for payload in read_archive_records(manifest):
            publisher.publish(
                build_market_envelope(payload, manifest.partition.feed, clock(), trace_id)
            )
            published += 1
    duration = monotonic() - started
    if published != expected:
        raise RuntimeError(
            f"archive replay count mismatch: expected {expected}, published {published}"
        )
    return ArchiveReplayResult(
        trace_id=trace_id,
        partition_count=len(manifests),
        expected_trades=expected,
        published_trades=published,
        duration_seconds=duration,
        events_per_second=published / duration if duration else 0.0,
    )
