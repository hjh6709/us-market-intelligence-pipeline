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


def build_archive_replay_key(
    manifest: ArchiveManifest,
    event_timestamp: str,
    *,
    segment_minutes: int = 15,
) -> str:
    """Build a deterministic key that preserves order inside one time segment."""
    if segment_minutes < 1:
        raise ValueError("segment_minutes must be positive")
    start = datetime.fromisoformat(manifest.partition.start.replace("Z", "+00:00"))
    event_at = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
    segment = int((event_at - start).total_seconds() // (segment_minutes * 60))
    if segment < 0:
        raise ValueError("event timestamp precedes archive partition start")
    return (
        f"{manifest.partition.event_type}|{manifest.partition.release_date}|"
        f"{manifest.partition.symbol}|segment-{segment:02d}"
    )


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
            envelope = build_market_envelope(
                payload, manifest.partition.feed, clock(), trace_id
            )
            envelope["economic_event"] = {
                "event_type": manifest.partition.event_type,
                "release_date": manifest.partition.release_date,
            }
            publisher.publish(
                envelope,
                key=build_archive_replay_key(manifest, str(payload["t"])),
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
