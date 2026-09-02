"""Build deterministic Alpaca archive partitions for events and symbols."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

from src.economic_event_schedule import EconomicRelease
from src.market_trade_archive import ArchivePartition


def _rfc3339(value) -> str:
    return value.isoformat().replace("+00:00", "Z")


def build_archive_plan(
    releases: Sequence[EconomicRelease],
    symbols: Sequence[str],
    *,
    minutes_before: int = 60,
    minutes_after: int = 60,
    feed: str = "sip",
) -> list[ArchivePartition]:
    """Create one [T-before, T+after+1min) partition per event and symbol."""
    if not releases:
        raise ValueError("at least one economic release is required")
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("symbols must be a non-empty unique list")
    if minutes_before < 0 or minutes_after < 0:
        raise ValueError("event-window minutes must not be negative")

    return [
        ArchivePartition(
            event_type=release.event_type,
            release_date=release.release_date.isoformat(),
            symbol=symbol,
            start=_rfc3339(release.released_at - timedelta(minutes=minutes_before)),
            end=_rfc3339(release.released_at + timedelta(minutes=minutes_after + 1)),
            feed=feed,
        )
        for release in releases
        for symbol in symbols
    ]
