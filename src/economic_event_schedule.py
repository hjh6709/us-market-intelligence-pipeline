"""Load and validate official economic-event release manifests."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo


EVENT_TYPE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]*")
DEFAULT_CATALOG_PATH = Path("config/market_event_catalog.json")


@dataclass(frozen=True)
class EconomicRelease:
    event_type: str
    reference_period: str
    release_date: date
    released_at: datetime
    timezone: str
    source: str
    source_url: str

    @property
    def event_id(self) -> str:
        released_at = self.released_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return f"{self.event_type}|{self.reference_period}|{released_at}"


def _load_manifest(
    path: Path,
    *,
    event_type: str,
    source: str,
) -> list[EconomicRelease]:
    if not EVENT_TYPE_PATTERN.fullmatch(event_type):
        raise ValueError(f"invalid event_type in catalog: {event_type}")
    raw_releases = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw_releases, list):
        raise ValueError(f"event manifest must be a list: {path}")

    releases: list[EconomicRelease] = []
    for raw in raw_releases:
        if not isinstance(raw, Mapping):
            raise ValueError(f"event entry must be an object: {path}")
        timezone_name = str(raw["timezone"])
        local_time = datetime.fromisoformat(
            f"{raw['release_date']}T{raw['release_time']}:00"
        ).replace(tzinfo=ZoneInfo(timezone_name))
        reference_period = str(raw["reference_period"]).strip()
        source_url = str(raw["source_url"]).strip()
        if not reference_period or not source_url.startswith("https://"):
            raise ValueError(f"event reference_period/source_url is invalid: {path}")
        releases.append(
            EconomicRelease(
                event_type=event_type,
                reference_period=reference_period,
                release_date=date.fromisoformat(str(raw["release_date"])),
                released_at=local_time.astimezone(UTC),
                timezone=timezone_name,
                source=source,
                source_url=source_url,
            )
        )
    return releases


def load_event_catalog(path: Path = DEFAULT_CATALOG_PATH) -> list[EconomicRelease]:
    """Load every manifest in one catalog and reject duplicate official events."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("manifests"), list):
        raise ValueError("event catalog must use schema_version 1 and contain manifests")

    releases: list[EconomicRelease] = []
    for spec in payload["manifests"]:
        if not isinstance(spec, Mapping):
            raise ValueError("event catalog manifest entry must be an object")
        manifest_path = path.parent / str(spec["path"])
        releases.extend(
            _load_manifest(
                manifest_path,
                event_type=str(spec["event_type"]),
                source=str(spec["source"]),
            )
        )

    if len({release.event_id for release in releases}) != len(releases):
        raise ValueError("event catalog contains duplicate events")
    return sorted(releases, key=lambda item: (item.released_at, item.event_type))


def event_counts(releases: Sequence[EconomicRelease]) -> dict[str, int]:
    return dict(sorted(Counter(item.event_type for item in releases).items()))
