"""Load official CPI release timestamps and matching ALFRED vintages."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from src.fred_client import FredClient, MacroObservation
from src.live_market_smoke import _read_env_file


DEFAULT_RELEASES_PATH = Path("config/cpi_releases.json")
DEFAULT_DATABASE_URL = "postgresql://market:market@localhost:55432/market"
CPI_SERIES = {
    "CPIAUCSL": (
        "Consumer Price Index for All Urban Consumers: All Items",
        "Monthly",
        "Index 1982-1984=100",
        "Seasonally Adjusted",
    ),
    "CPILFESL": (
        "Consumer Price Index for All Urban Consumers: All Items Less Food and Energy",
        "Monthly",
        "Index 1982-1984=100",
        "Seasonally Adjusted",
    ),
}


@dataclass(frozen=True)
class CpiRelease:
    reference_period: str
    release_date: date
    released_at: datetime
    timezone: str
    source_url: str

    @property
    def observation_date(self) -> date:
        return date.fromisoformat(f"{self.reference_period}-01")

    @property
    def event_id(self) -> str:
        released_at = self.released_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return f"CPI|{self.reference_period}|{released_at}"


def load_cpi_releases(path: Path = DEFAULT_RELEASES_PATH) -> list[CpiRelease]:
    raw_releases = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw_releases, list):
        raise ValueError("CPI release manifest must be a list")

    releases = []
    for raw in raw_releases:
        if not isinstance(raw, Mapping):
            raise ValueError("CPI release entry must be an object")
        timezone_name = str(raw["timezone"])
        local_time = datetime.fromisoformat(
            f"{raw['release_date']}T{raw['release_time']}:00"
        ).replace(tzinfo=ZoneInfo(timezone_name))
        release = CpiRelease(
            reference_period=str(raw["reference_period"]),
            release_date=date.fromisoformat(str(raw["release_date"])),
            released_at=local_time.astimezone(UTC),
            timezone=timezone_name,
            source_url=str(raw["source_url"]),
        )
        if release.observation_date >= release.release_date:
            raise ValueError("CPI reference period must precede its release date")
        releases.append(release)

    if len({release.event_id for release in releases}) != len(releases):
        raise ValueError("CPI release manifest contains duplicate events")
    return releases


def fetch_release_observations(
    client: FredClient,
    releases: Sequence[CpiRelease],
) -> list[tuple[CpiRelease, MacroObservation]]:
    results = []
    for release in releases:
        for series_id in CPI_SERIES:
            observations = client.fetch_observations(
                series_id=series_id,
                observation_start=release.observation_date,
                observation_end=release.observation_date,
                vintage_dates=[release.release_date],
            )
            if len(observations) != 1:
                raise ValueError(
                    f"Expected one {series_id} observation for {release.reference_period}"
                )
            results.append((release, observations[0]))
    return results


def upsert_cpi_data(
    releases: Sequence[CpiRelease],
    observations: Sequence[tuple[CpiRelease, MacroObservation]],
    *,
    database_url: str,
) -> tuple[int, int]:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO macro_series (
                    series_id, title, frequency, units,
                    seasonal_adjustment, source_url
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    frequency = EXCLUDED.frequency,
                    units = EXCLUDED.units,
                    seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                    source_url = EXCLUDED.source_url,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        series_id,
                        *metadata,
                        f"https://fred.stlouisfed.org/series/{series_id}",
                    )
                    for series_id, metadata in CPI_SERIES.items()
                ],
            )
            cursor.executemany(
                """
                INSERT INTO macro_observations (
                    series_id, observation_date, realtime_start,
                    realtime_end, value, source
                ) VALUES (%s, %s, %s, %s, %s, 'fred')
                ON CONFLICT (series_id, observation_date, realtime_start)
                DO UPDATE SET
                    realtime_end = EXCLUDED.realtime_end,
                    value = EXCLUDED.value,
                    ingested_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        observation.series_id,
                        observation.observation_date,
                        observation.realtime_start,
                        observation.realtime_end,
                        observation.value,
                    )
                    for _, observation in observations
                ],
            )
            cursor.executemany(
                """
                INSERT INTO economic_events (
                    economic_event_id, event_type, reference_period,
                    scheduled_at, released_at, original_timezone,
                    release_source, release_source_url, value_source,
                    vintage_as_of, quality_status
                ) VALUES (%s, 'CPI', %s, %s, %s, %s, 'bls', %s, 'fred', %s, 'READY')
                ON CONFLICT (economic_event_id) DO UPDATE SET
                    scheduled_at = EXCLUDED.scheduled_at,
                    released_at = EXCLUDED.released_at,
                    release_source_url = EXCLUDED.release_source_url,
                    vintage_as_of = EXCLUDED.vintage_as_of,
                    quality_status = EXCLUDED.quality_status,
                    ingested_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        release.event_id,
                        release.reference_period,
                        release.released_at,
                        release.released_at,
                        release.timezone,
                        release.source_url,
                        release.release_date,
                    )
                    for release in releases
                ],
            )
    return len(releases), len(observations)


def _settings(environ: Mapping[str, str], env_path: Path) -> tuple[str, str]:
    file_values = _read_env_file(env_path)
    fred_api_key = environ.get("FRED_API_KEY") or file_values.get("FRED_API_KEY", "")
    database_url = (
        environ.get("DATABASE_URL")
        or file_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    if not fred_api_key.strip():
        raise ValueError("FRED_API_KEY is missing")
    return fred_api_key.strip(), database_url.strip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--releases", type=Path, default=DEFAULT_RELEASES_PATH)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args(argv)

    fred_api_key, database_url = _settings(os.environ, args.env_file)
    releases = load_cpi_releases(args.releases)
    observations = fetch_release_observations(FredClient(fred_api_key), releases)
    event_count, observation_count = upsert_cpi_data(
        releases,
        observations,
        database_url=database_url,
    )
    print(
        json.dumps(
            {
                "economic_events_upserted": event_count,
                "macro_observations_upserted": observation_count,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
