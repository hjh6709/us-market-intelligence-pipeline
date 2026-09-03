"""Store confirmed economic-event release metadata independently of observations."""

from __future__ import annotations

from collections.abc import Sequence

import psycopg

from src.economic_event_schedule import EconomicRelease


def upsert_economic_events(
    releases: Sequence[EconomicRelease],
    *,
    database_url: str,
) -> int:
    """Upsert official release timestamps for every supported event type."""
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO economic_events (
                    economic_event_id, event_type, reference_period,
                    scheduled_at, released_at, original_timezone,
                    release_source, release_source_url, value_source,
                    vintage_as_of, quality_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'fred', %s, 'READY')
                ON CONFLICT (economic_event_id) DO UPDATE SET
                    scheduled_at = EXCLUDED.scheduled_at,
                    released_at = EXCLUDED.released_at,
                    original_timezone = EXCLUDED.original_timezone,
                    release_source = EXCLUDED.release_source,
                    release_source_url = EXCLUDED.release_source_url,
                    vintage_as_of = EXCLUDED.vintage_as_of,
                    quality_status = EXCLUDED.quality_status,
                    ingested_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        release.event_id,
                        release.event_type,
                        release.reference_period,
                        release.released_at,
                        release.released_at,
                        release.timezone,
                        release.source.lower(),
                        release.source_url,
                        release.release_date,
                    )
                    for release in releases
                ],
            )
    return len(releases)
