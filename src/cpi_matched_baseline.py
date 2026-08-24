"""Collect and compare same-weekday, same-local-time CPI control windows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg

from src.cpi_ingestion import CpiRelease, DEFAULT_DATABASE_URL, load_cpi_releases
from src.historical_bars import (
    AlpacaHistoricalBarsClient,
    HistoricalBar,
    fetch_all_bars,
    upsert_historical_bars,
)
from src.live_market_smoke import _read_env_file, load_credentials
from src.macro_event_impact import (
    ANALYSIS_VERSION,
    SYMBOLS,
    BarPoint,
    ImpactMetric,
    calculate_event_impacts,
)


BASELINE_VERSION = "same_weekday_1_2_3w_v1"


@dataclass(frozen=True)
class BaselineImpact:
    control_offset_weeks: int
    matched_at: datetime
    metric: ImpactMetric

    @property
    def baseline_impact_id(self) -> str:
        key = "|".join(
            (
                self.metric.economic_event_id,
                str(self.control_offset_weeks),
                self.metric.symbol,
                self.metric.window_name,
                BASELINE_VERSION,
            )
        )
        return "sha256:" + hashlib.sha256(key.encode("utf-8")).hexdigest()


def matched_control_time(release: CpiRelease, offset_weeks: int) -> datetime:
    if offset_weeks not in (1, 2, 3):
        raise ValueError("offset_weeks must be 1, 2, or 3")
    local_release = release.released_at.astimezone(ZoneInfo(release.timezone))
    return (local_release - timedelta(weeks=offset_weeks)).astimezone(UTC)


def bars_as_points(
    bars: Sequence[HistoricalBar],
) -> dict[str, list[BarPoint]]:
    by_symbol = {symbol: [] for symbol in SYMBOLS}
    for bar in bars:
        if bar.symbol in by_symbol:
            by_symbol[bar.symbol].append(
                BarPoint(bar.bar_start, bar.open, bar.close, bar.volume)
            )
    return by_symbol


def collect_baselines(
    client: AlpacaHistoricalBarsClient,
    releases: Sequence[CpiRelease],
) -> tuple[list[HistoricalBar], list[BaselineImpact], int]:
    all_bars = []
    baseline_impacts = []
    pages = 0
    release_times = {release.released_at for release in releases}
    for release in releases:
        for offset_weeks in (1, 2, 3):
            matched_at = matched_control_time(release, offset_weeks)
            if matched_at in release_times:
                continue
            bars, page_count = fetch_all_bars(
                client,
                symbols=SYMBOLS,
                start=matched_at - timedelta(minutes=60),
                end=matched_at + timedelta(minutes=60),
                feed="sip",
            )
            all_bars.extend(bars)
            pages += page_count
            metrics = calculate_event_impacts(
                release.event_id,
                matched_at,
                bars_as_points(bars),
            )
            baseline_impacts.extend(
                BaselineImpact(offset_weeks, matched_at, metric)
                for metric in metrics
            )
    return all_bars, baseline_impacts, pages


def store_baseline_impacts(
    baseline_impacts: Sequence[BaselineImpact],
    *,
    database_url: str,
) -> int:
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO macro_event_baseline_impacts (
                    baseline_impact_id, economic_event_id,
                    control_offset_weeks, matched_at, symbol,
                    source, feed, session_scope, window_name,
                    window_start, window_end, open_price, close_price,
                    return_pct, volume, realized_volatility,
                    coverage_status, coverage_reason, baseline_version
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    'alpaca', 'sip', 'EXTENDED_HOURS', %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    economic_event_id, control_offset_weeks, symbol,
                    source, feed, window_name, baseline_version
                ) DO UPDATE SET
                    baseline_impact_id = EXCLUDED.baseline_impact_id,
                    matched_at = EXCLUDED.matched_at,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    return_pct = EXCLUDED.return_pct,
                    volume = EXCLUDED.volume,
                    realized_volatility = EXCLUDED.realized_volatility,
                    coverage_status = EXCLUDED.coverage_status,
                    coverage_reason = EXCLUDED.coverage_reason,
                    created_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        baseline.baseline_impact_id,
                        baseline.metric.economic_event_id,
                        baseline.control_offset_weeks,
                        baseline.matched_at,
                        baseline.metric.symbol,
                        baseline.metric.window_name,
                        baseline.metric.window_start,
                        baseline.metric.window_end,
                        baseline.metric.open_price,
                        baseline.metric.close_price,
                        baseline.metric.return_pct,
                        baseline.metric.volume,
                        baseline.metric.realized_volatility,
                        baseline.metric.coverage_status,
                        baseline.metric.coverage_reason,
                        BASELINE_VERSION,
                    )
                    for baseline in baseline_impacts
                ],
            )
            cursor.execute(
                """
                WITH baseline AS (
                    SELECT
                        economic_event_id,
                        symbol,
                        window_name,
                        AVG(return_pct) AS avg_return_pct,
                        AVG(volume) AS avg_volume,
                        AVG(realized_volatility) AS avg_volatility,
                        COUNT(return_pct) AS sample_size
                    FROM macro_event_baseline_impacts
                    WHERE baseline_version = %s
                      AND coverage_status = 'COMPLETE'
                    GROUP BY economic_event_id, symbol, window_name
                )
                UPDATE macro_event_impacts impact
                SET
                    matched_baseline_return_pct = baseline.avg_return_pct,
                    return_vs_matched_baseline_pct =
                        impact.return_pct - baseline.avg_return_pct,
                    matched_baseline_volume = baseline.avg_volume,
                    volume_ratio_vs_matched_baseline =
                        CASE WHEN baseline.avg_volume > 0
                             THEN impact.volume / baseline.avg_volume END,
                    matched_baseline_volatility = baseline.avg_volatility,
                    volatility_ratio_vs_matched_baseline =
                        CASE WHEN baseline.avg_volatility > 0
                             THEN impact.realized_volatility / baseline.avg_volatility END,
                    baseline_sample_size = baseline.sample_size,
                    baseline_version = %s
                FROM baseline
                WHERE impact.economic_event_id = baseline.economic_event_id
                  AND impact.symbol = baseline.symbol
                  AND impact.window_name = baseline.window_name
                  AND impact.analysis_version = %s
                """,
                (BASELINE_VERSION, BASELINE_VERSION, ANALYSIS_VERSION),
            )
    return len(baseline_impacts)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args(argv)
    file_values = _read_env_file(args.env_file)
    database_url = (
        os.environ.get("DATABASE_URL")
        or file_values.get("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    key_id, secret_key = load_credentials(env_path=args.env_file)
    bars, impacts, pages = collect_baselines(
        AlpacaHistoricalBarsClient(key_id, secret_key),
        load_cpi_releases(),
    )
    stored_bars = upsert_historical_bars(
        bars,
        database_url=database_url,
        feed="sip",
    )
    stored_impacts = store_baseline_impacts(impacts, database_url=database_url)
    print(
        json.dumps(
            {
                "control_windows": len(impacts) // (len(SYMBOLS) * 4),
                "pages": pages,
                "fetched_bars": len(bars),
                "stored_bars": stored_bars,
                "baseline_impacts_upserted": stored_impacts,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
