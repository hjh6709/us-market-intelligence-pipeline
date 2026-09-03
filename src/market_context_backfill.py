"""Reusable one-economic-event/one-symbol market-context backfill."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.derived_bars import aggregate_derived_bars, upsert_derived_bars
from src.economic_event_schedule import EconomicRelease, load_event_catalog
from src.historical_bars import (
    HistoricalBar,
    fetch_all_bars,
    upsert_historical_bars,
)
from src.market_event_context import (
    available_request_end,
    build_context_requests,
    select_daily_context,
    select_session_context,
)
from src.market_universe import load_market_universe


@dataclass(frozen=True)
class MarketContextWorkItem:
    event_id: str
    event_type: str
    release_date: date
    released_at: datetime
    symbol: str
    feed: str
    timezone: str
    reference_period: str
    source: str
    source_url: str


@dataclass(frozen=True)
class MarketContextResult:
    event_id: str
    symbol: str
    session_1m_rows: int
    derived_3m_rows: int
    derived_5m_rows: int
    derived_3m_partial_rows: int
    derived_5m_partial_rows: int
    daily_rows: int
    daily_before: int
    daily_event: int
    daily_after: int
    coverage_status: str
    pages: int
    fallback_used: bool


@dataclass(frozen=True)
class MarketContextBatchResult:
    event_id: str
    results: tuple[MarketContextResult, ...]
    pages: int


BarFetcher = Callable[..., tuple[list[HistoricalBar], int]]
HistoricalWriter = Callable[..., int]
DerivedWriter = Callable[..., int]


def build_yearly_backfill_configs(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Split a multi-year request into bounded child-DAG configurations."""
    release_from = date.fromisoformat(str(config["release_from"]))
    release_to = date.fromisoformat(str(config["release_to"]))
    if release_to < release_from:
        raise ValueError("release_to must not be before release_from")

    child_configs = []
    for year in range(release_from.year, release_to.year + 1):
        child_from = max(release_from, date(year, 1, 1))
        child_to = min(release_to, date(year, 12, 31))
        child = dict(config)
        child["release_from"] = child_from.isoformat()
        child["release_to"] = child_to.isoformat()
        child_configs.append(child)
    return child_configs


def select_market_context_work(
    config: Mapping[str, Any],
) -> list[MarketContextWorkItem]:
    """Expand a run configuration into deterministic event-symbol work items."""

    catalog_path = Path(config.get("catalog", "config/market_event_catalog.json"))
    universe_path = Path(config.get("universe", "config/market_universe.json"))
    requested_types = {
        str(value).strip().upper() for value in config.get("event_types", [])
    }
    if not requested_types:
        raise ValueError("event_types must be a non-empty list")
    release_from = date.fromisoformat(str(config["release_from"]))
    release_to = date.fromisoformat(str(config["release_to"]))
    if release_to < release_from:
        raise ValueError("release_to must not be before release_from")
    feed = str(config.get("feed", "sip")).lower()
    if feed not in {"sip", "iex"}:
        raise ValueError("feed must be sip or iex")

    catalog = load_event_catalog(catalog_path)
    available_types = {release.event_type for release in catalog}
    unknown_types = requested_types - available_types
    if unknown_types:
        raise ValueError(f"event_types are not in the catalog: {sorted(unknown_types)}")
    releases = [
        release
        for release in catalog
        if release.event_type in requested_types
        and release_from <= release.release_date <= release_to
    ]
    if not releases:
        raise ValueError("no confirmed release falls inside the requested selection")

    configured_symbols = config.get("symbols")
    if configured_symbols:
        symbols = [str(value).strip().upper() for value in configured_symbols]
    else:
        symbols = [item.symbol for item in load_market_universe(universe_path)]
    if not symbols or len(symbols) != len(set(symbols)) or any(not value for value in symbols):
        raise ValueError("symbols must be a non-empty unique list")

    return [
        MarketContextWorkItem(
            event_id=release.event_id,
            event_type=release.event_type,
            release_date=release.release_date,
            released_at=release.released_at,
            symbol=symbol,
            feed=feed,
            timezone=release.timezone,
            reference_period=release.reference_period,
            source=release.source,
            source_url=release.source_url,
        )
        for release in releases
        for symbol in symbols
    ]


def collect_market_context_work_item(
    item: MarketContextWorkItem,
    *,
    client: object,
    database_url: str,
    provider_available_until: datetime,
    fetcher: BarFetcher = fetch_all_bars,
    historical_writer: HistoricalWriter = upsert_historical_bars,
    derived_writer: DerivedWriter = upsert_derived_bars,
) -> MarketContextResult:
    """Collect, derive and store one event-symbol unit without large XCom payloads."""

    release = EconomicRelease(
        event_type=item.event_type,
        reference_period=item.reference_period,
        release_date=item.release_date,
        released_at=item.released_at,
        timezone=item.timezone,
        source=item.source,
        source_url=item.source_url,
    )
    session_request, daily_request = build_context_requests([release], [item.symbol])

    session_bars, session_pages = _fetch_request(
        session_request,
        client=client,
        feed=item.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    session_rows = select_session_context(session_bars, session_request)
    historical_writer(
        session_rows,
        database_url=database_url,
        feed=item.feed,
        timeframe="1Min",
    )

    derived_counts: dict[int, int] = {}
    derived_partial_counts: dict[int, int] = {}
    for minutes in (3, 5):
        derived = aggregate_derived_bars(session_rows, minutes)
        derived_writer(
            derived,
            database_url=database_url,
            feed=item.feed,
        )
        derived_counts[minutes] = len(derived)
        derived_partial_counts[minutes] = sum(
            bar.coverage_status == "PARTIAL" for bar in derived
        )

    daily_bars, daily_pages = _fetch_request(
        daily_request,
        client=client,
        feed=item.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    daily = select_daily_context(daily_bars, release, item.symbol)
    historical_writer(
        daily.bars,
        database_url=database_url,
        feed=item.feed,
        timeframe="1Day",
    )

    coverage_status = _coverage_status(
        item,
        daily_complete=daily.complete,
        daily_event=daily.event_session,
        daily_rows=len(daily.bars),
        session_rows=len(session_rows),
        provider_available_until=provider_available_until,
    )

    return MarketContextResult(
        event_id=item.event_id,
        symbol=item.symbol,
        session_1m_rows=len(session_rows),
        derived_3m_rows=derived_counts[3],
        derived_5m_rows=derived_counts[5],
        derived_3m_partial_rows=derived_partial_counts[3],
        derived_5m_partial_rows=derived_partial_counts[5],
        daily_rows=len(daily.bars),
        daily_before=daily.sessions_before,
        daily_event=daily.event_session,
        daily_after=daily.sessions_after,
        coverage_status=coverage_status,
        pages=session_pages + daily_pages,
        fallback_used=False,
    )


def collect_market_context_event(
    items: Sequence[MarketContextWorkItem],
    *,
    client: object,
    database_url: str,
    provider_available_until: datetime,
    fetcher: BarFetcher = fetch_all_bars,
    historical_writer: HistoricalWriter = upsert_historical_bars,
    derived_writer: DerivedWriter = upsert_derived_bars,
) -> MarketContextBatchResult:
    """Fetch every symbol for one release in two provider requests."""
    if not items:
        raise ValueError("event batch must contain at least one symbol")
    event_ids = {item.event_id for item in items}
    feeds = {item.feed for item in items}
    symbols = [item.symbol for item in items]
    if len(event_ids) != 1 or len(feeds) != 1:
        raise ValueError("event batch must share one event and feed")
    if len(symbols) != len(set(symbols)):
        raise ValueError("event batch symbols must be unique")

    first = items[0]
    release = EconomicRelease(
        event_type=first.event_type,
        reference_period=first.reference_period,
        release_date=first.release_date,
        released_at=first.released_at,
        timezone=first.timezone,
        source=first.source,
        source_url=first.source_url,
    )
    session_request, daily_request = build_context_requests([release], symbols)
    session_bars, session_pages = _fetch_request(
        session_request,
        client=client,
        feed=first.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    session_rows = select_session_context(session_bars, session_request)
    historical_writer(
        session_rows,
        database_url=database_url,
        feed=first.feed,
        timeframe="1Min",
    )

    derived_by_minutes = {
        minutes: aggregate_derived_bars(session_rows, minutes)
        for minutes in (3, 5)
    }
    for derived in derived_by_minutes.values():
        derived_writer(derived, database_url=database_url, feed=first.feed)

    daily_bars, daily_pages = _fetch_request(
        daily_request,
        client=client,
        feed=first.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    daily_by_symbol = {
        item.symbol: select_daily_context(daily_bars, release, item.symbol)
        for item in items
    }
    selected_daily = [
        bar
        for item in items
        for bar in daily_by_symbol[item.symbol].bars
    ]
    historical_writer(
        selected_daily,
        database_url=database_url,
        feed=first.feed,
        timeframe="1Day",
    )

    results = []
    for item in items:
        symbol_session = [bar for bar in session_rows if bar.symbol == item.symbol]
        daily = daily_by_symbol[item.symbol]
        symbol_derived = {
            minutes: [
                bar
                for bar in derived_by_minutes[minutes]
                if bar.symbol == item.symbol
            ]
            for minutes in (3, 5)
        }
        results.append(
            MarketContextResult(
                event_id=item.event_id,
                symbol=item.symbol,
                session_1m_rows=len(symbol_session),
                derived_3m_rows=len(symbol_derived[3]),
                derived_5m_rows=len(symbol_derived[5]),
                derived_3m_partial_rows=sum(
                    bar.coverage_status == "PARTIAL" for bar in symbol_derived[3]
                ),
                derived_5m_partial_rows=sum(
                    bar.coverage_status == "PARTIAL" for bar in symbol_derived[5]
                ),
                daily_rows=len(daily.bars),
                daily_before=daily.sessions_before,
                daily_event=daily.event_session,
                daily_after=daily.sessions_after,
                coverage_status=_coverage_status(
                    item,
                    daily_complete=daily.complete,
                    daily_event=daily.event_session,
                    daily_rows=len(daily.bars),
                    session_rows=len(symbol_session),
                    provider_available_until=provider_available_until,
                ),
                pages=0,
                fallback_used=False,
            )
        )
    return MarketContextBatchResult(
        event_id=first.event_id,
        results=tuple(results),
        pages=session_pages + daily_pages,
    )


def _coverage_status(
    item: MarketContextWorkItem,
    *,
    daily_complete: bool,
    daily_event: int,
    daily_rows: int,
    session_rows: int,
    provider_available_until: datetime,
) -> str:
    if daily_complete:
        return "COMPLETE"
    if item.release_date > provider_available_until.date() - timedelta(days=12):
        return "FUTURE_SESSION_UNAVAILABLE"
    if daily_event == 0 and not session_rows:
        return "MARKET_CLOSED"
    if not daily_rows and not session_rows:
        return "NO_MARKET_DATA"
    return "PARTIAL"


def _fetch_request(
    request: object,
    *,
    client: object,
    feed: str,
    provider_available_until: datetime,
    fetcher: BarFetcher,
) -> tuple[list[HistoricalBar], int]:
    request_end = available_request_end(request, provider_available_until)
    if request_end <= request.start:
        return [], 0
    return fetcher(
        client,
        symbols=request.symbols,
        start=request.start,
        end=request_end,
        feed=feed,
        timeframe=request.timeframe,
        max_pages=20,
    )
