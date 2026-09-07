"""Reusable one-economic-event/one-symbol market-context backfill."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.derived_bars import DerivedBar, aggregate_derived_bars, upsert_derived_bars
from src.economic_event_schedule import EconomicRelease, load_event_catalog
from src.historical_bars import (
    HistoricalBar,
    fetch_all_bars,
    upsert_historical_bars,
)
from src.market_event_context import (
    EventContextRequest,
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
    session_collection_status: str
    daily_collection_status: str
    overall_collection_status: str
    session_coverage_status: str
    daily_coverage_status: str
    derived_3m_coverage_status: str
    derived_5m_coverage_status: str
    overall_coverage_status: str
    coverage_status: str = field(init=False)
    pages: int
    fallback_used: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "coverage_status", self.overall_coverage_status)


@dataclass(frozen=True)
class MarketContextBatchResult:
    event_id: str
    results: tuple[MarketContextResult, ...]
    pages: int


@dataclass(frozen=True)
class RequestCollectionResult:
    bars: tuple[HistoricalBar, ...]
    pages: int
    status: str


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

    session_collection = _fetch_request(
        session_request,
        client=client,
        feed=item.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    session_rows = select_session_context(session_collection.bars, session_request)
    historical_writer(
        session_rows,
        database_url=database_url,
        feed=item.feed,
        timeframe="1Min",
    )

    derived_counts: dict[int, int] = {}
    derived_partial_counts: dict[int, int] = {}
    derived_by_minutes: dict[int, list[DerivedBar]] = {}
    for minutes in (3, 5):
        derived = aggregate_derived_bars(session_rows, minutes)
        derived_by_minutes[minutes] = derived
        derived_writer(
            derived,
            database_url=database_url,
            feed=item.feed,
        )
        derived_counts[minutes] = len(derived)
        derived_partial_counts[minutes] = sum(
            bar.coverage_status == "PARTIAL" for bar in derived
        )

    daily_collection = _fetch_request(
        daily_request,
        client=client,
        feed=item.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    daily = select_daily_context(daily_collection.bars, release, item.symbol)
    historical_writer(
        daily.bars,
        database_url=database_url,
        feed=item.feed,
        timeframe="1Day",
    )

    coverage = _coverage_statuses(
        item,
        session_request=session_request,
        session_rows=session_rows,
        derived_3m=derived_by_minutes[3],
        derived_5m=derived_by_minutes[5],
        daily_complete=daily.complete,
        daily_event=daily.event_session,
        daily_rows=len(daily.bars),
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
        session_collection_status=session_collection.status,
        daily_collection_status=daily_collection.status,
        overall_collection_status=_overall_collection_status(
            session_collection.status,
            daily_collection.status,
        ),
        session_coverage_status=coverage["session"],
        daily_coverage_status=coverage["daily"],
        derived_3m_coverage_status=coverage["derived_3m"],
        derived_5m_coverage_status=coverage["derived_5m"],
        overall_coverage_status=coverage["overall"],
        pages=session_collection.pages + daily_collection.pages,
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
    session_collection = _fetch_request(
        session_request,
        client=client,
        feed=first.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    session_rows = select_session_context(session_collection.bars, session_request)
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

    daily_collection = _fetch_request(
        daily_request,
        client=client,
        feed=first.feed,
        provider_available_until=provider_available_until,
        fetcher=fetcher,
    )
    daily_by_symbol = {
        item.symbol: select_daily_context(daily_collection.bars, release, item.symbol)
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
        coverage = _coverage_statuses(
            item,
            session_request=session_request,
            session_rows=symbol_session,
            derived_3m=symbol_derived[3],
            derived_5m=symbol_derived[5],
            daily_complete=daily.complete,
            daily_event=daily.event_session,
            daily_rows=len(daily.bars),
            provider_available_until=provider_available_until,
        )
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
                session_collection_status=session_collection.status,
                daily_collection_status=daily_collection.status,
                overall_collection_status=_overall_collection_status(
                    session_collection.status,
                    daily_collection.status,
                ),
                session_coverage_status=coverage["session"],
                daily_coverage_status=coverage["daily"],
                derived_3m_coverage_status=coverage["derived_3m"],
                derived_5m_coverage_status=coverage["derived_5m"],
                overall_coverage_status=coverage["overall"],
                pages=0,
                fallback_used=False,
            )
        )
    return MarketContextBatchResult(
        event_id=first.event_id,
        results=tuple(results),
        pages=session_collection.pages + daily_collection.pages,
    )


def _coverage_statuses(
    item: MarketContextWorkItem,
    *,
    session_request: EventContextRequest,
    session_rows: Sequence[HistoricalBar],
    derived_3m: Sequence[DerivedBar],
    derived_5m: Sequence[DerivedBar],
    daily_complete: bool,
    daily_event: int,
    daily_rows: int,
    provider_available_until: datetime,
) -> dict[str, str]:
    expected_minutes = _expected_session_minutes(session_request)
    observed_minutes = {bar.bar_start for bar in session_rows}
    session_status = (
        "NO_MARKET_DATA"
        if not session_rows
        else "COMPLETE"
        if observed_minutes == set(expected_minutes)
        else "PARTIAL"
    )
    daily_status = "COMPLETE" if daily_complete else "PARTIAL"
    derived_3m_status = _derived_coverage_status(
        expected_minutes,
        derived_3m,
        3,
    )
    derived_5m_status = _derived_coverage_status(
        expected_minutes,
        derived_5m,
        5,
    )

    if all(
        status == "COMPLETE"
        for status in (
            daily_status,
            session_status,
            derived_3m_status,
            derived_5m_status,
        )
    ):
        overall_status = "COMPLETE"
    elif item.release_date > provider_available_until.date() - timedelta(days=12):
        overall_status = "FUTURE_SESSION_UNAVAILABLE"
    elif daily_event == 0 and not session_rows:
        overall_status = "MARKET_CLOSED"
    elif not daily_rows and not session_rows:
        overall_status = "NO_MARKET_DATA"
    else:
        overall_status = "PARTIAL"

    return {
        "session": session_status,
        "daily": daily_status,
        "derived_3m": derived_3m_status,
        "derived_5m": derived_5m_status,
        "overall": overall_status,
    }


def _overall_collection_status(session_status: str, daily_status: str) -> str:
    statuses = {session_status, daily_status}
    if statuses == {"COMPLETE"}:
        return "COMPLETE"
    if "NOT_AVAILABLE" in statuses:
        return "NOT_AVAILABLE"
    return "PARTIAL"


def _expected_session_minutes(request: EventContextRequest) -> tuple[datetime, ...]:
    minutes = []
    current = request.start
    while current < request.end:
        minutes.append(current)
        current += timedelta(minutes=1)
    return tuple(minutes)


def _derived_coverage_status(
    expected_minutes: Sequence[datetime],
    bars: Sequence[DerivedBar],
    minutes: int,
) -> str:
    if not bars:
        return "NO_MARKET_DATA"

    expected_source_counts: dict[datetime, int] = {}
    for minute in expected_minutes:
        bucket_start = minute.replace(
            minute=(minute.minute // minutes) * minutes,
            second=0,
            microsecond=0,
        )
        expected_source_counts[bucket_start] = (
            expected_source_counts.get(bucket_start, 0) + 1
        )
    actual_source_counts = {
        bar.bar_start: bar.source_bar_count
        for bar in bars
    }
    return (
        "COMPLETE"
        if actual_source_counts == expected_source_counts
        else "PARTIAL"
    )


def _fetch_request(
    request: EventContextRequest,
    *,
    client: object,
    feed: str,
    provider_available_until: datetime,
    fetcher: BarFetcher,
) -> RequestCollectionResult:
    request_end = available_request_end(request, provider_available_until)
    if request_end <= request.start:
        return RequestCollectionResult((), 0, "NOT_AVAILABLE")
    bars, pages = fetcher(
        client,
        symbols=request.symbols,
        start=request.start,
        end=request_end,
        feed=feed,
        timeframe=request.timeframe,
        max_pages=20,
    )
    return RequestCollectionResult(
        tuple(bars),
        pages,
        "COMPLETE" if request_end == request.end else "PARTIAL",
    )
