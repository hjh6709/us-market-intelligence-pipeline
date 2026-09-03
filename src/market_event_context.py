"""Plan and validate provider-bar context around official economic releases."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.economic_event_schedule import EconomicRelease
from src.historical_bars import HistoricalBar


@dataclass(frozen=True)
class EventContextRequest:
    event_id: str
    event_type: str
    release_date: str
    symbols: tuple[str, ...]
    layer: str
    timeframe: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class DailyContextSelection:
    bars: tuple[HistoricalBar, ...]
    sessions_before: int
    event_session: int
    sessions_after: int

    @property
    def complete(self) -> bool:
        return (
            self.sessions_before == 7
            and self.event_session == 1
            and self.sessions_after == 7
        )


def build_context_requests(
    releases: Sequence[EconomicRelease],
    symbols: Sequence[str],
    *,
    session_minutes_before: int = 60,
    session_minutes_after: int = 120,
    daily_calendar_buffer: int = 14,
) -> list[EventContextRequest]:
    """Create one 1-minute and one daily-bar API request per release."""
    if not releases:
        raise ValueError("at least one economic release is required")
    normalized_symbols = tuple(symbols)
    if not normalized_symbols or len(normalized_symbols) != len(set(normalized_symbols)):
        raise ValueError("symbols must be a non-empty unique list")
    if min(session_minutes_before, session_minutes_after, daily_calendar_buffer) < 0:
        raise ValueError("context window values must not be negative")

    requests: list[EventContextRequest] = []
    for release in releases:
        requests.append(
            EventContextRequest(
                event_id=release.event_id,
                event_type=release.event_type,
                release_date=release.release_date.isoformat(),
                symbols=normalized_symbols,
                layer="SESSION_1MIN",
                timeframe="1Min",
                start=release.released_at - timedelta(minutes=session_minutes_before),
                end=release.released_at + timedelta(minutes=session_minutes_after + 1),
            )
        )

        market_timezone = ZoneInfo(release.timezone)
        local_midnight = datetime.combine(
            release.release_date,
            time.min,
            tzinfo=market_timezone,
        )
        requests.append(
            EventContextRequest(
                event_id=release.event_id,
                event_type=release.event_type,
                release_date=release.release_date.isoformat(),
                symbols=normalized_symbols,
                layer="DAILY_15_SESSIONS",
                timeframe="1Day",
                start=local_midnight - timedelta(days=daily_calendar_buffer),
                end=local_midnight + timedelta(days=daily_calendar_buffer + 1),
            )
        )
    return requests


def select_daily_context(
    bars: Sequence[HistoricalBar],
    release: EconomicRelease,
    symbol: str,
    *,
    trading_days_each_side: int = 7,
) -> DailyContextSelection:
    """Keep seven observed trading sessions before/after the release session."""
    if trading_days_each_side != 7:
        raise ValueError("the current daily context contract requires seven sessions")

    market_timezone = ZoneInfo(release.timezone)
    selected_symbol = sorted(
        (bar for bar in bars if bar.symbol == symbol),
        key=lambda bar: bar.bar_start,
    )
    bars_by_date: dict = {}
    for bar in selected_symbol:
        local_date = bar.bar_start.astimezone(market_timezone).date()
        if local_date in bars_by_date:
            raise ValueError(f"multiple daily bars for {symbol} on {local_date}")
        bars_by_date[local_date] = bar

    before = [bar for day, bar in bars_by_date.items() if day < release.release_date]
    event = [bar for day, bar in bars_by_date.items() if day == release.release_date]
    after = [bar for day, bar in bars_by_date.items() if day > release.release_date]
    chosen = before[-trading_days_each_side:] + event + after[:trading_days_each_side]
    return DailyContextSelection(
        bars=tuple(chosen),
        sessions_before=min(len(before), trading_days_each_side),
        event_session=len(event),
        sessions_after=min(len(after), trading_days_each_side),
    )


def select_session_context(
    bars: Sequence[HistoricalBar],
    request: EventContextRequest,
) -> list[HistoricalBar]:
    """Enforce the planned half-open interval even if the provider end is inclusive."""
    if request.layer != "SESSION_1MIN" or request.timeframe != "1Min":
        raise ValueError("session selection requires a SESSION_1MIN request")
    return [bar for bar in bars if request.start <= bar.bar_start < request.end]


def available_request_end(
    request: EventContextRequest,
    provider_available_until: datetime,
) -> datetime:
    """Prevent historical requests from extending beyond provider availability."""
    if provider_available_until.tzinfo is None:
        raise ValueError("provider availability timestamp must include a timezone")
    return min(request.end, provider_available_until)
