"""Pure event-to-session planning over verified exchange-calendar records."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .platform_contracts import MarketStatus


class ReleasePhase(StrEnum):
    MARKET_CLOSED = "MARKET_CLOSED"
    PRE_MARKET = "PRE_MARKET"
    REGULAR_SESSION = "REGULAR_SESSION"
    POST_MARKET = "POST_MARKET"


class MarkerKind(StrEnum):
    RELEASE = "RELEASE"
    STATEMENT = "STATEMENT"
    PRESS_CONFERENCE = "PRESS_CONFERENCE"


def _require_aware_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


@dataclass(frozen=True)
class TradingSession:
    exchange: str
    session_date: date
    opens_at: datetime
    closes_at: datetime
    is_early_close: bool
    calendar_source: str
    calendar_version: str
    exchange_timezone: str = "America/New_York"

    def __post_init__(self) -> None:
        _require_aware_utc(self.opens_at, "opens_at")
        _require_aware_utc(self.closes_at, "closes_at")
        if self.opens_at >= self.closes_at:
            raise ValueError("session open must be before close")
        if (
            not self.exchange
            or not self.calendar_source
            or not self.calendar_version
            or not self.exchange_timezone
        ):
            raise ValueError("session requires exchange and calendar lineage")
        try:
            ZoneInfo(self.exchange_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("session requires a valid exchange timezone") from exc


@dataclass(frozen=True)
class EventMarker:
    kind: MarkerKind
    at: datetime

    def __post_init__(self) -> None:
        _require_aware_utc(self.at, "marker timestamp")


@dataclass(frozen=True)
class EventSessionPlan:
    released_at: datetime
    market_status: MarketStatus
    release_phase: ReleasePhase
    s_minus_1: TradingSession
    s0: TradingSession
    s_plus_1: TradingSession
    markers: tuple[EventMarker, ...]


def _validated_sessions(sessions: Iterable[TradingSession]) -> tuple[TradingSession, ...]:
    ordered = tuple(sessions)
    if len(ordered) < 3:
        raise ValueError("at least three verified sessions are required")
    if tuple(sorted(ordered, key=lambda item: item.session_date)) != ordered:
        raise ValueError("sessions must be ordered by session_date")
    if len({item.session_date for item in ordered}) != len(ordered):
        raise ValueError("sessions must have unique session dates")
    if len({item.exchange for item in ordered}) != 1:
        raise ValueError("sessions must belong to one exchange")
    if len({item.exchange_timezone for item in ordered}) != 1:
        raise ValueError("sessions must use one exchange timezone")
    for previous, current in zip(ordered, ordered[1:]):
        if previous.closes_at >= current.opens_at:
            raise ValueError("verified sessions must not overlap")
    return ordered


def plan_event_session(
    *,
    released_at: datetime,
    sessions: Iterable[TradingSession],
    markers: Iterable[EventMarker] = (),
) -> EventSessionPlan:
    """Map a release to adjacent verified sessions without guessing holidays."""

    _require_aware_utc(released_at, "released_at")
    ordered = _validated_sessions(sessions)
    release_session_date = released_at.astimezone(
        ZoneInfo(ordered[0].exchange_timezone)
    ).date()

    release_day_index = next(
        (
            index
            for index, session in enumerate(ordered)
            if session.session_date == release_session_date
        ),
        None,
    )
    if release_day_index is None:
        s0_index = next(
            (
                index
                for index, session in enumerate(ordered)
                if session.session_date > release_session_date
            ),
            None,
        )
        phase = ReleasePhase.MARKET_CLOSED
        market_status = MarketStatus.CLOSED
    else:
        s0_index = release_day_index
        s0_candidate = ordered[s0_index]
        market_status = (
            MarketStatus.EARLY_CLOSE
            if s0_candidate.is_early_close
            else MarketStatus.OPEN
        )
        if released_at < s0_candidate.opens_at:
            phase = ReleasePhase.PRE_MARKET
        elif released_at < s0_candidate.closes_at:
            phase = ReleasePhase.REGULAR_SESSION
        else:
            phase = ReleasePhase.POST_MARKET

    if s0_index is None or s0_index == 0 or s0_index + 1 >= len(ordered):
        raise ValueError("verified sessions do not cover S-1, S0 and S+1")

    all_markers = (EventMarker(MarkerKind.RELEASE, released_at), *tuple(markers))
    ordered_markers = tuple(sorted(all_markers, key=lambda item: item.at))
    return EventSessionPlan(
        released_at=released_at,
        market_status=market_status,
        release_phase=phase,
        s_minus_1=ordered[s0_index - 1],
        s0=ordered[s0_index],
        s_plus_1=ordered[s0_index + 1],
        markers=ordered_markers,
    )
