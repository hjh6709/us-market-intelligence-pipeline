"""Pure event-to-session planning over one verified calendar generation."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Iterable
from zoneinfo import ZoneInfo

from .platform_contracts import SessionDayType


TRUSTED_MARKET_TIMEZONES = {"US_EQUITIES": "America/New_York"}


class ReleasePhase(StrEnum):
    MARKET_CLOSED = "MARKET_CLOSED"
    PRE_MARKET = "PRE_MARKET"
    REGULAR_SESSION = "REGULAR_SESSION"
    POST_MARKET = "POST_MARKET"


class MarkerKind(StrEnum):
    RELEASE = "RELEASE"
    STATEMENT = "STATEMENT"
    PRESS_CONFERENCE = "PRESS_CONFERENCE"


class MarkerRole(StrEnum):
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"


def _normalize_aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class TradingSession:
    market_code: str
    listing_venue: str
    session_date: date
    opens_at: datetime
    closes_at: datetime
    day_type: SessionDayType
    calendar_source: str
    calendar_snapshot_id: str
    exchange_timezone: str = "America/New_York"

    def __post_init__(self) -> None:
        opens_at = _normalize_aware(self.opens_at, "opens_at")
        closes_at = _normalize_aware(self.closes_at, "closes_at")
        object.__setattr__(self, "opens_at", opens_at)
        object.__setattr__(self, "closes_at", closes_at)
        if opens_at >= closes_at:
            raise ValueError("session open must be before close")
        if self.market_code not in TRUSTED_MARKET_TIMEZONES:
            raise ValueError("session requires a supported market code")
        trusted_timezone = TRUSTED_MARKET_TIMEZONES[self.market_code]
        if self.exchange_timezone != trusted_timezone:
            raise ValueError(
                f"{self.market_code} trusted timezone is {trusted_timezone}"
            )
        if not self.listing_venue or not self.calendar_source or not self.calendar_snapshot_id:
            raise ValueError("session requires venue and calendar lineage")
        if self.day_type is SessionDayType.CLOSED:
            raise ValueError("closed dates are represented by absence of a session row")
        market_timezone = ZoneInfo(trusted_timezone)
        if opens_at.astimezone(market_timezone).date() != self.session_date:
            raise ValueError("session open local date must equal session_date")
        if closes_at.astimezone(market_timezone).date() != self.session_date:
            raise ValueError("session close local date must equal session_date")


@dataclass(frozen=True)
class EventMarker:
    marker_id: str
    kind: MarkerKind
    role: MarkerRole
    at: datetime
    marker_revision: int = 1

    def __post_init__(self) -> None:
        if not self.marker_id.strip():
            raise ValueError("marker identity must not be empty")
        if self.marker_revision < 1:
            raise ValueError("marker revision must be positive")
        object.__setattr__(self, "at", _normalize_aware(self.at, "marker timestamp"))


@dataclass(frozen=True)
class EventSessionPlan:
    canonical_marker: EventMarker
    release_calendar_date: date
    release_day_session: TradingSession | None
    release_day_type: SessionDayType
    release_phase: ReleasePhase
    s_minus_1: TradingSession
    s0: TradingSession
    s_plus_1: TradingSession
    markers: tuple[EventMarker, ...]
    planner_version: str
    market_code: str
    calendar_snapshot_id: str


def _validated_sessions(sessions: Iterable[TradingSession]) -> tuple[TradingSession, ...]:
    ordered = tuple(sessions)
    if len(ordered) < 3:
        raise ValueError("at least three verified sessions are required")
    if tuple(sorted(ordered, key=lambda item: item.session_date)) != ordered:
        raise ValueError("sessions must be ordered by session_date")
    if len({item.session_date for item in ordered}) != len(ordered):
        raise ValueError("sessions must have unique session dates")
    if len({item.market_code for item in ordered}) != 1:
        raise ValueError("sessions must belong to one market")
    if len({item.exchange_timezone for item in ordered}) != 1:
        raise ValueError("sessions must use one trusted timezone")
    if len({item.calendar_source for item in ordered}) != 1:
        raise ValueError("sessions must use one calendar source")
    if len({item.calendar_snapshot_id for item in ordered}) != 1:
        raise ValueError("sessions must use one calendar snapshot")
    for previous, current in zip(ordered, ordered[1:]):
        if previous.closes_at >= current.opens_at:
            raise ValueError("verified sessions must not overlap")
    return ordered


def _validated_markers(markers: Iterable[EventMarker]) -> tuple[EventMarker, ...]:
    ordered = tuple(
        sorted(markers, key=lambda item: (item.kind.value, item.marker_revision))
    )
    if not ordered:
        raise ValueError("at least one event marker is required")
    if len({item.marker_id for item in ordered}) != len(ordered):
        raise ValueError("marker identity must be unique")

    current: list[EventMarker] = []
    for kind in {item.kind for item in ordered}:
        revisions = [item for item in ordered if item.kind is kind]
        revision_numbers = [item.marker_revision for item in revisions]
        if len(set(revision_numbers)) != len(revision_numbers):
            raise ValueError("marker revision must be unique per event and kind")
        if revision_numbers != list(range(1, max(revision_numbers) + 1)):
            raise ValueError("marker revisions must form a consecutive chain")
        current.append(max(revisions, key=lambda item: item.marker_revision))

    current_markers = tuple(sorted(current, key=lambda item: (item.at, item.kind.value)))
    primary = [item for item in current_markers if item.role is MarkerRole.PRIMARY]
    if len(primary) != 1:
        raise ValueError("event requires exactly one primary marker")
    return current_markers


def plan_event_session(
    *,
    markers: Iterable[EventMarker],
    sessions: Iterable[TradingSession],
    planner_version: str,
) -> EventSessionPlan:
    """Map a primary event marker to verified S-1/S0/S+1 sessions.

    S0 is the first regular trading session able to absorb the information:
    the same session for premarket/regular releases, and the next session for
    post-market or market-closed releases.
    """

    if not planner_version.strip():
        raise ValueError("planner_version is required")
    ordered_sessions = _validated_sessions(sessions)
    ordered_markers = _validated_markers(markers)
    canonical_marker = next(
        item for item in ordered_markers if item.role is MarkerRole.PRIMARY
    )
    market_timezone = ZoneInfo(ordered_sessions[0].exchange_timezone)
    release_calendar_date = canonical_marker.at.astimezone(market_timezone).date()
    release_day_index = next(
        (
            index
            for index, session in enumerate(ordered_sessions)
            if session.session_date == release_calendar_date
        ),
        None,
    )

    if release_day_index is None:
        s0_index = next(
            (
                index
                for index, session in enumerate(ordered_sessions)
                if session.session_date > release_calendar_date
            ),
            None,
        )
        release_day_session = None
        release_day_type = SessionDayType.CLOSED
        phase = ReleasePhase.MARKET_CLOSED
    else:
        release_day_session = ordered_sessions[release_day_index]
        release_day_type = release_day_session.day_type
        if canonical_marker.at < release_day_session.opens_at:
            phase = ReleasePhase.PRE_MARKET
            s0_index = release_day_index
        elif canonical_marker.at < release_day_session.closes_at:
            phase = ReleasePhase.REGULAR_SESSION
            s0_index = release_day_index
        else:
            phase = ReleasePhase.POST_MARKET
            s0_index = release_day_index + 1

    if s0_index is None or s0_index == 0 or s0_index + 1 >= len(ordered_sessions):
        raise ValueError("verified sessions do not cover S-1, S0 and S+1")

    return EventSessionPlan(
        canonical_marker=canonical_marker,
        release_calendar_date=release_calendar_date,
        release_day_session=release_day_session,
        release_day_type=release_day_type,
        release_phase=phase,
        s_minus_1=ordered_sessions[s0_index - 1],
        s0=ordered_sessions[s0_index],
        s_plus_1=ordered_sessions[s0_index + 1],
        markers=ordered_markers,
        planner_version=planner_version,
        market_code=ordered_sessions[0].market_code,
        calendar_snapshot_id=ordered_sessions[0].calendar_snapshot_id,
    )
