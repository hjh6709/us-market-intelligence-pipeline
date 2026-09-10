import unittest
from datetime import date, datetime, timedelta, timezone

from src.platform_contracts import SessionDayType
import src.trading_sessions as subject


def aware(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class TradingSessionPlannerTest(unittest.TestCase):
    def calendar_snapshot(
        self,
        *,
        market_code: str = "US_EQUITIES",
        timezone_name: str = "America/New_York",
        calendar_source: str = "fixture:NYSE",
        calendar_snapshot_id: str = "calendar:v1",
    ) -> subject.CalendarSnapshot:
        return subject.CalendarSnapshot(
            calendar_snapshot_id,
            market_code,
            timezone_name,
            calendar_source,
            aware("2026-08-01T00:00:00Z"),
            "a" * 64,
        )

    def test_T49_session_offset_counts_verified_sessions_after_friday(self) -> None:
        snapshot = subject.CalendarSnapshot(
            "calendar:v1",
            "US_EQUITIES",
            "America/New_York",
            "XNYS_FIXTURE",
            aware("2026-08-01T00:00:00Z"),
            "a" * 64,
        )
        sessions = (
            subject.TradingSession("calendar:v1", date(2026, 8, 14), aware("2026-08-14T13:30:00Z"), aware("2026-08-14T20:00:00Z"), SessionDayType.REGULAR),
            subject.TradingSession("calendar:v1", date(2026, 8, 17), aware("2026-08-17T13:30:00Z"), aware("2026-08-17T20:00:00Z"), SessionDayType.REGULAR),
        )

        resolved = subject.resolve_session_offset(snapshot, sessions, date(2026, 8, 14), 1)

        self.assertEqual(resolved.session_date, date(2026, 8, 17))

    def test_T50_session_offset_seven_uses_verified_calendar_not_date_math(self) -> None:
        snapshot = subject.CalendarSnapshot(
            "calendar:v1",
            "US_EQUITIES",
            "America/New_York",
            "XNYS_FIXTURE",
            aware("2026-08-01T00:00:00Z"),
            "a" * 64,
        )
        dates = (
            date(2026, 8, 14), date(2026, 8, 17), date(2026, 8, 18),
            date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21),
            date(2026, 8, 24), date(2026, 8, 25),
        )
        sessions = tuple(
            subject.TradingSession(
                "calendar:v1",
                item,
                datetime(item.year, item.month, item.day, 13, 30, tzinfo=timezone.utc),
                datetime(item.year, item.month, item.day, 20, 0, tzinfo=timezone.utc),
                SessionDayType.REGULAR,
            )
            for item in dates
        )

        resolved = subject.resolve_session_offset(snapshot, sessions, dates[0], 7)

        self.assertEqual(resolved.session_date, date(2026, 8, 25))
        self.assertNotEqual(resolved.session_date, dates[0] + timedelta(days=7))

    def session(
        self,
        session_date: date,
        opens: str,
        closes: str,
        *,
        day_type: SessionDayType = SessionDayType.REGULAR,
        calendar_snapshot_id: str = "calendar:v1",
    ) -> subject.TradingSession:
        return subject.TradingSession(
            calendar_snapshot_id=calendar_snapshot_id,
            session_date=session_date,
            opens_at=aware(opens),
            closes_at=aware(closes),
            day_type=day_type,
        )

    def marker(
        self,
        marker_id: str,
        kind: subject.MarkerKind,
        role: subject.MarkerRole,
        at: str,
    ) -> subject.EventMarker:
        return subject.EventMarker(marker_id, kind, role, aware(at))

    def normal_sessions(self) -> tuple[subject.TradingSession, ...]:
        return (
            self.session(date(2026, 8, 11), "2026-08-11T13:30:00Z", "2026-08-11T20:00:00Z"),
            self.session(date(2026, 8, 12), "2026-08-12T13:30:00Z", "2026-08-12T20:00:00Z"),
            self.session(date(2026, 8, 13), "2026-08-13T13:30:00Z", "2026-08-13T20:00:00Z"),
            self.session(date(2026, 8, 14), "2026-08-14T13:30:00Z", "2026-08-14T20:00:00Z"),
        )

    def plan(self, at: str, *, sessions=None, snapshot=None) -> subject.EventSessionPlan:
        return subject.plan_event_session(
            calendar_snapshot=snapshot or self.calendar_snapshot(),
            markers=(
                self.marker("event:release", subject.MarkerKind.RELEASE, subject.MarkerRole.PRIMARY, at),
            ),
            sessions=sessions or self.normal_sessions(),
            planner_version="verified_session_planner_v2",
        )

    def test_premarket_maps_same_day_to_reaction_s0(self) -> None:
        plan = self.plan("2026-08-12T12:30:00Z")

        self.assertEqual(plan.release_phase, subject.ReleasePhase.PRE_MARKET)
        self.assertEqual(plan.s0.session_date, date(2026, 8, 12))
        self.assertEqual(plan.release_day_session.session_date, date(2026, 8, 12))

    def test_regular_session_maps_same_day_to_reaction_s0(self) -> None:
        plan = self.plan("2026-08-12T15:00:00Z")

        self.assertEqual(plan.release_phase, subject.ReleasePhase.REGULAR_SESSION)
        self.assertEqual(plan.s0.session_date, date(2026, 8, 12))

    def test_T22_post_market_maps_next_session_to_reaction_s0(self) -> None:
        plan = self.plan("2026-08-13T00:30:00Z")

        self.assertEqual(plan.release_phase, subject.ReleasePhase.POST_MARKET)
        self.assertEqual(plan.release_calendar_date, date(2026, 8, 12))
        self.assertEqual(plan.release_day_session.session_date, date(2026, 8, 12))
        self.assertEqual(plan.s0.session_date, date(2026, 8, 13))
        self.assertEqual(plan.s_plus_1.session_date, date(2026, 8, 14))

    def test_market_closed_maps_next_session_to_reaction_s0(self) -> None:
        sessions = (
            self.session(date(2026, 4, 2), "2026-04-02T13:30:00Z", "2026-04-02T20:00:00Z"),
            self.session(date(2026, 4, 6), "2026-04-06T13:30:00Z", "2026-04-06T20:00:00Z"),
            self.session(date(2026, 4, 7), "2026-04-07T13:30:00Z", "2026-04-07T20:00:00Z"),
        )

        plan = self.plan("2026-04-03T12:30:00Z", sessions=sessions)

        self.assertEqual(plan.release_phase, subject.ReleasePhase.MARKET_CLOSED)
        self.assertIsNone(plan.release_day_session)
        self.assertEqual(plan.s0.session_date, date(2026, 4, 6))

    def test_early_close_is_a_session_day_type(self) -> None:
        sessions = (
            self.session(date(2026, 11, 25), "2026-11-25T14:30:00Z", "2026-11-25T21:00:00Z"),
            self.session(date(2026, 11, 27), "2026-11-27T14:30:00Z", "2026-11-27T18:00:00Z", day_type=SessionDayType.EARLY_CLOSE),
            self.session(date(2026, 11, 30), "2026-11-30T14:30:00Z", "2026-11-30T21:00:00Z"),
        )

        plan = self.plan("2026-11-27T17:30:00Z", sessions=sessions)

        self.assertEqual(plan.release_phase, subject.ReleasePhase.REGULAR_SESSION)
        self.assertEqual(plan.release_day_type, SessionDayType.EARLY_CLOSE)

    def test_dst_boundary_uses_exchange_local_calendar_date(self) -> None:
        sessions = (
            self.session(date(2026, 3, 6), "2026-03-06T14:30:00Z", "2026-03-06T21:00:00Z"),
            self.session(date(2026, 3, 9), "2026-03-09T13:30:00Z", "2026-03-09T20:00:00Z"),
            self.session(date(2026, 3, 10), "2026-03-10T13:30:00Z", "2026-03-10T20:00:00Z"),
        )

        plan = self.plan("2026-03-09T12:30:00Z", sessions=sessions)

        self.assertEqual(plan.release_calendar_date, date(2026, 3, 9))
        self.assertEqual(plan.s0.session_date, date(2026, 3, 9))

    def test_any_aware_marker_is_normalized_to_utc(self) -> None:
        eastern = timezone(timedelta(hours=-4))
        marker = subject.EventMarker(
            "event:release",
            subject.MarkerKind.RELEASE,
            subject.MarkerRole.PRIMARY,
            datetime(2026, 8, 12, 8, 30, tzinfo=eastern),
        )

        plan = subject.plan_event_session(
            calendar_snapshot=self.calendar_snapshot(),
            markers=(marker,),
            sessions=self.normal_sessions(),
            planner_version="verified_session_planner_v2",
        )

        self.assertEqual(plan.canonical_marker.at, aware("2026-08-12T12:30:00Z"))

    def test_T19_rejects_unsupported_market_code(self) -> None:
        with self.assertRaisesRegex(ValueError, "supported market code"):
            self.calendar_snapshot(
                market_code="XNYS",
            )

    def test_T20_rejects_wrong_market_timezone(self) -> None:
        with self.assertRaisesRegex(ValueError, "trusted timezone"):
            self.calendar_snapshot(
                timezone_name="UTC",
            )

    def test_calendar_metadata_is_owned_only_by_snapshot(self) -> None:
        session = self.normal_sessions()[0]

        self.assertFalse(hasattr(session, "market_code"))
        self.assertFalse(hasattr(session, "listing_venue"))
        self.assertFalse(hasattr(session, "calendar_source"))
        self.assertFalse(hasattr(session, "exchange_timezone"))

    def test_rejects_mixed_calendar_snapshot(self) -> None:
        sessions = list(self.normal_sessions())
        sessions[2] = self.session(
            date(2026, 8, 13),
            "2026-08-13T13:30:00Z",
            "2026-08-13T20:00:00Z",
            calendar_snapshot_id="sha256:different",
        )
        with self.assertRaisesRegex(ValueError, "calendar snapshot"):
            self.plan("2026-08-12T12:30:00Z", sessions=tuple(sessions))

    def test_rejects_local_open_close_date_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "local date"):
            self.plan(
                "2026-08-12T12:30:00Z",
                sessions=(
                    self.session(date(2026, 8, 11), "2026-08-11T13:30:00Z", "2026-08-11T20:00:00Z"),
                    self.session(date(2026, 8, 12), "2026-08-13T13:30:00Z", "2026-08-13T20:00:00Z"),
                    self.session(date(2026, 8, 13), "2026-08-13T13:30:00Z", "2026-08-13T20:00:00Z"),
                ),
            )

    def test_rejects_duplicate_marker_identity(self) -> None:
        marker = self.marker("event:release", subject.MarkerKind.RELEASE, subject.MarkerRole.PRIMARY, "2026-08-12T12:30:00Z")
        duplicate = self.marker("event:release", subject.MarkerKind.PRESS_CONFERENCE, subject.MarkerRole.SECONDARY, "2026-08-12T13:00:00Z")
        with self.assertRaisesRegex(ValueError, "marker identity"):
            subject.plan_event_session(
                calendar_snapshot=self.calendar_snapshot(),
                markers=(marker, duplicate),
                sessions=self.normal_sessions(),
                planner_version="verified_session_planner_v2",
            )

    def test_fomc_statement_is_primary_and_press_conference_secondary(self) -> None:
        markers = (
            self.marker("fomc:statement", subject.MarkerKind.STATEMENT, subject.MarkerRole.PRIMARY, "2026-08-12T18:00:00Z"),
            self.marker("fomc:press", subject.MarkerKind.PRESS_CONFERENCE, subject.MarkerRole.SECONDARY, "2026-08-12T18:30:00Z"),
        )

        plan = subject.plan_event_session(
            calendar_snapshot=self.calendar_snapshot(),
            markers=markers,
            sessions=self.normal_sessions(),
            planner_version="verified_session_planner_v2",
        )

        self.assertEqual(plan.canonical_marker.kind, subject.MarkerKind.STATEMENT)
        self.assertEqual(len([item for item in plan.markers if item.role is subject.MarkerRole.PRIMARY]), 1)
        self.assertNotIn(subject.MarkerKind.RELEASE, {item.kind for item in plan.markers})

    def test_planner_uses_latest_marker_revision_as_current_primary(self) -> None:
        old = subject.EventMarker(
            "event:release:v1",
            subject.MarkerKind.RELEASE,
            subject.MarkerRole.PRIMARY,
            aware("2026-08-12T20:30:00Z"),
            marker_revision=1,
        )
        corrected = subject.EventMarker(
            "event:release:v2",
            subject.MarkerKind.RELEASE,
            subject.MarkerRole.PRIMARY,
            aware("2026-08-12T12:30:00Z"),
            marker_revision=2,
        )

        plan = subject.plan_event_session(
            calendar_snapshot=self.calendar_snapshot(),
            markers=(old, corrected),
            sessions=self.normal_sessions(),
            planner_version="verified_session_planner_v2",
        )

        self.assertEqual(plan.canonical_marker.marker_id, "event:release:v2")
        self.assertEqual(plan.s0.session_date, date(2026, 8, 12))

    def test_rejects_two_primary_markers(self) -> None:
        markers = (
            self.marker("event:release", subject.MarkerKind.RELEASE, subject.MarkerRole.PRIMARY, "2026-08-12T12:30:00Z"),
            self.marker("event:statement", subject.MarkerKind.STATEMENT, subject.MarkerRole.PRIMARY, "2026-08-12T18:00:00Z"),
        )
        with self.assertRaisesRegex(ValueError, "exactly one primary"):
            subject.plan_event_session(
                calendar_snapshot=self.calendar_snapshot(),
                markers=markers,
                sessions=self.normal_sessions(),
                planner_version="verified_session_planner_v2",
            )


if __name__ == "__main__":
    unittest.main()
