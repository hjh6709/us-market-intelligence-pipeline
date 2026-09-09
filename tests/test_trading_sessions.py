import importlib
import importlib.util
import inspect
import unittest
from datetime import date, datetime, timezone


MODULE_NAME = "src.trading_sessions"


def load_subject():
    if importlib.util.find_spec(MODULE_NAME) is None:
        return None
    return importlib.import_module(MODULE_NAME)


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class TradingSessionPlannerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.subject = load_subject()
        self.assertIsNotNone(
            self.subject,
            "src.trading_sessions must implement the verified-session planner",
        )

    def session(
        self,
        session_date: date,
        opens: str,
        closes: str,
        *,
        early_close: bool = False,
    ):
        return self.subject.TradingSession(
            exchange="XNYS",
            session_date=session_date,
            opens_at=utc(opens),
            closes_at=utc(closes),
            is_early_close=early_close,
            calendar_source="fixture:NYSE",
            calendar_version="2026.1",
        )

    def test_pre_market_cpi_maps_release_day_to_s0(self) -> None:
        sessions = (
            self.session(date(2026, 8, 11), "2026-08-11T13:30:00Z", "2026-08-11T20:00:00Z"),
            self.session(date(2026, 8, 12), "2026-08-12T13:30:00Z", "2026-08-12T20:00:00Z"),
            self.session(date(2026, 8, 13), "2026-08-13T13:30:00Z", "2026-08-13T20:00:00Z"),
        )

        plan = self.subject.plan_event_session(
            released_at=utc("2026-08-12T12:30:00Z"), sessions=sessions
        )

        self.assertEqual(plan.release_phase.value, "PRE_MARKET")
        self.assertEqual(plan.market_status.value, "OPEN")
        self.assertEqual(plan.s_minus_1.session_date, date(2026, 8, 11))
        self.assertEqual(plan.s0.session_date, date(2026, 8, 12))
        self.assertEqual(plan.s_plus_1.session_date, date(2026, 8, 13))
        self.assertEqual(plan.markers[0].kind.value, "RELEASE")

    def test_good_friday_maps_to_next_verified_session(self) -> None:
        sessions = (
            self.session(date(2026, 4, 2), "2026-04-02T13:30:00Z", "2026-04-02T20:00:00Z"),
            self.session(date(2026, 4, 6), "2026-04-06T13:30:00Z", "2026-04-06T20:00:00Z"),
            self.session(date(2026, 4, 7), "2026-04-07T13:30:00Z", "2026-04-07T20:00:00Z"),
        )

        plan = self.subject.plan_event_session(
            released_at=utc("2026-04-03T12:30:00Z"), sessions=sessions
        )

        self.assertEqual(plan.release_phase.value, "MARKET_CLOSED")
        self.assertEqual(plan.market_status.value, "CLOSED")
        self.assertEqual(plan.s_minus_1.session_date, date(2026, 4, 2))
        self.assertEqual(plan.s0.session_date, date(2026, 4, 6))
        self.assertEqual(plan.s_plus_1.session_date, date(2026, 4, 7))

    def test_fomc_keeps_statement_and_press_conference_markers(self) -> None:
        sessions = (
            self.session(date(2026, 7, 28), "2026-07-28T13:30:00Z", "2026-07-28T20:00:00Z"),
            self.session(date(2026, 7, 29), "2026-07-29T13:30:00Z", "2026-07-29T20:00:00Z"),
            self.session(date(2026, 7, 30), "2026-07-30T13:30:00Z", "2026-07-30T20:00:00Z"),
        )
        markers = (
            self.subject.EventMarker(self.subject.MarkerKind.STATEMENT, utc("2026-07-29T18:00:00Z")),
            self.subject.EventMarker(self.subject.MarkerKind.PRESS_CONFERENCE, utc("2026-07-29T18:30:00Z")),
        )

        plan = self.subject.plan_event_session(
            released_at=utc("2026-07-29T18:00:00Z"),
            sessions=sessions,
            markers=markers,
        )

        self.assertEqual(plan.release_phase.value, "REGULAR_SESSION")
        self.assertEqual(
            [(item.kind.value, item.at) for item in plan.markers],
            [
                ("RELEASE", utc("2026-07-29T18:00:00Z")),
                ("STATEMENT", utc("2026-07-29T18:00:00Z")),
                ("PRESS_CONFERENCE", utc("2026-07-29T18:30:00Z")),
            ],
        )

    def test_early_close_is_preserved_without_assuming_six_and_half_hours(self) -> None:
        sessions = (
            self.session(date(2026, 11, 25), "2026-11-25T14:30:00Z", "2026-11-25T21:00:00Z"),
            self.session(
                date(2026, 11, 27),
                "2026-11-27T14:30:00Z",
                "2026-11-27T18:00:00Z",
                early_close=True,
            ),
            self.session(date(2026, 11, 30), "2026-11-30T14:30:00Z", "2026-11-30T21:00:00Z"),
        )

        plan = self.subject.plan_event_session(
            released_at=utc("2026-11-27T17:30:00Z"), sessions=sessions
        )

        self.assertEqual(plan.release_phase.value, "REGULAR_SESSION")
        self.assertEqual(plan.market_status.value, "EARLY_CLOSE")
        self.assertEqual(plan.s0.closes_at, utc("2026-11-27T18:00:00Z"))

    def test_rejects_naive_release_timestamp(self) -> None:
        sessions = (
            self.session(date(2026, 8, 11), "2026-08-11T13:30:00Z", "2026-08-11T20:00:00Z"),
            self.session(date(2026, 8, 12), "2026-08-12T13:30:00Z", "2026-08-12T20:00:00Z"),
        )

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            self.subject.plan_event_session(
                released_at=datetime(2026, 8, 12, 12, 30), sessions=sessions
            )

    def test_after_hours_release_uses_exchange_local_session_date(self) -> None:
        self.assertIn(
            "exchange_timezone",
            inspect.signature(self.subject.TradingSession).parameters,
        )
        sessions = (
            self.session(date(2026, 8, 11), "2026-08-11T13:30:00Z", "2026-08-11T20:00:00Z"),
            self.session(date(2026, 8, 12), "2026-08-12T13:30:00Z", "2026-08-12T20:00:00Z"),
            self.session(date(2026, 8, 13), "2026-08-13T13:30:00Z", "2026-08-13T20:00:00Z"),
        )

        plan = self.subject.plan_event_session(
            released_at=utc("2026-08-13T00:30:00Z"), sessions=sessions
        )

        self.assertEqual(plan.release_phase.value, "POST_MARKET")
        self.assertEqual(plan.s0.session_date, date(2026, 8, 12))


if __name__ == "__main__":
    unittest.main()
