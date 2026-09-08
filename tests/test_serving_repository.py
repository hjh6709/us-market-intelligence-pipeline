import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from src.serving_repository import PostgresServingRepository


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.connection.executions.append((normalized, params))
        self.rows = self.connection.results.pop(0)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConnection:
    def __init__(self, results):
        self.results = list(results)
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self, **_kwargs):
        return FakeCursor(self)


class ConnectFactory:
    def __init__(self, results):
        self.connection = FakeConnection(results)
        self.calls = []

    def __call__(self, database_url, **kwargs):
        self.calls.append((database_url, kwargs))
        return self.connection


class PostgresServingRepositoryTest(unittest.TestCase):
    def test_list_symbols_uses_canonical_analysis_version(self):
        connect = ConnectFactory([[["NVDA"], ["SPY"]]])
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        symbols = repo.list_symbols("event-1")

        sql, params = connect.connection.executions[-1]
        self.assertIn("analysis_version = %s", sql)
        self.assertEqual(params, ("event-1", "multi_event_sip_v1"))
        self.assertEqual(symbols, ["NVDA", "SPY"])

    def test_list_events_uses_bound_parameters_for_filters(self):
        released_at = datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc)
        connect = ConnectFactory(
            [[("event-1", "CPI", "2026-07", released_at, "BLS", "READY", None, None, None)]]
        )
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        events = repo.list_events(
            event_type="CPI",
            released_from=date(2026, 1, 1),
            released_to=date(2026, 8, 31),
        )

        sql, params = connect.connection.executions[-1]
        self.assertNotIn("2026-08-31", sql)
        self.assertNotIn("'CPI'", sql)
        self.assertNotIn("released_at::date", sql)
        self.assertIn("released_at >= %s", sql)
        self.assertIn("released_at < %s", sql)
        self.assertEqual(
            params,
            (
                "CPI",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 9, 1, tzinfo=timezone.utc),
            ),
        )
        self.assertEqual(events[0].event_id, "event-1")
        self.assertEqual(events[0].released_at, released_at)

    def test_get_bars_bounds_query_to_event_window_and_validates_timeframe(self):
        released_at = datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc)
        connect = ConnectFactory(
            [
                [(released_at,)],
                [
                    (
                        "NVDA",
                        "1m",
                        released_at,
                        Decimal("180"),
                        Decimal("181"),
                        Decimal("179"),
                        Decimal("180.5"),
                        1000,
                        None,
                        None,
                        None,
                    )
                ],
            ]
        )
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        bars = repo.get_bars("event-1", "NVDA", "1m")

        sql, params = connect.connection.executions[-1]
        self.assertIn("source = %s", sql)
        self.assertIn("feed = %s", sql)
        self.assertEqual(params[0:2], ("NVDA", "1m"))
        self.assertEqual(params[2:4], ("alpaca", "sip"))
        self.assertEqual(params[4], released_at.replace(hour=11, minute=30))
        self.assertEqual(params[5], released_at.replace(hour=14, minute=30))
        self.assertEqual(bars[0].close, Decimal("180.5"))

        with self.assertRaises(ValueError):
            repo.get_bars("event-1", "NVDA", "15m")

    def test_strategy_summary_uses_fixed_strategy_identity(self):
        connect = ConnectFactory([[(2020, 1988, Decimal("-0.1565"), 782)]])
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        summary = repo.get_strategy_summary()

        _sql, params = connect.connection.executions[-1]
        self.assertEqual(params, ("pre60_momentum_post60", "v1"))
        self.assertEqual(summary.total_count, 2020)
        self.assertEqual(summary.eligible_count, 1988)
        self.assertEqual(summary.positive_count, 782)

    def test_historical_and_cross_asset_queries_pin_analysis_identity(self):
        released_at = datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc)
        connect = ConnectFactory(
            [
                [("event-1", released_at, Decimal("0.5"), Decimal("0.2"), "COMPLETE")],
                [("NVDA", Decimal("0.5"), Decimal("0.2"), "COMPLETE")],
            ]
        )
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        historical = repo.get_historical_impacts("CPI", "NVDA", "POST_60M")
        cross_asset = repo.get_cross_asset_impacts("event-1", "POST_60M")

        first_sql, first_params = connect.connection.executions[0]
        second_sql, second_params = connect.connection.executions[1]
        self.assertIn("analysis_version = %s", first_sql)
        self.assertEqual(
            first_params,
            ("CPI", "NVDA", "POST_60M", "alpaca", "sip", "multi_event_sip_v1"),
        )
        self.assertIn("analysis_version = %s", second_sql)
        self.assertEqual(
            second_params,
            ("event-1", "POST_60M", "alpaca", "sip", "multi_event_sip_v1"),
        )
        self.assertEqual(historical[0].event_id, "event-1")
        self.assertEqual(cross_asset[0].symbol, "NVDA")

    def test_overview_metrics_use_explicit_units_and_pinned_versions(self):
        connect = ConnectFactory(
            [
                [(202, 10, 2020, 308512, 112593, 70090, 2020, 8080, 2020, 1988)],
                [("CPI", 55), ("EMPLOYMENT", 55), ("PCE", 55), ("FOMC", 37)],
                [("AAPL",), ("NVDA",), ("SPY",)],
            ]
        )
        repo = PostgresServingRepository("postgresql://unused", connect=connect)

        metrics = repo.get_overview_metrics()
        counts = repo.get_event_type_counts()
        symbols = repo.list_supported_symbols()

        sql, params = connect.connection.executions[0]
        self.assertIn("analysis_version=%s", sql)
        self.assertIn("multi_event_sip_v1", params)
        self.assertIn("pre60_momentum_post60", params)
        self.assertIn("v1", params)
        self.assertEqual(metrics.event_symbol_intervals, 2020)
        self.assertEqual(metrics.stored_1m_bars, 308512)
        self.assertEqual(counts["FOMC"], 37)
        self.assertEqual(symbols, ["AAPL", "NVDA", "SPY"])
        symbol_sql, symbol_params = connect.connection.executions[2]
        self.assertIn("analysis_version=%s", symbol_sql)
        self.assertEqual(
            symbol_params, ("alpaca", "sip", "multi_event_sip_v1")
        )


if __name__ == "__main__":
    unittest.main()
