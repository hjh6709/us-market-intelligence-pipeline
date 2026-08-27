import importlib
import re
import unittest
from pathlib import Path


def load_target():
    try:
        return importlib.import_module("src.airflow_market_replay")
    except ModuleNotFoundError as error:
        raise AssertionError("Airflow market replay contract is not implemented") from error


class AirflowMarketReplayConfigTest(unittest.TestCase):
    def test_builds_one_run_config_per_requested_ticker(self) -> None:
        target = load_target()

        configs = target.validate_run_configs(
            {
                "tickers": ["SPY", "QQQ", "SMH", "NVDA"],
                "start": "2026-08-12T12:25:00Z",
                "end": "2026-08-12T12:35:00Z",
                "feed": "sip",
            },
            run_id="manual__multi-symbol",
        )

        self.assertEqual(
            [config.ticker for config in configs],
            ["SPY", "QQQ", "SMH", "NVDA"],
        )
        self.assertEqual(len({config.trace_id for config in configs}), 4)

    def test_rejects_empty_or_duplicate_ticker_list(self) -> None:
        target = load_target()
        base = {
            "start": "2026-08-12T12:25:00Z",
            "end": "2026-08-12T12:35:00Z",
            "feed": "sip",
        }

        for tickers in ([], ["SPY", "SPY"]):
            with self.subTest(tickers=tickers):
                with self.assertRaisesRegex(ValueError, "tickers"):
                    target.validate_run_configs(
                        {**base, "tickers": tickers},
                        run_id="manual__invalid-list",
                    )

    def test_accepts_ticker_feed_and_timezone_aware_window(self) -> None:
        target = load_target()

        config = target.validate_run_config(
            {
                "ticker": "SPY",
                "start": "2026-08-12T12:25:00Z",
                "end": "2026-08-12T12:35:00Z",
                "feed": "sip",
            },
            run_id="manual__2026-08-27T08:00:00+00:00",
        )

        self.assertEqual(config.ticker, "SPY")
        self.assertEqual(config.start, "2026-08-12T12:25:00Z")
        self.assertEqual(config.end, "2026-08-12T12:35:00Z")
        self.assertEqual(config.feed, "sip")

    def test_rejects_lowercase_ticker_consistently_with_dag_schema(self) -> None:
        target = load_target()

        with self.assertRaisesRegex(ValueError, "ticker"):
            target.validate_run_config(
                {
                    "ticker": "spy",
                    "start": "2026-08-12T12:25:00Z",
                    "end": "2026-08-12T12:35:00Z",
                    "feed": "sip",
                },
                run_id="manual__lowercase",
            )

    def test_rejects_invalid_ticker(self) -> None:
        target = load_target()

        with self.assertRaisesRegex(ValueError, "ticker"):
            target.validate_run_config(
                {
                    "ticker": "NVDA; rm",
                    "start": "2026-08-12T12:25:00Z",
                    "end": "2026-08-12T12:35:00Z",
                    "feed": "sip",
                },
                run_id="manual__bad-ticker",
            )

    def test_rejects_unsupported_feed(self) -> None:
        target = load_target()

        with self.assertRaisesRegex(ValueError, "feed"):
            target.validate_run_config(
                {
                    "ticker": "NVDA",
                    "start": "2026-08-12T12:25:00Z",
                    "end": "2026-08-12T12:35:00Z",
                    "feed": "otc",
                },
                run_id="manual__bad-feed",
            )

    def test_rejects_naive_or_reversed_window(self) -> None:
        target = load_target()

        with self.assertRaisesRegex(ValueError, "timezone"):
            target.validate_run_config(
                {
                    "ticker": "NVDA",
                    "start": "2026-08-12T12:25:00",
                    "end": "2026-08-12T12:35:00Z",
                    "feed": "sip",
                },
                run_id="manual__naive-time",
            )

        with self.assertRaisesRegex(ValueError, "start must be before end"):
            target.validate_run_config(
                {
                    "ticker": "NVDA",
                    "start": "2026-08-12T12:35:00Z",
                    "end": "2026-08-12T12:25:00Z",
                    "feed": "sip",
                },
                run_id="manual__reversed-time",
            )

    def test_trace_id_is_stable_for_one_run_and_distinct_between_runs(self) -> None:
        target = load_target()
        params = {
            "ticker": "NVDA",
            "start": "2026-08-12T12:25:00Z",
            "end": "2026-08-12T12:35:00Z",
            "feed": "sip",
        }

        first = target.validate_run_config(params, run_id="manual__run-a")
        repeated = target.validate_run_config(params, run_id="manual__run-a")
        second = target.validate_run_config(params, run_id="manual__run-b")

        self.assertEqual(first.trace_id, repeated.trace_id)
        self.assertNotEqual(first.trace_id, second.trace_id)
        self.assertRegex(first.trace_id, re.compile(r"^airflow-market-replay-[0-9a-f]{16}$"))


class FakeCursor:
    def __init__(self, row) -> None:
        self.row = row
        self.parameters = None

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, _query, parameters) -> None:
        self.parameters = parameters

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, row) -> None:
        self.cursor_instance = FakeCursor(row)

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self):
        return self.cursor_instance


class AirflowMarketReplayStageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.target = load_target()
        self.config = self.target.validate_run_config(
            {
                "ticker": "SPY",
                "start": "2026-08-12T12:25:00Z",
                "end": "2026-08-12T12:35:00Z",
                "feed": "sip",
            },
            run_id="manual__stage-test",
        )

    def test_builds_each_stage_arguments_from_config_and_upstream_count(self) -> None:
        replay_args = self.target.build_replay_args(self.config, env_file=Path(".env"))
        replay_summary = {
            "trace_id": self.config.trace_id,
            "published_trades": 321,
            "topic": "raw.market-sip.v1",
            "offset_ranges": [
                {
                    "topic": "raw.market-sip.v1",
                    "partition": 2,
                    "start": 100,
                    "end": 421,
                }
            ],
        }
        consumer_args = self.target.build_consumer_args(
            replay_summary,
            env_file=Path(".env"),
        )
        spark_args = self.target.build_spark_args(self.config, replay_summary)

        self.assertEqual(replay_args.symbol, "SPY")
        self.assertEqual(replay_args.start, "2026-08-12T12:25:00Z")
        self.assertEqual(replay_args.end, "2026-08-12T12:35:00Z")
        self.assertEqual(replay_args.feed, "sip")
        self.assertEqual(replay_args.topic, "raw.market-sip.v1")
        self.assertEqual(replay_args.trace_id, self.config.trace_id)
        self.assertEqual(consumer_args.expected_count, 321)
        self.assertEqual(consumer_args.topic, "raw.market-sip.v1")
        self.assertEqual(consumer_args.offset_ranges, replay_summary["offset_ranges"])
        self.assertEqual(spark_args.symbols, ["SPY"])
        self.assertEqual(spark_args.topic, "raw.market-sip.v1")
        self.assertEqual(spark_args.trace_id, self.config.trace_id)
        self.assertEqual(spark_args.offset_ranges, replay_summary["offset_ranges"])

    def test_fails_when_spark_stage_loses_or_rejects_delivered_trades(self) -> None:
        good = {
            "published_trades": 100,
            "consumer_received": 100,
            "spark_input_trades": 100,
            "spark_invalid_trades": 0,
            "spark_valid_unique_trades": 100,
            "spark_output_bars": 10,
            "postgres_upserted_bars": 10,
        }

        self.target.verify_spark_result(good)
        for field, bad_value in (
            ("consumer_received", 99),
            ("spark_input_trades", 99),
            ("spark_invalid_trades", 1),
            ("spark_valid_unique_trades", 99),
            ("postgres_upserted_bars", 9),
        ):
            broken = {**good, field: bad_value}
            with self.subTest(field=field):
                with self.assertRaisesRegex(RuntimeError, "integrity"):
                    self.target.verify_spark_result(broken)

    def test_verifies_postgres_count_for_requested_symbol_and_window(self) -> None:
        connection = FakeConnection(("SPY", 10))

        summary = self.target.verify_stored_result(
            self.config,
            {"spark_output_bars": 10},
            database_url="postgresql://secret-value",
            connector=lambda *_args, **_kwargs: connection,
        )

        self.assertEqual(summary["stored_bars"], 10)
        self.assertEqual(summary["symbol"], "SPY")
        self.assertNotIn("database_url", summary)
        self.assertEqual(
            connection.cursor_instance.parameters,
            (
                "SPY",
                "2026-08-12T12:25:00Z",
                "2026-08-12T12:35:00Z",
                "sip",
            ),
        )

    def test_fails_when_postgres_bar_count_does_not_match_spark(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "stored bar count"):
            self.target.verify_stored_result(
                self.config,
                {"spark_output_bars": 10},
                database_url="postgresql://secret-value",
                connector=lambda *_args, **_kwargs: FakeConnection(("SPY", 9)),
            )

    def test_fails_when_postgres_returns_a_different_symbol(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "symbol"):
            self.target.verify_stored_result(
                self.config,
                {"spark_output_bars": 10},
                database_url="postgresql://secret-value",
                connector=lambda *_args, **_kwargs: FakeConnection(("NVDA", 10)),
            )


if __name__ == "__main__":
    unittest.main()
