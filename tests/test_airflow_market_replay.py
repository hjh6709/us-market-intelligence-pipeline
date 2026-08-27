import importlib
import re
import unittest


def load_target():
    try:
        return importlib.import_module("src.airflow_market_replay")
    except ModuleNotFoundError as error:
        raise AssertionError("Airflow market replay contract is not implemented") from error


class AirflowMarketReplayConfigTest(unittest.TestCase):
    def test_accepts_ticker_feed_and_timezone_aware_window(self) -> None:
        target = load_target()

        config = target.validate_run_config(
            {
                "ticker": "spy",
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


if __name__ == "__main__":
    unittest.main()
