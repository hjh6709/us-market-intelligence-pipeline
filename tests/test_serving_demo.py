import unittest
from types import SimpleNamespace

from src.serving_demo import run_serving_demo


class FakeService:
    def __init__(self, calls):
        self.calls = calls

    def get_event_symbol_detail(self, event_id, symbol):
        self.calls.append("read_result")
        return SimpleNamespace(
            impacts=[1, 2, 3, 4],
            execution_readiness=SimpleNamespace(
                stage="RESEARCH_ONLY", order_action="NO_TRADE"
            ),
        )

    def get_bars(self, event_id, symbol, timeframe):
        return [SimpleNamespace()]


class ServingDemoTest(unittest.TestCase):
    def test_demo_runs_process_store_read_in_order(self):
        calls = []

        def impact_runner(database_url, **kwargs):
            calls.append("process_impacts")
            self.assertEqual(kwargs["event_ids"], ["event"])
            self.assertEqual(kwargs["symbols"], ["NVDA"])
            return 1, 4

        def strategy_runner(database_url, **kwargs):
            calls.append("store_strategy")
            return {"rows": 1}

        result = run_serving_demo(
            "postgresql://unused",
            "event",
            "NVDA",
            impact_runner=impact_runner,
            strategy_runner=strategy_runner,
            service=FakeService(calls),
            duplicate_counter=lambda *_args: 0,
        )

        self.assertEqual(calls, ["process_impacts", "store_strategy", "read_result"])
        self.assertEqual(result.processing.impact_rows_upserted, 4)
        self.assertEqual(result.storage.strategy_rows_upserted, 1)
        self.assertEqual(result.read.bar_timeframes, ["1m", "3m", "5m"])
        self.assertEqual(result.result.order_action, "NO_TRADE")

    def test_demo_rejects_lowercase_symbol_before_processing(self):
        with self.assertRaises(ValueError):
            run_serving_demo(
                "postgresql://unused",
                "event",
                "nvda",
                impact_runner=lambda *_args, **_kwargs: (1, 4),
                strategy_runner=lambda *_args, **_kwargs: {"rows": 1},
                service=FakeService([]),
            )


if __name__ == "__main__":
    unittest.main()
