import unittest
from decimal import Decimal

from src.execution_readiness import ReadinessInput, evaluate_execution_readiness


def fully_ready_input() -> ReadinessInput:
    return ReadinessInput(
        market_data_ready=True,
        strategy_result_ready=True,
        strategy_mean_net_return_pct=Decimal("0.25"),
        forecast=Decimal("2.9"),
        actual=Decimal("2.7"),
        surprise=Decimal("-0.2"),
        paper_execution_enabled=True,
        position_recovery_enabled=True,
        kill_switch_enabled=True,
    )


def current_project_input() -> ReadinessInput:
    return ReadinessInput(
        market_data_ready=True,
        strategy_result_ready=True,
        strategy_mean_net_return_pct=Decimal("-0.1565"),
        forecast=None,
        actual=None,
        surprise=None,
        paper_execution_enabled=False,
        position_recovery_enabled=False,
        kill_switch_enabled=False,
    )


class ExecutionReadinessTest(unittest.TestCase):
    def test_research_signal_never_becomes_an_order_in_this_release(self):
        result = evaluate_execution_readiness(fully_ready_input())

        self.assertEqual(result.stage, "RESEARCH_ONLY")
        self.assertEqual(result.order_action, "NO_TRADE")
        self.assertFalse(result.eligible_for_order)
        self.assertFalse(result.requires_human_approval)

    def test_negative_strategy_and_missing_surprise_are_reported_separately(self):
        result = evaluate_execution_readiness(current_project_input())
        checks = {item.name: item.status for item in result.checks}

        self.assertEqual(
            list(checks),
            [
                "market_data",
                "strategy_result",
                "strategy_performance",
                "event_surprise",
                "paper_execution",
                "position_recovery",
                "kill_switch",
            ],
        )
        self.assertEqual(checks["market_data"], "PASS")
        self.assertEqual(checks["strategy_result"], "PASS")
        self.assertEqual(checks["strategy_performance"], "FAIL")
        self.assertEqual(checks["event_surprise"], "FAIL")
        self.assertIn("strategy performance gate", " ".join(result.reasons))

    def test_fully_ready_checks_do_not_bypass_release_level_lock(self):
        result = evaluate_execution_readiness(fully_ready_input())

        self.assertTrue(all(check.status == "PASS" for check in result.checks))
        self.assertEqual(result.stage, "RESEARCH_ONLY")
        self.assertEqual(result.order_action, "NO_TRADE")
        self.assertIn("release-level execution lock", " ".join(result.reasons))


if __name__ == "__main__":
    unittest.main()
