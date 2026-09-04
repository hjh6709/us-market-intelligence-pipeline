from decimal import Decimal

from pydantic import BaseModel

from src.serving_models import ExecutionReadinessView, ReadinessCheckView


CHECK_NAMES = (
    "market_data",
    "strategy_result",
    "strategy_performance",
    "event_surprise",
    "paper_execution",
    "position_recovery",
    "kill_switch",
)


class ReadinessInput(BaseModel):
    market_data_ready: bool
    strategy_result_ready: bool
    strategy_mean_net_return_pct: Decimal | None
    forecast: Decimal | None
    actual: Decimal | None
    surprise: Decimal | None
    paper_execution_enabled: bool
    position_recovery_enabled: bool
    kill_switch_enabled: bool


def evaluate_execution_readiness(value: ReadinessInput) -> ExecutionReadinessView:
    check_values = {
        "market_data": value.market_data_ready,
        "strategy_result": value.strategy_result_ready,
        "strategy_performance": (
            value.strategy_mean_net_return_pct is not None
            and value.strategy_mean_net_return_pct > 0
        ),
        "event_surprise": all(
            item is not None for item in (value.forecast, value.actual, value.surprise)
        ),
        "paper_execution": value.paper_execution_enabled,
        "position_recovery": value.position_recovery_enabled,
        "kill_switch": value.kill_switch_enabled,
    }
    failure_reasons = {
        "market_data": "required market data is incomplete",
        "strategy_result": "stored strategy result is unavailable",
        "strategy_performance": "exploratory strategy has not passed the strategy performance gate",
        "event_surprise": "consensus-versus-actual event surprise is unavailable",
        "paper_execution": "paper execution is not implemented and verified",
        "position_recovery": "position recovery is not implemented and verified",
        "kill_switch": "kill switch is not implemented and verified",
    }

    checks = [
        ReadinessCheckView(
            name=name,
            status="PASS" if check_values[name] else "FAIL",
            reason=None if check_values[name] else failure_reasons[name],
        )
        for name in CHECK_NAMES
    ]
    reasons = [failure_reasons[name] for name in CHECK_NAMES if not check_values[name]]
    reasons.append("release-level execution lock keeps this service research-only")

    return ExecutionReadinessView(
        stage="RESEARCH_ONLY",
        order_action="NO_TRADE",
        eligible_for_order=False,
        requires_human_approval=False,
        checks=checks,
        reasons=reasons,
    )
