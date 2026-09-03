"""Fan a full-history market-context backfill into bounded yearly DAG runs."""

from __future__ import annotations

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task

from src.market_context_backfill import build_yearly_backfill_configs


DEFAULT_SYMBOLS = ["SPY", "QQQ", "IWM", "TLT", "XLF", "SMH", "GLD", "NVDA", "AAPL", "JPM"]


@dag(
    dag_id="market_context_backfill_orchestrator",
    description="Run the full economic-event market backfill one year at a time",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "event_types": Param(
            ["CPI", "EMPLOYMENT", "PCE", "FOMC"],
            type="array",
            items={"type": "string"},
            minItems=1,
            uniqueItems=True,
        ),
        "release_from": Param("2022-01-01", type="string", format="date"),
        "release_to": Param("2026-08-26", type="string", format="date"),
        "symbols": Param(
            DEFAULT_SYMBOLS,
            type="array",
            items={"type": "string", "pattern": r"^[A-Z][A-Z0-9.-]{0,9}$"},
            minItems=1,
            uniqueItems=True,
        ),
        "feed": Param("sip", type="string", enum=["sip", "iex"]),
        "data_cutoff": Param(
            "2026-09-03T00:00:00Z", type="string", format="date-time"
        ),
    },
    tags=["market-data", "economic-events", "orchestrator"],
)
def build_market_context_backfill_orchestrator():
    @task(task_id="build_yearly_runs")
    def build_yearly_runs() -> list[dict]:
        config = dict(get_current_context()["params"])
        yearly = build_yearly_backfill_configs(config)
        # Validate each child before creating any external API work.
        from src.market_context_backfill import select_market_context_work

        for child in yearly:
            select_market_context_work(child)
        return yearly

    configs = build_yearly_runs()
    TriggerDagRunOperator.partial(
        task_id="run_market_context_year",
        trigger_dag_id="market_context_backfill_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    ).expand(conf=configs)


market_context_backfill_orchestrator = build_market_context_backfill_orchestrator()
