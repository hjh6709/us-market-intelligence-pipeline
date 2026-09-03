import importlib
import os
import unittest
from pathlib import Path


os.environ.setdefault("AIRFLOW_HOME", str(Path.cwd() / "airflow-runtime"))


class MarketContextOrchestratorDagTest(unittest.TestCase):
    def test_orchestrator_maps_bounded_yearly_child_runs(self) -> None:
        dag = importlib.import_module(
            "dags.market_context_backfill_orchestrator"
        ).market_context_backfill_orchestrator

        self.assertEqual(dag.dag_id, "market_context_backfill_orchestrator")
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(
            set(dag.task_ids), {"build_yearly_runs", "run_market_context_year"}
        )
        mapped = dag.get_task("run_market_context_year")
        self.assertEqual(type(mapped).__name__, "MappedOperator")
        self.assertEqual(
            mapped.partial_kwargs["trigger_dag_id"],
            "market_context_backfill_pipeline",
        )
        self.assertTrue(mapped.partial_kwargs["wait_for_completion"])


if __name__ == "__main__":
    unittest.main()
