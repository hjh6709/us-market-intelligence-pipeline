import importlib
import os
import unittest
from pathlib import Path


os.environ.setdefault("AIRFLOW_HOME", str(Path.cwd() / "airflow-runtime"))


class MarketContextBackfillDagTest(unittest.TestCase):
    def test_dag_maps_event_symbol_work_with_bounded_resources(self) -> None:
        dag = importlib.import_module(
            "dags.market_context_backfill_pipeline"
        ).market_context_backfill_pipeline

        self.assertEqual(dag.dag_id, "market_context_backfill_pipeline")
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(
            set(dag.params),
            {
                "event_types",
                "release_from",
                "release_to",
                "symbols",
                "feed",
                "data_cutoff",
            },
        )
        self.assertEqual(
            set(dag.task_ids),
            {
                "validate_run_config",
                "register_run",
                "build_work_items",
                "collect_market_context",
                "verify_run",
                "finish_run",
            },
        )
        mapped = dag.get_task("collect_market_context")
        self.assertEqual(type(mapped).__name__, "DecoratedMappedOperator")
        self.assertEqual(mapped.pool, "alpaca_api_pool")
        self.assertEqual(mapped.max_active_tis_per_dag, 4)

    def test_pool_script_declares_bounded_external_resources(self) -> None:
        script = Path("scripts/configure_airflow_pools.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('"alpaca_api_pool": 2', script)
        self.assertIn('"fred_api_pool": 1', script)
        self.assertIn('"spark_pool": 1', script)
        self.assertIn('"postgres_write_pool": 2', script)

    def test_ci_compiles_dag_sources(self) -> None:
        workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

        self.assertIn("compileall -q src dags tests", workflow)


if __name__ == "__main__":
    unittest.main()
