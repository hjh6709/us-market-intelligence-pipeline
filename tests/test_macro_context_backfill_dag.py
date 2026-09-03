import importlib
import os
import unittest
from pathlib import Path


os.environ.setdefault("AIRFLOW_HOME", str(Path.cwd() / "airflow-runtime"))


class MacroContextBackfillDagTest(unittest.TestCase):
    def test_dag_maps_one_rate_limited_task_per_release(self) -> None:
        dag = importlib.import_module(
            "dags.macro_context_backfill_pipeline"
        ).macro_context_backfill_pipeline

        self.assertEqual(dag.dag_id, "macro_context_backfill_pipeline")
        self.assertEqual(
            set(dag.task_ids),
            {"build_event_work_items", "collect_event_macro_context"},
        )
        mapped = dag.get_task("collect_event_macro_context")
        self.assertEqual(type(mapped).__name__, "DecoratedMappedOperator")
        self.assertEqual(mapped.pool, "fred_api_pool")
        self.assertEqual(mapped.max_active_tis_per_dag, 1)


if __name__ == "__main__":
    unittest.main()
