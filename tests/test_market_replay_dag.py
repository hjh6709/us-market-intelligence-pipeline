import importlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


os.environ.setdefault("AIRFLOW_HOME", str(Path.cwd() / "airflow-runtime"))


def load_dag_module():
    try:
        return importlib.import_module("dags.market_replay_pipeline")
    except ModuleNotFoundError as error:
        raise AssertionError("market replay DAG is not implemented") from error


class MarketReplayDagTest(unittest.TestCase):
    def test_editable_install_exposes_src_package_outside_repository(self) -> None:
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        with tempfile.TemporaryDirectory() as outside_repository:
            result = subprocess.run(
                [sys.executable, "-c", "import src.airflow_market_replay"],
                cwd=outside_repository,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_exposes_parameterized_manual_dag_with_ordered_pipeline_tasks(self) -> None:
        module = load_dag_module()
        dag = module.market_sip_replay_pipeline

        self.assertEqual(dag.dag_id, "market_sip_replay_pipeline")
        self.assertIsNone(dag.schedule)
        self.assertEqual(set(dag.params), {"ticker", "start", "end", "feed"})
        self.assertEqual(
            set(dag.task_ids),
            {
                "validate_run_config",
                "replay_trades_to_kafka",
                "verify_kafka_delivery",
                "build_minute_bars_with_spark",
                "verify_stored_result",
            },
        )

        expected_chain = [
            "validate_run_config",
            "replay_trades_to_kafka",
            "verify_kafka_delivery",
            "build_minute_bars_with_spark",
            "verify_stored_result",
        ]
        for upstream_id, downstream_id in zip(expected_chain, expected_chain[1:]):
            upstream = dag.get_task(upstream_id)
            self.assertEqual(upstream.downstream_task_ids, {downstream_id})


if __name__ == "__main__":
    unittest.main()
