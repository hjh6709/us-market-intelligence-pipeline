import ast
import unittest
from pathlib import Path


DAG_PATH = Path("dags/fred_macro_dag.py")
COMPOSE_PATH = Path("compose.yml")
DOCKERFILE_PATH = Path("docker/airflow/Dockerfile")


class FredMacroDagContractTest(unittest.TestCase):
    def test_dag_uses_airflow_3_public_sdk_and_pinned_contract(self) -> None:
        source = DAG_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)

        self.assertIn("from airflow.sdk import dag, get_current_context, task", source)
        self.assertIn('dag_id="fred_macro_daily"', source)
        self.assertIn('schedule="0 14 * * *"', source)
        self.assertIn("catchup=False", source)
        self.assertIn('task_id="ingest_series"', source)
        self.assertIn('task_id="quality_gate"', source)
        self.assertIn("retries", source)
        self.assertIn("retry_exponential_backoff", source)
        self.assertIn("max_retry_delay", source)
        self.assertIn("execution_timeout", source)
        self.assertIn(".expand(series_id=list(FRED_SERIES))", source)
        self.assertNotIn("api_key=", source)
        self.assertNotIn("postgresql://", source)
        self.assertTrue(any(isinstance(node, ast.FunctionDef) for node in ast.walk(tree)))

    def test_batch_profile_is_isolated_and_local_only(self) -> None:
        compose = COMPOSE_PATH.read_text(encoding="utf-8")
        dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")

        self.assertIn("airflow:", compose)
        self.assertIn('profiles: ["batch"]', compose)
        self.assertIn('127.0.0.1:8080:8080', compose)
        self.assertIn("./dags:/opt/project/dags:ro", compose)
        self.assertIn("./src:/opt/project/src:ro", compose)
        self.assertIn("airflow_runtime:/opt/airflow", compose)
        self.assertIn("apache/airflow:3.3.0", dockerfile)
        self.assertNotIn("COPY .env", dockerfile)


if __name__ == "__main__":
    unittest.main()
