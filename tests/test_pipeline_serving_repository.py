import unittest
from datetime import UTC, datetime

from src.pipeline_serving import PostgresPipelineRepository


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        self.connection.executions.append((" ".join(sql.split()), params))
        self.rows = self.connection.results.pop(0)

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, results):
        self.results = list(results)
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return FakeCursor(self)


class Connect:
    def __init__(self, results):
        self.connection = FakeConnection(results)

    def __call__(self, *_args, **_kwargs):
        return self.connection


class PipelineServingRepositoryTest(unittest.TestCase):
    def test_run_aggregation_does_not_fabricate_unrecorded_counts(self):
        now = datetime(2026, 9, 8, tzinfo=UTC)
        connect = Connect([[("run-1", "dag", "SUCCEEDED", now, now, 2, 0, 1, 0, 0, 2, None, None)]])
        repository = PostgresPipelineRepository("postgresql://unused", connect=connect)

        records = repository.list_runs(
            dag_id="dag", status="SUCCEEDED", limit=10, offset=0
        )

        sql, params = connect.connection.executions[0]
        self.assertIn("status = 'WARN'", sql)
        self.assertIn("status = 'FAIL'", sql)
        self.assertIn("check_name = 'observed_bar_coverage'", sql)
        self.assertEqual(params, ("dag", "SUCCEEDED", 10, 0))
        self.assertIsNone(records[0].input_count)
        self.assertIsNone(records[0].output_count)
        self.assertEqual(records[0].warning_check_count, 1)
        self.assertEqual(records[0].failed_check_count, 0)


if __name__ == "__main__":
    unittest.main()
