import json
import tempfile
import unittest
from pathlib import Path

from src.pipeline_experiment import ExperimentResult, failed_result, write_result


class PipelineExperimentTest(unittest.TestCase):
    def test_result_is_machine_readable_and_does_not_serialize_secrets(self) -> None:
        result = ExperimentResult(
            experiment_run_id="run-1",
            dataset_id="dataset-1",
            environment="local",
            status="succeeded",
            raw_input_trades=100,
            kafka_published=100,
            kafka_consumed=100,
            spark_input=100,
            spark_invalid=0,
            spark_duplicates=0,
            spark_output_bars=10,
            postgres_stored_bars=10,
            postgres_business_key_duplicates=0,
            duration_seconds=1.0,
            events_per_second=100.0,
        )

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.json"
            write_result(result, path)
            payload = path.read_text(encoding="utf-8")

        self.assertEqual(json.loads(payload)["kafka_consumed"], 100)
        self.assertNotIn("postgresql://", payload)
        self.assertNotIn("APCA_API", payload)

    def test_failed_result_records_the_failure_without_secrets(self) -> None:
        result = failed_result(
            experiment_run_id="load-1",
            dataset_id="dataset-1",
            environment="local",
            raw_input_trades=7_360_804,
            started=100.0,
            finished=220.0,
            error=RuntimeError(
                "database postgresql://market:secret@localhost/market failed"
            ),
            kafka_published=7_360_804,
            kafka_consumed=7_360_804,
        )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error_type, "RuntimeError")
        self.assertEqual(result.kafka_published, 7_360_804)
        self.assertNotIn("postgresql://", result.error_message or "")
        self.assertNotIn("secret", result.error_message or "")


if __name__ == "__main__":
    unittest.main()
