import tempfile
import unittest
from pathlib import Path

from src.spark_market_processor import (
    KAFKA_CONNECTOR_PACKAGE,
    checkpoint_paths,
    parse_args,
    summarize_invalid_reasons,
)
from src.spark_session import create_local_spark


class SparkMarketProcessorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_local_spark("spark-market-processor-unit-test")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_uses_pinned_spark_kafka_connector(self) -> None:
        self.assertEqual(
            KAFKA_CONNECTOR_PACKAGE,
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )

    def test_cli_defaults_to_live_offsets_and_two_minute_watermark(self) -> None:
        args = parse_args([])

        self.assertEqual(args.bootstrap_servers, "localhost:9092")
        self.assertEqual(args.topic, "raw.market.v1")
        self.assertEqual(args.symbols, ["SPY", "QQQ", "NVDA"])
        self.assertEqual(args.starting_offsets, "latest")
        self.assertEqual(args.watermark, "2 minutes")

    def test_separates_stateful_query_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bars, invalid = checkpoint_paths(Path(temp_dir))

        self.assertEqual(bars.name, "bars")
        self.assertEqual(invalid.name, "invalid-metrics")
        self.assertNotEqual(bars, invalid)

    def test_summarizes_invalid_reasons_per_micro_batch(self) -> None:
        frame = self.spark.createDataFrame(
            [(["INVALID_PRICE", "INVALID_SIZE"],), (["INVALID_PRICE"],)],
            ["reason_codes"],
        )

        counts = {
            row.reason: row["count"]
            for row in summarize_invalid_reasons(frame).collect()
        }

        self.assertEqual(counts, {"INVALID_PRICE": 2, "INVALID_SIZE": 1})


if __name__ == "__main__":
    unittest.main()
