import json
import os
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic

from src.kafka_publisher import KafkaPublisher
from src.market_event import build_market_envelope
from src.spark_market_processor import build_streams, create_market_spark


@unittest.skipUnless(
    os.environ.get("RUN_SPARK_KAFKA_INTEGRATION") == "1",
    "set RUN_SPARK_KAFKA_INTEGRATION=1 to test Spark with local Kafka",
)
class SparkMarketProcessorIntegrationTest(unittest.TestCase):
    def test_finalizes_deduplicated_late_aware_bar_and_recovers_checkpoint(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = f"raw-market-spark-test-{uuid.uuid4().hex[:10]}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(10)
        spark = create_market_spark("spark-kafka-integration-test")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            checkpoint = root / "checkpoint"
            output = root / "bars"
            query = None
            restarted = None
            try:
                bars, _ = build_streams(
                    spark,
                    bootstrap_servers=bootstrap,
                    topic=topic,
                    starting_offsets="earliest",
                    symbols=["NVDA"],
                    watermark="2 minutes",
                )
                query = (
                    bars.writeStream.format("parquet")
                    .outputMode("append")
                    .option("path", str(output))
                    .option("checkpointLocation", str(checkpoint))
                    .trigger(processingTime="1 second")
                    .start()
                )

                first = [
                    self.envelope(1, "2026-08-19T13:30:10Z", 100.0, 3),
                    self.envelope(2, "2026-08-19T13:30:30Z", 105.0, 2),
                    self.envelope(3, "2026-08-19T13:30:50Z", 102.0, 5),
                ]
                self.publish(bootstrap, topic, first + [first[-1]])
                query.processAllAvailable()

                self.publish(
                    bootstrap,
                    topic,
                    [
                        self.envelope(4, "2026-08-19T13:31:30Z", 110.0, 1),
                        self.envelope(5, "2026-08-19T13:30:20Z", 99.0, 1),
                    ],
                )
                query.processAllAvailable()

                self.publish(
                    bootstrap,
                    topic,
                    [self.envelope(6, "2026-08-19T13:33:30Z", 111.0, 1)],
                )
                query.processAllAvailable()

                self.publish(
                    bootstrap,
                    topic,
                    [self.envelope(7, "2026-08-19T13:30:40Z", 500.0, 100)],
                )
                query.processAllAvailable()

                result = spark.read.parquet(str(output)).where(
                    "symbol = 'NVDA' AND bar_start = timestamp'2026-08-19 13:30:00'"
                )
                row = result.collect()[0]
                self.assertEqual(str(row.open), "100.000000")
                self.assertEqual(str(row.high), "105.000000")
                self.assertEqual(str(row.low), "99.000000")
                self.assertEqual(str(row.close), "102.000000")
                self.assertEqual(row.volume, 11)
                self.assertEqual(row.trade_count, 4)
                self.assertEqual(str(row.vwap), "101.727273")
                before_restart = spark.read.parquet(str(output)).count()
                progress = query.lastProgress
                dropped_by_watermark = sum(
                    operator.get("numRowsDroppedByWatermark", 0)
                    for operator in progress.get("stateOperators", [])
                )
                self.assertGreaterEqual(dropped_by_watermark, 1)

                query.stop()
                query = None
                bars_after_restart, _ = build_streams(
                    spark,
                    bootstrap_servers=bootstrap,
                    topic=topic,
                    starting_offsets="earliest",
                    symbols=["NVDA"],
                    watermark="2 minutes",
                )
                restarted = (
                    bars_after_restart.writeStream.format("parquet")
                    .outputMode("append")
                    .option("path", str(output))
                    .option("checkpointLocation", str(checkpoint))
                    .trigger(processingTime="1 second")
                    .start()
                )
                restarted.processAllAvailable()
                after_restart = spark.read.parquet(str(output)).count()
                self.assertEqual(after_restart, before_restart)
                self.assertIsNotNone(progress)
                print(
                    json.dumps(
                        {
                            "final_bar": {
                                "open": str(row.open),
                                "high": str(row.high),
                                "low": str(row.low),
                                "close": str(row.close),
                                "volume": row.volume,
                                "trade_count": row.trade_count,
                                "vwap": str(row.vwap),
                            },
                            "dropped_by_watermark": dropped_by_watermark,
                            "rows_before_restart": before_restart,
                            "rows_after_restart": after_restart,
                        },
                        ensure_ascii=False,
                    )
                )
            finally:
                if query is not None and query.isActive:
                    query.stop()
                if restarted is not None and restarted.isActive:
                    restarted.stop()
                spark.stop()
                admin.delete_topics([topic], operation_timeout=10)[topic].result(10)

    @staticmethod
    def envelope(trade_id: int, timestamp: str, price: float, size: int) -> dict:
        payload = {
            "T": "t",
            "S": "NVDA",
            "i": trade_id,
            "x": "V",
            "p": price,
            "s": size,
            "c": ["@"],
            "t": timestamp,
            "z": "C",
        }
        return build_market_envelope(
            payload,
            feed="iex",
            ingested_at=datetime.now(timezone.utc),
            trace_id="spark-integration",
        )

    @staticmethod
    def publish(bootstrap: str, topic: str, envelopes: list[dict]) -> None:
        publisher = KafkaPublisher(bootstrap, topic=topic)
        for envelope in envelopes:
            publisher.publish(envelope)
        publisher.close()
        time.sleep(0.2)


if __name__ == "__main__":
    unittest.main()
