import json
import os
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

from src.kafka_publisher import KafkaPublisher
from src.market_event import build_market_envelope
from src.postgres import postgres_bar_sink
from src.spark_market_processor import build_streams, create_market_spark


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://market:market@localhost:55432/market",
)


@unittest.skipUnless(
    os.environ.get("RUN_KAFKA_SPARK_POSTGRES_INTEGRATION") == "1",
    "set RUN_KAFKA_SPARK_POSTGRES_INTEGRATION=1 for the vertical slice",
)
class KafkaSparkPostgresIntegrationTest(unittest.TestCase):
    def test_replay_produces_one_idempotent_final_market_bar(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = f"raw-market-postgres-test-{uuid.uuid4().hex[:10]}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(10)
        migration = Path("db/migrations/001_market_bars.sql").read_text()
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(migration)
            connection.execute("TRUNCATE market_bars")

        spark = create_market_spark("kafka-spark-postgres-integration")
        query = None
        restarted = None
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint = Path(temp_dir) / "checkpoint"
            try:
                bars, _ = build_streams(
                    spark,
                    bootstrap_servers=bootstrap,
                    topic=topic,
                    starting_offsets="earliest",
                    symbols=["NVDA"],
                    watermark="2 minutes",
                )
                query = self.start_query(bars, checkpoint)

                trades = [
                    self.envelope(1, "2026-08-19T13:30:10Z", 100.0, 3),
                    self.envelope(2, "2026-08-19T13:30:30Z", 105.0, 2),
                    self.envelope(3, "2026-08-19T13:30:50Z", 102.0, 5),
                ]
                self.publish(bootstrap, topic, trades + [trades[-1]])
                self.publish(
                    bootstrap,
                    topic,
                    [self.envelope(4, "2026-08-19T13:33:30Z", 111.0, 1)],
                )
                query.processAllAvailable()

                before_restart = self.read_result()
                self.assertEqual(
                    before_restart,
                    [
                        (
                            "NVDA",
                            "2026-08-19T13:30:00+00:00",
                            "100.000000",
                            "105.000000",
                            "100.000000",
                            "102.000000",
                            10,
                            3,
                        )
                    ],
                )

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
                restarted = self.start_query(bars_after_restart, checkpoint)
                restarted.processAllAvailable()
                after_restart = self.read_result()

                self.assertEqual(after_restart, before_restart)
                print(
                    json.dumps(
                        {
                            "published_events": 5,
                            "unique_final_bar_trades": 3,
                            "postgres_rows_before_restart": len(before_restart),
                            "postgres_rows_after_restart": len(after_restart),
                            "final_bar": before_restart[0],
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
    def start_query(bars, checkpoint: Path):
        return (
            bars.writeStream.foreachBatch(postgres_bar_sink(DATABASE_URL))
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint))
            .trigger(processingTime="1 second")
            .start()
        )

    @staticmethod
    def read_result() -> list[tuple]:
        with psycopg.connect(DATABASE_URL) as connection:
            rows = connection.execute(
                """
                SELECT symbol, bar_start, open, high, low, close, volume, trade_count
                FROM market_bars
                ORDER BY symbol, bar_start
                """
            ).fetchall()
        return [
            (
                symbol,
                bar_start.isoformat(),
                str(open_),
                str(high),
                str(low),
                str(close),
                volume,
                count,
            )
            for symbol, bar_start, open_, high, low, close, volume, count in rows
        ]

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
            trace_id="postgres-vertical-integration",
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
