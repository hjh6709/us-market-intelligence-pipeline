import json
import os
import time
import unittest
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer

from src.kafka_publisher import KafkaPublisher
from src.market_event import build_market_envelope


@unittest.skipUnless(
    os.environ.get("RUN_KAFKA_INTEGRATION") == "1",
    "set RUN_KAFKA_INTEGRATION=1 to test a local Kafka broker",
)
class KafkaMarketProducerIntegrationTest(unittest.TestCase):
    def test_publishes_and_consumes_canonical_market_record(self) -> None:
        bootstrap_servers = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"market-producer-test-{uuid.uuid4()}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(["raw.market.v1"])

        try:
            assignment_deadline = time.monotonic() + 10
            while not consumer.assignment() and time.monotonic() < assignment_deadline:
                consumer.poll(0.1)
            self.assertTrue(consumer.assignment(), "consumer did not receive an assignment")

            raw = {
                "T": "t",
                "S": "NVDA",
                "i": 23,
                "x": "V",
                "p": 221.69,
                "s": 5,
                "c": ["@", "I"],
                "z": "C",
                "t": "2026-08-19T13:30:00.102733966Z",
            }
            envelope = build_market_envelope(
                raw,
                feed="test",
                ingested_at=datetime(2026, 8, 19, 13, 30, 1, tzinfo=timezone.utc),
                trace_id="integration-test",
            )
            publisher = KafkaPublisher(bootstrap_servers)
            publisher.publish(envelope)
            publisher.close()

            message = None
            message_deadline = time.monotonic() + 10
            while message is None and time.monotonic() < message_deadline:
                candidate = consumer.poll(0.5)
                if candidate is not None:
                    message = candidate

            self.assertIsNotNone(message, "record was not consumed within 10 seconds")
            self.assertIsNone(message.error())
            self.assertEqual(message.key(), b"NVDA")
            self.assertEqual(json.loads(message.value()), envelope)
        finally:
            consumer.close()


if __name__ == "__main__":
    unittest.main()
