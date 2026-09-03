import json
import unittest
from unittest.mock import patch

from src.kafka_publisher import KafkaDeliveryError, KafkaPublisher


ENVELOPE = {
    "event_id": "sha256:abc",
    "event_type": "market.trade.raw",
    "schema_version": 1,
    "source": "alpaca",
    "feed": "iex",
    "source_event_id": "23",
    "event_timestamp": "2026-08-19T13:30:00Z",
    "ingested_at": "2026-08-19T13:30:01Z",
    "trace_id": "run-1",
    "payload": {"T": "t", "S": "NVDA", "i": 23, "t": "2026-08-19T13:30:00Z"},
}


class RecordingProducer:
    def __init__(
        self,
        delivery_error=None,
        buffer_failures: int = 0,
        remaining: int = 0,
        partitions=None,
    ):
        self.delivery_error = delivery_error
        self.buffer_failures = buffer_failures
        self.remaining = remaining
        self.attempts = 0
        self.poll_calls = []
        self.records = []
        self.next_offset = 41
        self.partitions = iter(partitions or [])

    def produce(self, topic, *, key, value, on_delivery) -> None:
        self.attempts += 1
        if self.attempts <= self.buffer_failures:
            raise BufferError("queue full")
        self.records.append({"topic": topic, "key": key, "value": value})
        try:
            partition = next(self.partitions)
        except StopIteration:
            partition = 2
        on_delivery(
            self.delivery_error,
            DeliveryMessage(topic, partition=partition, offset=self.next_offset),
        )
        self.next_offset += 1

    def poll(self, timeout) -> None:
        self.poll_calls.append(timeout)

    def flush(self, timeout) -> int:
        return self.remaining


class DeliveryMessage:
    def __init__(self, topic: str, *, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class KafkaPublisherTest(unittest.TestCase):
    def test_publishes_symbol_key_and_canonical_json_value(self) -> None:
        recorder = RecordingProducer()
        publisher = KafkaPublisher("localhost:9092", producer=recorder)

        publisher.publish(ENVELOPE)

        record = recorder.records[0]
        self.assertEqual(record["topic"], "raw.market.v1")
        self.assertEqual(record["key"], b"NVDA")
        self.assertEqual(json.loads(record["value"]), ENVELOPE)
        self.assertEqual(recorder.poll_calls, [0])

    def test_accepts_explicit_replay_partition_key(self) -> None:
        recorder = RecordingProducer()
        publisher = KafkaPublisher("localhost:9092", producer=recorder)

        publisher.publish(ENVELOPE, key="CPI|2026-08-12|NVDA|segment-04")

        self.assertEqual(
            recorder.records[0]["key"],
            b"CPI|2026-08-12|NVDA|segment-04",
        )

    def test_default_client_enables_idempotence_and_all_acks(self) -> None:
        with patch("src.kafka_publisher.Producer") as producer_class:
            KafkaPublisher("kafka:19092")

        config = producer_class.call_args.args[0]
        self.assertEqual(config["bootstrap.servers"], "kafka:19092")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual(config["acks"], "all")

    def test_retries_when_local_producer_queue_is_full(self) -> None:
        recorder = RecordingProducer(buffer_failures=2)
        publisher = KafkaPublisher("localhost:9092", producer=recorder)

        publisher.publish(ENVELOPE)

        self.assertEqual(recorder.attempts, 3)
        self.assertEqual(recorder.poll_calls, [1, 1, 0])

    def test_raises_after_three_full_queue_results(self) -> None:
        publisher = KafkaPublisher(
            "localhost:9092", producer=RecordingProducer(buffer_failures=3)
        )

        with self.assertRaisesRegex(KafkaDeliveryError, "queue remained full"):
            publisher.publish(ENVELOPE)

    def test_close_fails_when_delivery_callback_reports_error(self) -> None:
        recorder = RecordingProducer(delivery_error=RuntimeError("broker unavailable"))
        publisher = KafkaPublisher("localhost:9092", producer=recorder)
        publisher.publish(ENVELOPE)

        with self.assertRaisesRegex(KafkaDeliveryError, "broker unavailable"):
            publisher.close()

    def test_close_fails_when_messages_remain_unflushed(self) -> None:
        publisher = KafkaPublisher(
            "localhost:9092", producer=RecordingProducer(remaining=1)
        )

        with self.assertRaisesRegex(KafkaDeliveryError, "1 message"):
            publisher.close()

    def test_reports_exact_written_offset_ranges_after_delivery(self) -> None:
        recorder = RecordingProducer()
        publisher = KafkaPublisher("localhost:9092", producer=recorder)

        publisher.publish(ENVELOPE)
        publisher.publish(ENVELOPE)
        publisher.close()

        self.assertEqual(
            publisher.offset_ranges,
            [{"topic": "raw.market.v1", "partition": 2, "start": 41, "end": 43}],
        )

    def test_reports_exact_delivery_count_per_partition(self) -> None:
        publisher = KafkaPublisher(
            "localhost:9092",
            producer=RecordingProducer(partitions=[0, 2, 0]),
        )

        for _ in range(3):
            publisher.publish(ENVELOPE)
        publisher.close()

        self.assertEqual(publisher.partition_counts, {0: 2, 2: 1})


if __name__ == "__main__":
    unittest.main()
