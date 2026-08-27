import json
import unittest
from pathlib import Path
from unittest.mock import patch

import src.kafka_trace_consumer as trace_consumer

from src.kafka_trace_consumer import (
    KafkaTraceConsumerError,
    count_trace_messages,
)


class FakeMessage:
    def __init__(
        self,
        value: dict | bytes,
        error=None,
        *,
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = value if isinstance(value, bytes) else json.dumps(value).encode()
        self._error = error
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def error(self):
        return self._error

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self, messages) -> None:
        self.messages = list(messages)
        self.assignments = []
        self.closed = False

    def assign(self, assignments) -> None:
        self.assignments = assignments

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def close(self) -> None:
        self.closed = True


class AdvancingClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        self.value += 0.1
        return self.value


class KafkaTraceConsumerTest(unittest.TestCase):
    def test_cli_accepts_explicit_topic(self) -> None:
        args = trace_consumer.parse_args(
            [
                "--trace-id",
                "airflow-run",
                "--expected-count",
                "2",
                "--topic",
                "raw.market-sip.v1",
                "--offset-ranges",
                '[{"topic":"raw.market-sip.v1","partition":0,"start":0,"end":2}]',
            ]
        )

        self.assertEqual(args.topic, "raw.market-sip.v1")

    def test_counts_only_messages_inside_the_published_offset_range(self) -> None:
        consumer = FakeConsumer(
            [
                FakeMessage({"trace_id": "assignment-run"}, partition=2, offset=41),
                FakeMessage({"trace_id": "assignment-run"}, partition=2, offset=42),
                FakeMessage({"trace_id": "assignment-run"}, partition=2, offset=43),
            ]
        )

        matched, scanned = count_trace_messages(
            consumer,
            topic="raw.market.v1",
            trace_id="assignment-run",
            expected_count=2,
            offset_ranges=[
                {"topic": "raw.market.v1", "partition": 2, "start": 41, "end": 43}
            ],
            timeout_seconds=5,
            monotonic=AdvancingClock(),
        )

        self.assertEqual((matched, scanned), (2, 2))
        self.assertEqual(len(consumer.assignments), 1)
        self.assertEqual(consumer.assignments[0].partition, 2)
        self.assertEqual(consumer.assignments[0].offset, 41)

    def test_counts_extra_same_trace_record_within_bounded_range(self) -> None:
        consumer = FakeConsumer(
            [
                FakeMessage({"trace_id": "assignment-run"}, offset=10),
                FakeMessage({"trace_id": "assignment-run"}, offset=11),
                FakeMessage({"trace_id": "assignment-run"}, offset=12),
            ]
        )

        matched, scanned = count_trace_messages(
            consumer,
            topic="raw.market.v1",
            trace_id="assignment-run",
            expected_count=2,
            offset_ranges=[
                {"topic": "raw.market.v1", "partition": 0, "start": 10, "end": 13}
            ],
            timeout_seconds=5,
            monotonic=AdvancingClock(),
        )

        self.assertEqual((matched, scanned), (3, 3))

    def test_ignores_malformed_json_and_returns_partial_count_on_timeout(self) -> None:
        consumer = FakeConsumer(
            [
                FakeMessage(b"not-json"),
                FakeMessage({"trace_id": "assignment-run"}),
            ]
        )

        matched, scanned = count_trace_messages(
            consumer,
            topic="raw.market.v1",
                trace_id="assignment-run",
                expected_count=2,
                offset_ranges=[
                    {"topic": "raw.market.v1", "partition": 0, "start": 0, "end": 2}
                ],
                timeout_seconds=0.5,
            monotonic=AdvancingClock(),
        )

        self.assertEqual((matched, scanned), (1, 2))

    def test_fails_on_kafka_consumer_error(self) -> None:
        consumer = FakeConsumer([FakeMessage(b"", error="broker unavailable")])

        with self.assertRaisesRegex(KafkaTraceConsumerError, "broker unavailable"):
            count_trace_messages(
                consumer,
                topic="raw.market.v1",
                trace_id="assignment-run",
                expected_count=1,
                offset_ranges=[
                    {"topic": "raw.market.v1", "partition": 0, "start": 0, "end": 1}
                ],
                timeout_seconds=5,
                monotonic=AdvancingClock(),
            )

    def test_callable_run_returns_delivery_summary(self) -> None:
        if not hasattr(trace_consumer, "run"):
            self.fail("Kafka trace consumer does not expose a callable run function")
        consumer = FakeConsumer(
            [
                FakeMessage({"trace_id": "airflow-run"}),
                FakeMessage({"trace_id": "airflow-run"}),
            ]
        )
        args = trace_consumer.parse_args(
            [
                "--trace-id",
                "airflow-run",
                "--expected-count",
                "2",
                "--offset-ranges",
                '[{"topic":"raw.market-sip.v1","partition":0,"start":0,"end":2}]',
                "--timeout",
                "5",
                "--env-file",
                str(Path(".env")),
            ]
        )

        with (
            patch.object(trace_consumer, "_read_env_file", return_value={"KAFKA_TOPIC": "raw.market-sip.v1"}),
            patch.object(trace_consumer, "Consumer", return_value=consumer),
            patch.object(trace_consumer.time, "monotonic", new=AdvancingClock()),
        ):
            summary = trace_consumer.run(args)

        self.assertEqual(
            summary,
            {
                "step": "consumer_count",
                "topic": "raw.market-sip.v1",
                "trace_id": "airflow-run",
                "expected_count": 2,
                "consumer_received": 2,
                "scanned_messages": 2,
                "offset_ranges": [
                    {
                        "topic": "raw.market-sip.v1",
                        "partition": 0,
                        "start": 0,
                        "end": 2,
                    }
                ],
            },
        )
        self.assertTrue(consumer.closed)


if __name__ == "__main__":
    unittest.main()
