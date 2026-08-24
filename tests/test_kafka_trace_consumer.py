import json
import unittest

from src.kafka_trace_consumer import (
    KafkaTraceConsumerError,
    count_trace_messages,
)


class FakeMessage:
    def __init__(self, value: dict | bytes, error=None) -> None:
        self._value = value if isinstance(value, bytes) else json.dumps(value).encode()
        self._error = error

    def value(self) -> bytes:
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, messages) -> None:
        self.messages = list(messages)
        self.topics = []

    def subscribe(self, topics) -> None:
        self.topics = topics

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None


class AdvancingClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        self.value += 0.1
        return self.value


class KafkaTraceConsumerTest(unittest.TestCase):
    def test_counts_only_matching_trace_and_stops_at_expected_count(self) -> None:
        consumer = FakeConsumer(
            [
                FakeMessage({"trace_id": "older"}),
                FakeMessage({"trace_id": "assignment-run"}),
                FakeMessage({"trace_id": "assignment-run"}),
                FakeMessage({"trace_id": "assignment-run"}),
            ]
        )

        matched, scanned = count_trace_messages(
            consumer,
            topic="raw.market.v1",
            trace_id="assignment-run",
            expected_count=2,
            timeout_seconds=5,
            monotonic=AdvancingClock(),
        )

        self.assertEqual((matched, scanned), (2, 3))
        self.assertEqual(consumer.topics, ["raw.market.v1"])

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
                timeout_seconds=5,
                monotonic=AdvancingClock(),
            )


if __name__ == "__main__":
    unittest.main()
