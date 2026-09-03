"""Reliable Kafka publishing boundary for canonical market envelopes."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from confluent_kafka import Producer


DEFAULT_TOPIC = "raw.market.v1"
DEFAULT_PRODUCER_CONFIG: dict[str, Any] = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "none",
    "linger.ms": 0,
}


class KafkaDeliveryError(RuntimeError):
    """Raised when a Kafka record cannot be queued or delivered."""


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = DEFAULT_TOPIC,
        producer: Any | None = None,
    ) -> None:
        config = {
            "bootstrap.servers": bootstrap_servers,
            **DEFAULT_PRODUCER_CONFIG,
        }
        self.topic = topic
        self._producer = producer if producer is not None else Producer(config)
        self._delivery_errors: list[str] = []
        self._delivered_ranges: dict[tuple[str, int], list[int]] = {}
        self._partition_counts: dict[int, int] = {}

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))
            return
        key = (message.topic(), int(message.partition()))
        partition = int(message.partition())
        self._partition_counts[partition] = self._partition_counts.get(partition, 0) + 1
        offset = int(message.offset())
        bounds = self._delivered_ranges.setdefault(key, [offset, offset + 1])
        bounds[0] = min(bounds[0], offset)
        bounds[1] = max(bounds[1], offset + 1)

    @property
    def offset_ranges(self) -> list[dict[str, int | str]]:
        """Return inclusive-start/exclusive-end ranges confirmed by Kafka."""
        return [
            {
                "topic": topic,
                "partition": partition,
                "start": bounds[0],
                "end": bounds[1],
            }
            for (topic, partition), bounds in sorted(self._delivered_ranges.items())
        ]

    @property
    def partition_counts(self) -> dict[int, int]:
        """Return callback-confirmed record counts for each Kafka partition."""
        return dict(sorted(self._partition_counts.items()))

    def publish(
        self,
        envelope: Mapping[str, Any],
        *,
        key: str | None = None,
    ) -> None:
        routing_key = key or str(envelope["payload"]["S"])
        value = json.dumps(
            envelope,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        for attempt in range(3):
            try:
                self._producer.produce(
                    self.topic,
                    key=routing_key.encode("utf-8"),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError as error:
                self._producer.poll(1)
                if attempt == 2:
                    raise KafkaDeliveryError(
                        "Kafka producer queue remained full after 3 attempts"
                    ) from error

    def close(self, timeout_seconds: float = 10.0) -> None:
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            raise KafkaDeliveryError(
                f"Kafka flush timed out with {remaining} message(s) still queued"
            )
        if self._delivery_errors:
            errors = "; ".join(self._delivery_errors)
            raise KafkaDeliveryError(f"Kafka delivery failed: {errors}")
