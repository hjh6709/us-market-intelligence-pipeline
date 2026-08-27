"""Count Kafka records for one ingestion trace without logging payloads."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer

from src.kafka_publisher import DEFAULT_TOPIC
from src.live_market_smoke import _read_env_file


class KafkaTraceConsumerError(RuntimeError):
    """Raised when trace-count verification cannot complete safely."""


def count_trace_messages(
    consumer: Any,
    *,
    topic: str,
    trace_id: str,
    expected_count: int,
    timeout_seconds: float,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[int, int]:
    """Return matched and scanned counts, stopping at the expected count."""
    if expected_count < 1 or timeout_seconds <= 0:
        raise ValueError("expected_count and timeout_seconds must be positive")

    consumer.subscribe([topic])
    deadline = monotonic() + timeout_seconds
    matched = 0
    scanned = 0
    while matched < expected_count:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        message = consumer.poll(min(1.0, remaining))
        if message is None:
            continue
        if message.error():
            raise KafkaTraceConsumerError(str(message.error()))
        scanned += 1
        try:
            envelope = json.loads(message.value())
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            continue
        if envelope.get("trace_id") == trace_id:
            matched += 1
    return matched, scanned


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-id", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, int | str]:
    """Count one trace and return a log-safe delivery summary."""
    env_values = _read_env_file(args.env_file)
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or env_values.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    topic = os.environ.get("KAFKA_TOPIC") or env_values.get(
        "KAFKA_TOPIC", DEFAULT_TOPIC
    )
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"assignment-trace-count-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        matched, scanned = count_trace_messages(
            consumer,
            topic=topic,
            trace_id=args.trace_id,
            expected_count=args.expected_count,
            timeout_seconds=args.timeout,
        )
    finally:
        consumer.close()

    return {
        "step": "consumer_count",
        "topic": topic,
        "trace_id": args.trace_id,
        "expected_count": args.expected_count,
        "consumer_received": matched,
        "scanned_messages": scanned,
    }


def main(argv: Sequence[str] | None = None) -> int:
    summary = run(parse_args(argv))
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["consumer_received"] == summary["expected_count"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
