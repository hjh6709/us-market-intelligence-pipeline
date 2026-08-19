"""Stream raw Alpaca trades into Kafka using the canonical event envelope."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.kafka_publisher import DEFAULT_TOPIC, KafkaPublisher
from src.live_market_smoke import (
    AlpacaStreamError,
    _read_env_file,
    build_auth_message,
    build_subscribe_message,
    load_credentials,
    require_success,
)
from src.market_event import build_market_envelope


def process_messages(
    messages: Sequence[Mapping[str, Any]],
    publisher: Any,
    feed: str,
    trace_id: str,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    """Publish trade messages and ignore Alpaca control or other data messages."""
    published = 0
    for message in messages:
        if message.get("T") != "t":
            continue
        envelope = build_market_envelope(message, feed, clock(), trace_id)
        publisher.publish(envelope)
        published += 1
    return published


def _load_setting(name: str, default: str, env_path: Path = Path(".env")) -> str:
    return os.environ.get(name) or _read_env_file(env_path).get(name) or default


def _raise_stream_error(messages: Sequence[Mapping[str, Any]]) -> None:
    for message in messages:
        if message.get("T") == "error":
            raise AlpacaStreamError(
                f"Alpaca error {message.get('code')}: {message.get('msg')}"
            )


async def run_collector(
    feed: str,
    symbols: Sequence[str],
    max_trades: int,
    timeout_seconds: float,
    bootstrap_servers: str,
    topic: str,
) -> int:
    from websockets.asyncio.client import connect

    key_id, secret_key = load_credentials()
    publisher = KafkaPublisher(bootstrap_servers, topic=topic)
    trace_id = f"collector-{uuid.uuid4()}"
    published = 0
    deadline = time.monotonic() + timeout_seconds

    try:
        url = f"wss://stream.data.alpaca.markets/v2/{feed}"
        async with connect(url, open_timeout=10) as websocket:
            connected = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            require_success(connected, "connected")

            await websocket.send(build_auth_message(key_id, secret_key))
            authenticated = json.loads(
                await asyncio.wait_for(websocket.recv(), timeout=10)
            )
            require_success(authenticated, "authenticated")

            await websocket.send(build_subscribe_message(symbols))
            subscription = json.loads(
                await asyncio.wait_for(websocket.recv(), timeout=10)
            )
            _raise_stream_error(subscription)

            while published < max_trades:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    messages = json.loads(
                        await asyncio.wait_for(websocket.recv(), timeout=remaining)
                    )
                except TimeoutError:
                    break
                _raise_stream_error(messages)
                for message in messages:
                    published += process_messages(
                        [message], publisher, feed, trace_id
                    )
                    if published >= max_trades:
                        break
    finally:
        publisher.close()

    print(
        json.dumps(
            {
                "step": "summary",
                "feed": feed,
                "symbols": list(symbols),
                "published_trades": published,
                "topic": topic,
            },
            ensure_ascii=False,
        )
    )
    return published


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", choices=("test", "iex"), default="iex")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--max-trades", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = args.symbols or (["FAKEPACA"] if args.feed == "test" else ["SPY", "QQQ", "NVDA"])
    published = asyncio.run(
        run_collector(
            feed=args.feed,
            symbols=symbols,
            max_trades=args.max_trades,
            timeout_seconds=args.timeout,
            bootstrap_servers=_load_setting(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            topic=_load_setting("KAFKA_TOPIC", DEFAULT_TOPIC),
        )
    )
    return 0 if published > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
