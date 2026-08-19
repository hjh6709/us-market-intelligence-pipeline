"""Minimal Alpaca WebSocket authentication and live-trade smoke test."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


REQUIRED_TRADE_FIELDS = frozenset({"T", "S", "i", "x", "p", "s", "c", "t", "z"})


class AlpacaStreamError(RuntimeError):
    """Raised when Alpaca rejects or unexpectedly answers a stream request."""


def require_success(messages: Sequence[Mapping[str, Any]], expected_message: str) -> None:
    for message in messages:
        if message.get("T") == "success" and message.get("msg") == expected_message:
            return
        if message.get("T") == "error":
            raise AlpacaStreamError(f"Alpaca error {message.get('code')}: {message.get('msg')}")
    raise AlpacaStreamError(f"Expected Alpaca success message: {expected_message}")


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        values[name.strip()] = value.strip().strip("'\"")
    return values


def load_credentials(
    environ: Mapping[str, str] = os.environ,
    env_path: Path = Path(".env"),
) -> tuple[str, str]:
    file_values = _read_env_file(env_path)
    key_id = environ.get("APCA_API_KEY_ID") or file_values.get("APCA_API_KEY_ID", "")
    secret_key = environ.get("APCA_API_SECRET_KEY") or file_values.get(
        "APCA_API_SECRET_KEY", ""
    )
    if not key_id.strip() or not secret_key.strip():
        raise ValueError("Alpaca credentials are missing from the environment or .env")
    return key_id.strip(), secret_key.strip()


def build_auth_message(key_id: str, secret_key: str) -> str:
    return json.dumps({"action": "auth", "key": key_id, "secret": secret_key})


def build_subscribe_message(symbols: Sequence[str]) -> str:
    return json.dumps({"action": "subscribe", "trades": list(symbols)})


def is_complete_trade(message: Mapping[str, Any]) -> bool:
    return message.get("T") == "t" and REQUIRED_TRADE_FIELDS.issubset(message)


async def run_probe(
    feed: str,
    symbols: Sequence[str],
    max_trades: int,
    timeout_seconds: float,
) -> int:
    from websockets.asyncio.client import connect

    key_id, secret_key = load_credentials()
    url = f"wss://stream.data.alpaca.markets/v2/{feed}"
    trade_count = 0
    deadline = time.monotonic() + timeout_seconds

    async with connect(url, open_timeout=10) as websocket:
        connected = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        print(json.dumps({"step": "connected", "response": connected}, ensure_ascii=False))
        require_success(connected, "connected")

        await websocket.send(build_auth_message(key_id, secret_key))
        authenticated = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        print(
            json.dumps(
                {"step": "authenticated", "response": authenticated},
                ensure_ascii=False,
            )
        )
        require_success(authenticated, "authenticated")

        await websocket.send(build_subscribe_message(symbols))
        subscription = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        print(
            json.dumps(
                {"step": "subscribed", "response": subscription},
                ensure_ascii=False,
            )
        )

        while trade_count < max_trades:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            except TimeoutError:
                break

            messages = json.loads(raw_message)
            for message in messages:
                if is_complete_trade(message):
                    trade_count += 1
                    print(json.dumps({"step": "trade", "data": message}, ensure_ascii=False))
                    if trade_count >= max_trades:
                        break

    print(
        json.dumps(
            {
                "step": "summary",
                "feed": feed,
                "symbols": list(symbols),
                "complete_trades": trade_count,
            },
            ensure_ascii=False,
        )
    )
    return trade_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", choices=("test", "iex"), default="test")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--max-trades", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = args.symbols or (["FAKEPACA"] if args.feed == "test" else ["SPY", "QQQ", "NVDA"])
    trade_count = asyncio.run(
        run_probe(
            feed=args.feed,
            symbols=symbols,
            max_trades=args.max_trades,
            timeout_seconds=args.timeout,
        )
    )
    return 0 if trade_count > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
