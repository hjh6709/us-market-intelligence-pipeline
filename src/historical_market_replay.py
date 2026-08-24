"""Replay actual Alpaca historical IEX trades through the Kafka ingestion path."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from src.kafka_publisher import DEFAULT_TOPIC, KafkaPublisher
from src.live_market_smoke import _read_env_file, is_complete_trade, load_credentials
from src.market_event import build_market_envelope


ALPACA_DATA_BASE_URL = "https://data.alpaca.markets/v2/stocks"


class HistoricalTradeError(RuntimeError):
    """Raised when historical trades cannot be fetched as a complete page set."""


class AlpacaHistoricalClient:
    def __init__(
        self,
        key_id: str,
        secret_key: str,
        *,
        timeout_seconds: float = 15.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        self._key_id = key_id
        self._secret_key = secret_key
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    def fetch_page(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        feed: str,
        limit: int,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        params = {
            "start": start,
            "end": end,
            "feed": feed,
            "limit": str(limit),
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token
        url = (
            f"{ALPACA_DATA_BASE_URL}/{quote(symbol, safe='')}/trades?"
            f"{urlencode(params)}"
        )
        request = Request(
            url,
            headers={
                "APCA-API-KEY-ID": self._key_id,
                "APCA-API-SECRET-KEY": self._secret_key,
                "Accept": "application/json",
            },
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read())
        except HTTPError as error:
            raise HistoricalTradeError(
                f"Alpaca historical trades request failed with HTTP {error.code}"
            ) from error
        except (TimeoutError, URLError) as error:
            raise HistoricalTradeError(
                "Alpaca historical trades request could not be completed"
            ) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise HistoricalTradeError(
                "Alpaca historical trades response was not valid JSON"
            ) from error

        trades = payload.get("trades")
        if not isinstance(trades, list):
            raise HistoricalTradeError(
                "Alpaca historical trades response did not contain a trades list"
            )
        next_page_token = payload.get("next_page_token")
        if next_page_token is not None and not isinstance(next_page_token, str):
            raise HistoricalTradeError("Alpaca returned an invalid next_page_token")
        return trades, next_page_token


def fetch_all_trades(
    client: AlpacaHistoricalClient,
    *,
    symbol: str,
    start: str,
    end: str,
    feed: str,
    limit: int,
    max_pages: int,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch complete pages up to a hard safety limit; never silently truncate."""
    trades: list[dict[str, Any]] = []
    page_token = None
    for page_number in range(1, max_pages + 1):
        page, page_token = client.fetch_page(
            symbol=symbol,
            start=start,
            end=end,
            feed=feed,
            limit=limit,
            page_token=page_token,
        )
        trades.extend(page)
        if not page_token:
            return trades, page_number
    raise HistoricalTradeError(
        f"Historical trade pagination exceeded the configured {max_pages} page limit"
    )


def normalize_historical_trade(
    symbol: str,
    trade: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = {
        "T": "t",
        "S": symbol,
        "i": trade.get("i"),
        "x": trade.get("x"),
        "p": trade.get("p"),
        "s": trade.get("s"),
        "c": trade.get("c"),
        "t": trade.get("t"),
        "z": trade.get("z"),
    }
    if not is_complete_trade(normalized):
        raise HistoricalTradeError(
            "Alpaca historical trade did not match the required trade contract"
        )
    return normalized


def publish_historical_trades(
    symbol: str,
    trades: Sequence[Mapping[str, Any]],
    publisher: Any,
    *,
    feed: str,
    trace_id: str,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    speed_multiplier: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    published = 0
    first_event_at: datetime | None = None
    replay_started_at: float | None = None
    for trade in trades:
        payload = normalize_historical_trade(symbol, trade)
        if speed_multiplier is not None:
            event_at = _parse_timestamp(payload["t"])
            if first_event_at is None:
                first_event_at = event_at
                replay_started_at = monotonic()
            else:
                assert replay_started_at is not None
                delay = _replay_delay_seconds(
                    first_event_at,
                    event_at,
                    speed_multiplier,
                    monotonic() - replay_started_at,
                )
                if delay:
                    sleep(delay)
        publisher.publish(build_market_envelope(payload, feed, clock(), trace_id))
        published += 1
    return published


def _replay_delay_seconds(
    first_event_at: datetime,
    event_at: datetime,
    speed_multiplier: float,
    elapsed: float,
) -> float:
    """Return how long to wait so event-time gaps follow the requested speed."""
    target_elapsed = (event_at - first_event_at).total_seconds() / speed_multiplier
    return max(0.0, target_elapsed - elapsed)


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise HistoricalTradeError(
            "start and end must be RFC-3339 timestamps"
        ) from error
    if parsed.tzinfo is None:
        raise HistoricalTradeError("start and end must include a timezone")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="SMH")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--feed", choices=("iex",), default="iex")
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument(
        "--trace-id",
        default=None,
        help="Optional run identifier used to verify Kafka consumer counts",
    )
    parser.add_argument(
        "--speed-multiplier",
        type=float,
        default=None,
        help="Replay event-time gaps at 1x, 10x, 50x, or 100x; omit for no delay",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if _parse_timestamp(args.start) >= _parse_timestamp(args.end):
        raise HistoricalTradeError("start must be before end")
    if not 1 <= args.limit <= 10_000 or args.max_pages < 1:
        raise HistoricalTradeError("limit or max-pages is outside the safe range")
    if args.speed_multiplier is not None and args.speed_multiplier <= 0:
        raise HistoricalTradeError("speed-multiplier must be positive")

    key_id, secret_key = load_credentials(env_path=args.env_file)
    env_values = _read_env_file(args.env_file)
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or env_values.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    topic = os.environ.get("KAFKA_TOPIC") or env_values.get(
        "KAFKA_TOPIC", DEFAULT_TOPIC
    )
    trades, pages = fetch_all_trades(
        AlpacaHistoricalClient(key_id, secret_key),
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        feed=args.feed,
        limit=args.limit,
        max_pages=args.max_pages,
    )
    publisher = KafkaPublisher(bootstrap_servers, topic=topic)
    trace_id = args.trace_id or f"historical-replay-{uuid.uuid4()}"
    replay_started_at = time.monotonic()
    try:
        published = publish_historical_trades(
            args.symbol,
            trades,
            publisher,
            feed=args.feed,
            trace_id=trace_id,
            speed_multiplier=args.speed_multiplier,
        )
    finally:
        publisher.close()
    duration_seconds = time.monotonic() - replay_started_at
    events_per_second = published / duration_seconds if duration_seconds else 0.0

    print(
        json.dumps(
            {
                "step": "summary",
                "source": "alpaca_historical_trades",
                "feed": args.feed,
                "symbol": args.symbol,
                "start": args.start,
                "end": args.end,
                "pages": pages,
                "fetched_trades": len(trades),
                "published_trades": published,
                "topic": topic,
                "trace_id": trace_id,
                "speed_multiplier": args.speed_multiplier,
                "duration_seconds": round(duration_seconds, 6),
                "events_per_second": round(events_per_second, 3),
            },
            ensure_ascii=False,
        )
    )
    return 0 if published else 2


if __name__ == "__main__":
    raise SystemExit(main())
