"""Fetch and store Alpaca historical one-minute bars without using Kafka."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg

from src.postgres import UPSERT_MARKET_BAR_SQL


ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"


class HistoricalBarError(RuntimeError):
    """Raised when a historical bar request or payload is incomplete."""


@dataclass(frozen=True)
class HistoricalBar:
    symbol: str
    bar_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    vwap: Decimal | None


class AlpacaHistoricalBarsClient:
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
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        feed: str,
        limit: int,
        page_token: str | None = None,
    ) -> tuple[dict[str, list[dict[str, Any]]], str | None]:
        if not symbols:
            raise ValueError("At least one symbol is required")
        params = {
            "symbols": ",".join(symbols),
            "timeframe": "1Min",
            "start": _rfc3339(start),
            "end": _rfc3339(end),
            "feed": feed,
            "adjustment": "raw",
            "sort": "asc",
            "limit": str(limit),
        }
        if page_token:
            params["page_token"] = page_token
        request = Request(
            f"{ALPACA_BARS_URL}?{urlencode(params)}",
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
            raise HistoricalBarError(
                f"Alpaca historical bars request failed with HTTP {error.code}"
            ) from error
        except (TimeoutError, URLError) as error:
            raise HistoricalBarError(
                "Alpaca historical bars request could not be completed"
            ) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise HistoricalBarError(
                "Alpaca historical bars response was not valid JSON"
            ) from error

        raw_bars = payload.get("bars")
        if not isinstance(raw_bars, Mapping):
            raise HistoricalBarError("Alpaca response did not contain a bars object")
        bars = {}
        for symbol, rows in raw_bars.items():
            if not isinstance(symbol, str) or not isinstance(rows, list):
                raise HistoricalBarError("Alpaca returned an invalid bars object")
            bars[symbol] = rows
        next_page_token = payload.get("next_page_token")
        if next_page_token is not None and not isinstance(next_page_token, str):
            raise HistoricalBarError("Alpaca returned an invalid next_page_token")
        return bars, next_page_token


def fetch_all_bars(
    client: AlpacaHistoricalBarsClient,
    *,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    feed: str,
    limit: int = 10_000,
    max_pages: int = 20,
) -> tuple[list[HistoricalBar], int]:
    if end <= start:
        raise ValueError("end must be after start")
    bars = []
    requested_symbols = set(symbols)
    page_token = None
    for page_number in range(1, max_pages + 1):
        page, page_token = client.fetch_page(
            symbols=symbols,
            start=start,
            end=end,
            feed=feed,
            limit=limit,
            page_token=page_token,
        )
        for symbol, rows in page.items():
            if symbol not in requested_symbols:
                raise HistoricalBarError(
                    f"Alpaca returned an unrequested symbol: {symbol}"
                )
            bars.extend(normalize_bar(symbol, row) for row in rows)
        if not page_token:
            return bars, page_number
    raise HistoricalBarError(
        f"Historical bar pagination exceeded the configured {max_pages} page limit"
    )


def normalize_bar(symbol: str, raw: Mapping[str, Any]) -> HistoricalBar:
    try:
        bar = HistoricalBar(
            symbol=symbol,
            bar_start=datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00")),
            open=Decimal(str(raw["o"])),
            high=Decimal(str(raw["h"])),
            low=Decimal(str(raw["l"])),
            close=Decimal(str(raw["c"])),
            volume=int(raw["v"]),
            trade_count=int(raw["n"]),
            vwap=None if raw.get("vw") is None else Decimal(str(raw["vw"])),
        )
    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise HistoricalBarError("Alpaca bar had invalid required fields") from error
    if bar.bar_start.tzinfo is None:
        raise HistoricalBarError("Alpaca bar timestamp must include a timezone")
    if min(bar.open, bar.high, bar.low, bar.close) <= 0:
        raise HistoricalBarError("Alpaca bar prices must be positive")
    if bar.high < max(bar.open, bar.low, bar.close):
        raise HistoricalBarError("Alpaca bar high was inconsistent")
    if bar.low > min(bar.open, bar.high, bar.close):
        raise HistoricalBarError("Alpaca bar low was inconsistent")
    if bar.volume < 0 or bar.trade_count < 0:
        raise HistoricalBarError("Alpaca bar counts must be non-negative")
    return bar


def upsert_historical_bars(
    bars: Sequence[HistoricalBar],
    *,
    database_url: str,
    feed: str,
) -> int:
    rows = [
        (
            bar.symbol,
            bar.bar_start,
            "1m",
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.trade_count,
            bar.vwap,
            "alpaca",
            feed,
            True,
            "provider_aggregated_v1",
            -1,
        )
        for bar in bars
    ]
    if not rows:
        return 0
    with psycopg.connect(database_url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.executemany(UPSERT_MARKET_BAR_SQL, rows)
    return len(rows)


def _rfc3339(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Historical bar boundaries must include a timezone")
    return value.isoformat().replace("+00:00", "Z")
