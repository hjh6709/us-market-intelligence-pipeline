"""Validated market-symbol universe used by multi-event collection jobs."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


DEFAULT_UNIVERSE_PATH = Path("config/market_universe.json")
SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]*")


@dataclass(frozen=True)
class MarketInstrument:
    symbol: str
    asset_type: str
    role: str
    reason: str


def load_market_universe(path: Path = DEFAULT_UNIVERSE_PATH) -> list[MarketInstrument]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("instruments"), list):
        raise ValueError("market universe must use schema_version 1 and contain instruments")

    instruments: list[MarketInstrument] = []
    for raw in payload["instruments"]:
        if not isinstance(raw, Mapping):
            raise ValueError("market universe entry must be an object")
        instrument = MarketInstrument(
            symbol=str(raw["symbol"]),
            asset_type=str(raw["asset_type"]),
            role=str(raw["role"]),
            reason=str(raw["reason"]),
        )
        if not SYMBOL_PATTERN.fullmatch(instrument.symbol):
            raise ValueError(f"invalid market symbol: {instrument.symbol}")
        if not all((instrument.asset_type, instrument.role, instrument.reason)):
            raise ValueError(f"market universe metadata is incomplete: {instrument.symbol}")
        instruments.append(instrument)

    symbols = [item.symbol for item in instruments]
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("market universe must contain unique symbols")
    return instruments
