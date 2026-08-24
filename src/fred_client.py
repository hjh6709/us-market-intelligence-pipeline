"""Small FRED/ALFRED client that preserves point-in-time observation fields."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


FRED_OBSERVATIONS_URL = (
    "https://api.stlouisfed.org/fred/series/observations"
)


class FredDataError(RuntimeError):
    """Raised when a FRED request or response violates the ingestion contract."""


@dataclass(frozen=True)
class MacroObservation:
    series_id: str
    observation_date: date
    realtime_start: date
    realtime_end: date
    value: Decimal | None

    def is_valid_on(self, as_of: date) -> bool:
        """Return whether this was the provider's valid vintage on the date."""
        return self.realtime_start <= as_of <= self.realtime_end


class FredClient:
    def __init__(
        self,
        api_key: str,
        *,
        timeout_seconds: float = 15.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if not api_key.strip():
            raise ValueError("FRED API key is required")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    def fetch_observations(
        self,
        *,
        series_id: str,
        observation_start: date,
        observation_end: date,
        vintage_dates: Sequence[date] | None = None,
        output_type: int = 1,
    ) -> list[MacroObservation]:
        """Fetch observations and retain ALFRED realtime/vintage boundaries."""
        if observation_end < observation_start:
            raise ValueError("observation_end must be on or after observation_start")
        if output_type not in (1, 2, 3, 4):
            raise ValueError("output_type must be between 1 and 4")

        params = {
            "series_id": series_id,
            "api_key": self._api_key,
            "file_type": "json",
            "observation_start": observation_start.isoformat(),
            "observation_end": observation_end.isoformat(),
            "output_type": str(output_type),
        }
        if vintage_dates:
            params["vintage_dates"] = ",".join(
                vintage_date.isoformat() for vintage_date in vintage_dates
            )

        request = Request(
            f"{FRED_OBSERVATIONS_URL}?{urlencode(params)}",
            headers={"Accept": "application/json"},
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read())
        except HTTPError as error:
            raise FredDataError(
                f"FRED observations request failed with HTTP {error.code}"
            ) from error
        except (TimeoutError, URLError) as error:
            raise FredDataError(
                "FRED observations request could not be completed"
            ) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise FredDataError(
                "FRED observations response was not valid JSON"
            ) from error

        return normalize_observations(payload, series_id=series_id)


def normalize_observations(
    payload: Mapping[str, Any],
    *,
    series_id: str,
) -> list[MacroObservation]:
    """Normalize FRED strings without discarding missing or revision metadata."""
    raw_observations = payload.get("observations")
    if not isinstance(raw_observations, list):
        raise FredDataError("FRED response did not contain an observations list")

    observations = []
    for raw in raw_observations:
        if not isinstance(raw, Mapping):
            raise FredDataError("FRED observation was not an object")
        try:
            value_text = raw["value"]
            value = None if value_text == "." else Decimal(str(value_text))
            observation = MacroObservation(
                series_id=series_id,
                observation_date=date.fromisoformat(str(raw["date"])),
                realtime_start=date.fromisoformat(str(raw["realtime_start"])),
                realtime_end=date.fromisoformat(str(raw["realtime_end"])),
                value=value,
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as error:
            raise FredDataError("FRED observation had invalid required fields") from error
        if observation.realtime_end < observation.realtime_start:
            raise FredDataError("FRED observation had an invalid realtime period")
        observations.append(observation)
    return observations


def observations_as_of(
    observations: Sequence[MacroObservation],
    as_of: date,
) -> list[MacroObservation]:
    """Select the vintages valid on a historical date and exclude future revisions."""
    return [observation for observation in observations if observation.is_valid_on(as_of)]
