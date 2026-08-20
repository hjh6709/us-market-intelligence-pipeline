"""Small FRED HTTP client with point-in-time windows and sanitized errors."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


FRED_BASE_URL = "https://api.stlouisfed.org/fred"


class FredError(RuntimeError):
    """Base class for sanitized FRED failures."""


class FredRateLimitError(FredError):
    """FRED rejected the request because its rate limit was exceeded."""


class FredTimeoutError(FredError):
    """The FRED request did not complete before its timeout."""


class FredTransportError(FredError):
    """FRED or the network failed in a retryable way."""


class FredContractError(FredError):
    """The request or FRED response did not match the expected contract."""


@dataclass(frozen=True, slots=True)
class FredWindow:
    realtime_start: date
    realtime_end: date
    observation_start: date
    observation_end: date

    def __post_init__(self) -> None:
        fields = (
            self.realtime_start,
            self.realtime_end,
            self.observation_start,
            self.observation_end,
        )
        if not all(isinstance(value, date) for value in fields):
            raise ValueError("FRED window bounds must be dates")
        if self.realtime_start > self.realtime_end:
            raise ValueError("realtime_start must not be after realtime_end")
        if self.observation_start > self.observation_end:
            raise ValueError("observation_start must not be after observation_end")


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
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    def fetch_series(self, series_id: str, *, as_of: date) -> dict[str, Any]:
        rows = self._request_collection(
            "/series",
            {
                "series_id": series_id,
                "realtime_start": as_of.isoformat(),
                "realtime_end": as_of.isoformat(),
            },
            collection="seriess",
        )
        if len(rows) != 1:
            raise FredContractError("FRED series response must contain exactly one row")
        return rows[0]

    def fetch_observations(
        self,
        series_id: str,
        window: FredWindow,
    ) -> list[dict[str, Any]]:
        return self._request_collection(
            "/series/observations",
            {
                "series_id": series_id,
                "realtime_start": window.realtime_start.isoformat(),
                "realtime_end": window.realtime_end.isoformat(),
                "observation_start": window.observation_start.isoformat(),
                "observation_end": window.observation_end.isoformat(),
                "output_type": "1",
            },
            collection="observations",
        )

    def _request_collection(
        self,
        path: str,
        params: Mapping[str, str],
        *,
        collection: str,
    ) -> list[dict[str, Any]]:
        query = {**params, "api_key": self._api_key, "file_type": "json"}
        request = Request(
            f"{FRED_BASE_URL}{path}?{urlencode(query)}",
            headers={"Accept": "application/json"},
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read())
        except HTTPError as error:
            if error.code == 429:
                raise FredRateLimitError("FRED rate limit exceeded") from error
            if 500 <= error.code < 600:
                raise FredTransportError(
                    f"FRED service failed with HTTP {error.code}"
                ) from error
            raise FredContractError(
                f"FRED request was rejected with HTTP {error.code}"
            ) from error
        except TimeoutError as error:
            raise FredTimeoutError("FRED request timed out") from error
        except URLError as error:
            if isinstance(error.reason, TimeoutError):
                raise FredTimeoutError("FRED request timed out") from error
            raise FredTransportError("FRED request could not be completed") from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise FredContractError("FRED response was not valid JSON") from error

        if not isinstance(payload, dict):
            raise FredContractError("FRED response must be a JSON object")
        rows = payload.get(collection)
        if not isinstance(rows, list) or not all(
            isinstance(row, dict) for row in rows
        ):
            raise FredContractError(
                f"FRED response did not contain a valid {collection} collection"
            )
        return [dict(row) for row in rows]
