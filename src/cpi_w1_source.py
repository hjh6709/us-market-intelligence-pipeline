"""Bounded BLS CPI retrieval with no economic-semantic inference."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from urllib.parse import urljoin, urlsplit

import httpx


class SourceFailureKind(StrEnum):
    RETRYABLE = "RETRYABLE"
    NOT_FOUND = "NOT_FOUND"
    POLICY = "POLICY"
    OVERSIZED = "OVERSIZED"
    PROTOCOL = "PROTOCOL"


class SourceFetchError(RuntimeError):
    def __init__(
        self,
        kind: SourceFailureKind,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.status_code = status_code


@dataclass(frozen=True)
class SourceLocator:
    key: str
    url: str
    artifact_contract_kind: str
    surface_role: str
    max_bytes: int


@dataclass(frozen=True)
class CapturedResponse:
    locator_key: str
    requested_url: str
    final_url: str
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    captured_at: datetime


@dataclass(frozen=True)
class BlsCpiSourceContract:
    contract_version: str
    allowlisted_hosts: frozenset[str]
    max_redirects: int
    connect_timeout_seconds: float
    read_timeout_seconds: float
    accept_encoding: str
    max_response_bytes: int
    locators: Mapping[str, SourceLocator]

    @classmethod
    def from_json(cls, path: str | Path) -> "BlsCpiSourceContract":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        transport = raw["transport"]
        if not transport.get("https_only"):
            raise ValueError("CPI W1 source contract must require HTTPS")
        limits = raw["artifact_limits"]
        locators = {
            key: SourceLocator(
                key=key,
                url=value["url"],
                artifact_contract_kind=value["artifact_contract_kind"],
                surface_role=value["surface_role"],
                max_bytes=int(limits[value["artifact_contract_kind"]]),
            )
            for key, value in raw["locators"].items()
        }
        contract = cls(
            contract_version=raw["contract_version"],
            allowlisted_hosts=frozenset(transport["allowlisted_hosts"]),
            max_redirects=int(transport["max_redirects"]),
            connect_timeout_seconds=float(transport["connect_timeout_seconds"]),
            read_timeout_seconds=float(transport["read_timeout_seconds"]),
            accept_encoding=str(transport["accept_encoding"]),
            max_response_bytes=int(transport["max_response_bytes"]),
            locators=locators,
        )
        if contract.contract_version != "bls-cpi-source-v1":
            raise ValueError("unsupported CPI source contract version")
        for locator in contract.locators.values():
            contract.validate_url(locator.url)
        return contract

    def validate_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https":
            raise SourceFetchError(SourceFailureKind.POLICY, "source URL must use HTTPS")
        if parsed.username is not None or parsed.password is not None:
            raise SourceFetchError(
                SourceFailureKind.POLICY,
                "source URL credentials are forbidden",
            )
        if parsed.hostname not in self.allowlisted_hosts:
            raise SourceFetchError(
                SourceFailureKind.POLICY,
                "source host is not allowlisted",
            )
        if parsed.port not in (None, 443):
            raise SourceFetchError(
                SourceFailureKind.POLICY,
                "non-default HTTPS port is forbidden",
            )


class BlsCpiSourceClient:
    def __init__(
        self,
        contract: BlsCpiSourceContract,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.contract = contract
        self._owns_client = client is None
        self._client = client or httpx.Client(
            follow_redirects=False,
            timeout=httpx.Timeout(
                connect=contract.connect_timeout_seconds,
                read=contract.read_timeout_seconds,
                write=contract.read_timeout_seconds,
                pool=contract.connect_timeout_seconds,
            ),
            headers={
                "User-Agent": "us-market-intelligence-pipeline/cpi-w1",
                "Accept-Encoding": contract.accept_encoding,
            },
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "BlsCpiSourceClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _read_bounded(self, response: httpx.Response, max_bytes: int) -> bytes:
        encoding = response.headers.get("content-encoding", "").strip().lower()
        if encoding not in ("", "identity"):
            raise SourceFetchError(
                SourceFailureKind.PROTOCOL,
                "unexpected content encoding; CPI W1 requires identity transfer",
                status_code=response.status_code,
            )

        content_length = response.headers.get("content-length")
        if content_length:
            try:
                declared = int(content_length)
            except ValueError as exc:
                raise SourceFetchError(
                    SourceFailureKind.PROTOCOL,
                    "invalid Content-Length",
                    status_code=response.status_code,
                ) from exc
            if declared > min(max_bytes, self.contract.max_response_bytes):
                raise SourceFetchError(
                    SourceFailureKind.OVERSIZED,
                    "declared response exceeds byte limit",
                    status_code=response.status_code,
                )

        limit = min(max_bytes, self.contract.max_response_bytes)
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > limit:
                raise SourceFetchError(
                    SourceFailureKind.OVERSIZED,
                    "streamed response exceeds byte limit",
                    status_code=response.status_code,
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def fetch(self, locator: SourceLocator) -> CapturedResponse:
        requested_url = locator.url
        current_url = requested_url

        for redirect_count in range(self.contract.max_redirects + 1):
            self.contract.validate_url(current_url)
            try:
                with self._client.stream("GET", current_url) as response:
                    status = response.status_code

                    if status in (301, 302, 303, 307, 308):
                        if redirect_count >= self.contract.max_redirects:
                            raise SourceFetchError(
                                SourceFailureKind.PROTOCOL,
                                "redirect limit exceeded",
                                status_code=status,
                            )
                        location = response.headers.get("location")
                        if not location:
                            raise SourceFetchError(
                                SourceFailureKind.PROTOCOL,
                                "redirect response missing Location",
                                status_code=status,
                            )
                        next_url = urljoin(current_url, location)
                        self.contract.validate_url(next_url)
                        current_url = next_url
                        continue

                    if status == 429 or 500 <= status <= 599:
                        raise SourceFetchError(
                            SourceFailureKind.RETRYABLE,
                            "BLS source temporarily unavailable",
                            status_code=status,
                        )
                    if status == 404:
                        raise SourceFetchError(
                            SourceFailureKind.NOT_FOUND,
                            "BLS source returned 404",
                            status_code=status,
                        )
                    if status in (401, 403):
                        raise SourceFetchError(
                            SourceFailureKind.RETRYABLE,
                            "BLS source access denied or throttled",
                            status_code=status,
                        )
                    if status < 200 or status >= 300:
                        raise SourceFetchError(
                            SourceFailureKind.PROTOCOL,
                            f"unexpected BLS HTTP status {status}",
                            status_code=status,
                        )

                    body = self._read_bounded(response, locator.max_bytes)
                    return CapturedResponse(
                        locator_key=locator.key,
                        requested_url=requested_url,
                        final_url=current_url,
                        status_code=status,
                        headers=dict(response.headers),
                        body=body,
                        captured_at=datetime.now(timezone.utc),
                    )
            except SourceFetchError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                raise SourceFetchError(
                    SourceFailureKind.RETRYABLE,
                    "BLS source transport failure",
                ) from exc

        raise AssertionError("unreachable redirect loop")
