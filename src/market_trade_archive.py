"""Collect actual Alpaca trades once and archive them as verified Parquet."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from src.historical_market_replay import normalize_historical_trade


@dataclass(frozen=True)
class ArchivePartition:
    event_type: str
    release_date: str
    symbol: str
    start: str
    end: str
    feed: str = "sip"

    def directory(self, archive_root: Path) -> Path:
        return (
            archive_root
            / f"event_type={self.event_type}"
            / f"release_date={self.release_date}"
            / f"symbol={self.symbol}"
        )


@dataclass(frozen=True)
class ArchiveManifest:
    partition: ArchivePartition
    parquet_path: Path
    manifest_path: Path
    row_count: int
    page_count: int
    sha256: str
    collected_at: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "partition": asdict(self.partition),
            "parquet_file": self.parquet_path.name,
            "row_count": self.row_count,
            "page_count": self.page_count,
            "sha256": self.sha256,
            "collected_at": self.collected_at,
        }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_completed(partition: ArchivePartition, archive_root: Path) -> ArchiveManifest | None:
    directory = partition.directory(archive_root)
    manifest_path = directory / "manifest.json"
    parquet_path = directory / "trades.parquet"
    if not manifest_path.exists() or not parquet_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if payload["partition"] != asdict(partition):
            return None
        if payload["sha256"] != _sha256(parquet_path):
            return None
        if pq.ParquetFile(parquet_path).metadata.num_rows != payload["row_count"]:
            return None
        return ArchiveManifest(
            partition=partition,
            parquet_path=parquet_path,
            manifest_path=manifest_path,
            row_count=payload["row_count"],
            page_count=payload["page_count"],
            sha256=payload["sha256"],
            collected_at=payload["collected_at"],
        )
    except (KeyError, OSError, ValueError, json.JSONDecodeError):
        return None


def collect_archive_partition(
    client: Any,
    partition: ArchivePartition,
    *,
    archive_root: Path = Path("data/archive"),
    limit: int = 10_000,
    max_pages: int = 10_000,
) -> ArchiveManifest:
    """Fetch one bounded event window page by page and publish it atomically."""
    completed = _load_completed(partition, archive_root)
    if completed is not None:
        return completed
    if limit < 1 or max_pages < 1:
        raise ValueError("limit and max_pages must be positive")

    directory = partition.directory(archive_root)
    directory.mkdir(parents=True, exist_ok=True)
    parquet_path = directory / "trades.parquet"
    manifest_path = directory / "manifest.json"
    temporary_parquet = directory / ".trades.parquet.tmp"
    temporary_manifest = directory / ".manifest.json.tmp"
    schema = pa.schema([("payload_json", pa.string())])
    writer: pq.ParquetWriter | None = None
    row_count = 0
    page_count = 0
    page_token: str | None = None

    try:
        writer = pq.ParquetWriter(temporary_parquet, schema, compression="zstd")
        for page_number in range(1, max_pages + 1):
            trades, next_page_token = client.fetch_page(
                symbol=partition.symbol,
                start=partition.start,
                end=partition.end,
                feed=partition.feed,
                limit=limit,
                page_token=page_token,
            )
            encoded = [
                json.dumps(
                    normalize_historical_trade(partition.symbol, item),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                for item in trades
            ]
            if encoded:
                writer.write_table(pa.table({"payload_json": encoded}, schema=schema))
            row_count += len(encoded)
            page_count = page_number
            page_token = next_page_token
            if not page_token:
                break
        else:
            raise RuntimeError(f"pagination exceeded the configured {max_pages} page limit")

        writer.close()
        writer = None
        with temporary_parquet.open("rb") as stream:
            os.fsync(stream.fileno())
        temporary_parquet.replace(parquet_path)
        digest = _sha256(parquet_path)
        manifest = ArchiveManifest(
            partition=partition,
            parquet_path=parquet_path,
            manifest_path=manifest_path,
            row_count=row_count,
            page_count=page_count,
            sha256=digest,
            collected_at=datetime.now(UTC).isoformat(),
        )
        temporary_manifest.write_text(
            json.dumps(manifest.to_payload(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        with temporary_manifest.open("rb") as stream:
            os.fsync(stream.fileno())
        temporary_manifest.replace(manifest_path)
        return manifest
    except Exception:
        if writer is not None:
            writer.close()
        temporary_parquet.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
        parquet_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        raise


def read_archive_records(manifest: ArchiveManifest) -> Iterator[dict[str, Any]]:
    if _sha256(manifest.parquet_path) != manifest.sha256:
        raise RuntimeError("archive checksum does not match its manifest")
    parquet = pq.ParquetFile(manifest.parquet_path)
    for batch in parquet.iter_batches(columns=["payload_json"], batch_size=10_000):
        for payload_json in batch.column(0).to_pylist():
            yield json.loads(payload_json)
