#!/usr/bin/env python3
"""Stage exact official CPI corpus bytes without promoting them to golden truth."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from src.cpi_w1_source import (
    BlsCpiSourceClient,
    BlsCpiSourceContract,
    CapturedResponse,
    SourceLocator,
)


_SAFE_ID = re.compile(r"^[A-Za-z0-9:._-]+$")


class CorpusMaterializationError(RuntimeError):
    pass


def _max_bytes_for_kind(
    contract: BlsCpiSourceContract,
    artifact_contract_kind: str,
) -> int:
    values = {
        locator.max_bytes
        for locator in contract.locators.values()
        if locator.artifact_contract_kind == artifact_contract_kind
    }
    values.update(
        item.max_bytes
        for item in contract.dynamic_artifact_contracts.values()
        if item.artifact_contract_kind == artifact_contract_kind
    )
    if len(values) != 1:
        raise CorpusMaterializationError(
            "artifact kind must map to one byte limit"
        )
    return next(iter(values))


def locator_for_entry(
    contract: BlsCpiSourceContract,
    entry: dict[str, Any],
) -> SourceLocator:
    corpus_id = entry.get("corpus_id")
    if not isinstance(corpus_id, str) or _SAFE_ID.fullmatch(corpus_id) is None:
        raise CorpusMaterializationError("corpus_id is not canonical")
    if entry.get("source_contract_version") != contract.contract_version:
        raise CorpusMaterializationError("entry source contract mismatch")
    kind = entry.get("artifact_contract_kind")
    url = entry.get("official_locator")
    if not isinstance(kind, str) or not isinstance(url, str):
        raise CorpusMaterializationError("entry kind and official locator are required")
    contract.validate_url(url)
    return SourceLocator(
        key=corpus_id,
        url=url,
        artifact_contract_kind=kind,
        surface_role=contract.surface_role_for_artifact_kind(kind),
        max_bytes=_max_bytes_for_kind(contract, kind),
        capture_chronology_allowed=(
            contract.capture_chronology_allowed_for_artifact_kind(kind)
        ),
    )


def _staging_stem(entry: dict[str, Any]) -> str:
    corpus_id = entry["corpus_id"]
    digest = hashlib.sha256(corpus_id.encode("utf-8")).hexdigest()[:12]
    month = entry["reference_month"]
    kind = entry["artifact_contract_kind"].lower()
    return f"{month}-{kind}-{digest}"


def materialize_entry(
    *,
    entry: dict[str, Any],
    contract: BlsCpiSourceContract,
    client: BlsCpiSourceClient,
    staging_dir: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    if entry.get("materialization_status") != "REMOTE_ONLY":
        raise CorpusMaterializationError(
            "only REMOTE_ONLY official inventory entries may be staged"
        )
    locator = locator_for_entry(contract, entry)
    response = client.fetch(locator)

    staging_dir.mkdir(parents=True, exist_ok=True)
    stem = _staging_stem(entry)
    extension = (
        ".xlsx"
        if locator.artifact_contract_kind.endswith("_XLSX")
        else ".html"
    )
    data_path = staging_dir / f"{stem}{extension}"
    metadata_path = staging_dir / f"{stem}.json"

    if not overwrite and (data_path.exists() or metadata_path.exists()):
        raise CorpusMaterializationError(
            "staging output already exists; explicit --overwrite required"
        )

    content_sha256 = hashlib.sha256(response.body).hexdigest()
    content_type = response.headers.get("content-type")
    metadata = {
        "schema_version": "cpi-w1-corpus-capture-v1",
        "corpus_id": entry["corpus_id"],
        "reference_month": entry["reference_month"],
        "artifact_contract_kind": entry["artifact_contract_kind"],
        "source_contract_version": contract.contract_version,
        "requested_url": response.requested_url,
        "final_url": response.final_url,
        "http_status": response.status_code,
        "content_type": content_type,
        "content_sha256": content_sha256,
        "byte_count": len(response.body),
        "captured_at": response.captured_at.isoformat(),
        "promotion_status": "STAGED_REVIEW_REQUIRED",
    }

    data_path.write_bytes(response.body)
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "data_path": str(data_path),
        "metadata_path": str(metadata_path),
        "metadata": metadata,
    }


def load_entries(manifest_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise CorpusMaterializationError("manifest entries must be a list")
    return entries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("tests/fixtures/cpi_w1/corpus.json"),
    )
    parser.add_argument(
        "--source-contract",
        type=Path,
        default=Path("config/cpi_w1_source_contract.json"),
    )
    parser.add_argument("--staging-dir", type=Path, required=True)
    parser.add_argument("--corpus-id", action="append", default=[])
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if args.all == bool(args.corpus_id):
        parser.error("choose exactly one of --all or one/more --corpus-id")

    entries = load_entries(args.manifest)
    selected_ids = set(args.corpus_id)
    selected = (
        [entry for entry in entries if entry.get("replay_required")]
        if args.all
        else [entry for entry in entries if entry.get("corpus_id") in selected_ids]
    )
    if not args.all and {entry["corpus_id"] for entry in selected} != selected_ids:
        missing = sorted(selected_ids - {entry["corpus_id"] for entry in selected})
        raise CorpusMaterializationError(f"unknown corpus ids: {missing}")

    contract = BlsCpiSourceContract.from_json(args.source_contract)
    results: list[dict[str, Any]] = []
    with BlsCpiSourceClient(contract) as client:
        for entry in selected:
            results.append(
                materialize_entry(
                    entry=entry,
                    contract=contract,
                    client=client,
                    staging_dir=args.staging_dir,
                    overwrite=args.overwrite,
                )
            )

    print(
        json.dumps(
            {
                "status": "STAGED_REVIEW_REQUIRED",
                "count": len(results),
                "captures": [item["metadata"] for item in results],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
