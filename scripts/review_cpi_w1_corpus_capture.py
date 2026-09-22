#!/usr/bin/env python3
"""Review staged CPI corpus bytes before they become official golden-corpus candidates."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from src.cpi_w1_source import BlsCpiSourceContract


class CorpusReviewError(RuntimeError):
    pass


def _require_canonical_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CorpusReviewError(f"{label} must be canonical and non-empty")
    return value


def _parse_aware_timestamp(value: Any, label: str) -> datetime:
    text = _require_canonical_text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CorpusReviewError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CorpusReviewError(f"{label} must be timezone-aware")
    return parsed


def _entry_by_id(manifest: dict[str, Any], corpus_id: str) -> dict[str, Any]:
    matches = [
        entry
        for entry in manifest.get("entries", [])
        if entry.get("corpus_id") == corpus_id
    ]
    if len(matches) != 1:
        raise CorpusReviewError("corpus_id must resolve to exactly one manifest entry")
    return matches[0]


def review_capture(
    *,
    manifest: dict[str, Any],
    contract: BlsCpiSourceContract,
    corpus_id: str,
    data_path: Path,
    metadata_path: Path,
    reviewer_ref: str,
    verified_sha256: str,
    approved_root: Path,
    repo_root: Path,
) -> dict[str, Any]:
    reviewer_ref = _require_canonical_text(reviewer_ref, "reviewer_ref")
    if len(verified_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in verified_sha256):
        raise CorpusReviewError("verified_sha256 must be lowercase SHA-256")

    entry = _entry_by_id(manifest, corpus_id)
    if entry.get("materialization_status") != "REMOTE_ONLY":
        raise CorpusReviewError("only REMOTE_ONLY entries may be reviewed for materialization")
    if entry.get("expected_sha256") is not None or entry.get("local_path") is not None:
        raise CorpusReviewError("REMOTE_ONLY entry already carries pinned local material")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata.get("schema_version") != "cpi-w1-corpus-capture-v1":
        raise CorpusReviewError("unsupported capture sidecar schema")
    if metadata.get("promotion_status") != "STAGED_REVIEW_REQUIRED":
        raise CorpusReviewError("capture is not awaiting review")

    required_equal = {
        "corpus_id": entry["corpus_id"],
        "reference_month": entry["reference_month"],
        "artifact_contract_kind": entry["artifact_contract_kind"],
        "source_contract_version": entry["source_contract_version"],
        "requested_url": entry["official_locator"],
    }
    for key, expected in required_equal.items():
        if metadata.get(key) != expected:
            raise CorpusReviewError(f"capture sidecar {key} does not match manifest entry")

    if metadata.get("http_status") != 200:
        raise CorpusReviewError("only successful official captures may be reviewed")
    final_url = _require_canonical_text(metadata.get("final_url"), "final_url")
    contract.validate_url(final_url)
    _parse_aware_timestamp(metadata.get("captured_at"), "captured_at")

    body = data_path.read_bytes()
    actual_sha256 = hashlib.sha256(body).hexdigest()
    if metadata.get("byte_count") != len(body):
        raise CorpusReviewError("capture byte_count does not match staged bytes")
    if metadata.get("content_sha256") != actual_sha256:
        raise CorpusReviewError("capture sidecar hash does not match staged bytes")
    if verified_sha256 != actual_sha256:
        raise CorpusReviewError("reviewer-verified hash does not match staged bytes")

    repo_root = repo_root.resolve()
    allowed_root = (repo_root / "tests/fixtures/cpi_w1/official").resolve()
    approved_root = (
        approved_root
        if approved_root.is_absolute()
        else repo_root / approved_root
    ).resolve()
    if approved_root != allowed_root:
        raise CorpusReviewError(
            "approved_root must be the repository official corpus directory"
        )
    approved_root.mkdir(parents=True, exist_ok=True)
    if approved_root.is_symlink():
        raise CorpusReviewError("approved corpus root must not be a symlink")
    suffix = ".xlsx" if entry["artifact_contract_kind"].endswith("_XLSX") else ".html"
    stable_name = (
        f"{entry['reference_month']}-"
        f"{entry['artifact_contract_kind'].lower()}-"
        f"{actual_sha256[:16]}{suffix}"
    )
    approved_path = approved_root / stable_name
    if approved_path.is_symlink():
        raise CorpusReviewError("approved corpus file must not be a symlink")
    if approved_path.exists():
        if not approved_path.is_file():
            raise CorpusReviewError("approved corpus path must be a regular file")
        if hashlib.sha256(approved_path.read_bytes()).hexdigest() != actual_sha256:
            raise CorpusReviewError("approved corpus path already contains different bytes")
    else:
        shutil.copyfile(data_path, approved_path)

    reviewed_sidecar = approved_path.with_suffix(approved_path.suffix + ".review.json")
    reviewed_payload = {
        "schema_version": "cpi-w1-corpus-review-v1",
        "corpus_id": corpus_id,
        "reviewer_ref": reviewer_ref,
        "verified_sha256": actual_sha256,
        "capture_metadata": metadata,
        "status": "REVIEWED_BYTES_ONLY",
    }
    reviewed_sidecar.write_text(
        json.dumps(reviewed_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    candidate_entry = dict(entry)
    candidate_entry["materialization_status"] = "MATERIALIZED"
    candidate_entry["expected_sha256"] = actual_sha256
    candidate_entry["local_path"] = approved_path.relative_to(repo_root).as_posix()
    return {
        "status": "REVIEWED_BYTES_ONLY",
        "approved_path": str(approved_path),
        "review_sidecar": str(reviewed_sidecar),
        "candidate_entry": candidate_entry,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("tests/fixtures/cpi_w1/corpus.json"))
    parser.add_argument("--source-contract", type=Path, default=Path("config/cpi_w1_source_contract.json"))
    parser.add_argument("--corpus-id", required=True)
    parser.add_argument("--data-path", type=Path, required=True)
    parser.add_argument("--metadata-path", type=Path, required=True)
    parser.add_argument("--reviewer-ref", required=True)
    parser.add_argument("--verified-sha256", required=True)
    parser.add_argument("--approved-root", type=Path, default=Path("tests/fixtures/cpi_w1/official"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    contract = BlsCpiSourceContract.from_json(args.source_contract)
    repo_root = Path(__file__).resolve().parents[1]
    result = review_capture(
        manifest=manifest,
        contract=contract,
        corpus_id=args.corpus_id,
        data_path=args.data_path,
        metadata_path=args.metadata_path,
        reviewer_ref=args.reviewer_ref,
        verified_sha256=args.verified_sha256,
        approved_root=args.approved_root,
        repo_root=repo_root,
    )
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
