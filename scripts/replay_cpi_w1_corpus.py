#!/usr/bin/env python3
"""Deterministic CPI W1 corpus inventory validation and semantic replay."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from src.cpi_w1_release import extract_core4_from_release_html
from src.cpi_w1_schedule import parse_bls_revised_release_dates_html


_SCHEMA_VERSION = "cpi-w1-corpus-v1"
_RELEASE_EXTRACTOR = "bls-cpi-release-html-v1"
_CANCELLATION_EXTRACTOR = "bls-cpi-revised-release-dates-v1"
_ALLOWED_MATERIALIZATION = {"MATERIALIZED", "REMOTE_ONLY"}
_PASS_SEMANTIC = {"SEMANTIC_UNCHANGED", "EXPECTED_CHANGED"}


class CorpusValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ReplayEntryResult:
    corpus_id: str
    inventory_status: str
    semantic_status: str
    detail: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "corpus_id": self.corpus_id,
            "inventory_status": self.inventory_status,
            "semantic_status": self.semantic_status,
            "detail": self.detail,
        }


def _months(start: str, end: str) -> list[str]:
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    result: list[str] = []
    y, m = sy, sm
    while y < ey or (y == ey and m <= em):
        result.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y += 1
            m = 1
    return result


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise CorpusValidationError("unsupported corpus schema_version")
    return payload


def validate_manifest(manifest: dict[str, Any], repo_root: Path) -> None:
    baseline = manifest.get("baseline") or {}
    start = baseline.get("start_reference_month")
    end = baseline.get("end_reference_month")
    if not isinstance(start, str) or not isinstance(end, str):
        raise CorpusValidationError("baseline start/end reference month required")

    expected_months = _months(start, end)
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise CorpusValidationError("entries must be a list")

    seen_ids: set[str] = set()
    seen_months: set[str] = set()
    for entry in entries:
        corpus_id = entry.get("corpus_id")
        month = entry.get("reference_month")
        if not isinstance(corpus_id, str) or not corpus_id:
            raise CorpusValidationError("corpus_id required")
        if corpus_id in seen_ids:
            raise CorpusValidationError(f"duplicate corpus_id: {corpus_id}")
        seen_ids.add(corpus_id)
        if not isinstance(month, str):
            raise CorpusValidationError(f"{corpus_id}: reference_month required")
        if month in seen_months:
            raise CorpusValidationError(f"duplicate baseline reference_month: {month}")
        seen_months.add(month)

        locator = entry.get("official_locator")
        if not isinstance(locator, str) or not locator.startswith("https://www.bls.gov/"):
            raise CorpusValidationError(f"{corpus_id}: official BLS HTTPS locator required")
        if entry.get("source_contract_version") != "bls-cpi-source-v1":
            raise CorpusValidationError(f"{corpus_id}: unexpected source contract")

        status = entry.get("materialization_status")
        if status not in _ALLOWED_MATERIALIZATION:
            raise CorpusValidationError(f"{corpus_id}: invalid materialization_status")

        local_path = entry.get("local_path")
        expected_sha256 = entry.get("expected_sha256")
        if status == "MATERIALIZED":
            if not isinstance(local_path, str) or not local_path:
                raise CorpusValidationError(f"{corpus_id}: MATERIALIZED requires local_path")
            if (
                not isinstance(expected_sha256, str)
                or len(expected_sha256) != 64
                or any(c not in "0123456789abcdef" for c in expected_sha256)
            ):
                raise CorpusValidationError(
                    f"{corpus_id}: MATERIALIZED requires lowercase SHA-256"
                )
            full_path = repo_root / local_path
            if not full_path.is_file():
                raise CorpusValidationError(f"{corpus_id}: local fixture missing")
            actual = hashlib.sha256(full_path.read_bytes()).hexdigest()
            if actual != expected_sha256:
                raise CorpusValidationError(f"{corpus_id}: fixture SHA-256 mismatch")
        else:
            if local_path is not None or expected_sha256 is not None:
                raise CorpusValidationError(
                    f"{corpus_id}: REMOTE_ONLY must not pretend bytes are pinned"
                )

    if set(expected_months) != seen_months:
        missing = sorted(set(expected_months) - seen_months)
        extra = sorted(seen_months - set(expected_months))
        raise CorpusValidationError(
            f"baseline month coverage mismatch missing={missing} extra={extra}"
        )


def validate_conformance_fixtures(
    manifest: dict[str, Any],
    repo_root: Path,
) -> None:
    fixtures = manifest.get("conformance_fixtures", [])
    if not isinstance(fixtures, list):
        raise CorpusValidationError("conformance_fixtures must be a list")
    seen: set[str] = set()
    fixtures_root = (repo_root / "tests/fixtures/cpi_w1").resolve()
    for fixture in fixtures:
        fixture_id = fixture.get("fixture_id")
        if not isinstance(fixture_id, str) or not fixture_id:
            raise CorpusValidationError("conformance fixture_id required")
        if fixture_id in seen:
            raise CorpusValidationError(f"duplicate conformance fixture_id: {fixture_id}")
        seen.add(fixture_id)
        if fixture.get("fixture_kind") != "SYNTHETIC_CONFORMANCE":
            raise CorpusValidationError(
                f"{fixture_id}: fixture_kind must be SYNTHETIC_CONFORMANCE"
            )
        local_path = fixture.get("local_path")
        expected_sha256 = fixture.get("expected_sha256")
        if not isinstance(local_path, str) or not local_path:
            raise CorpusValidationError(f"{fixture_id}: local_path required")
        if (
            not isinstance(expected_sha256, str)
            or len(expected_sha256) != 64
            or any(c not in "0123456789abcdef" for c in expected_sha256)
        ):
            raise CorpusValidationError(f"{fixture_id}: lowercase SHA-256 required")
        full_path = (repo_root / local_path).resolve()
        if fixtures_root not in full_path.parents:
            raise CorpusValidationError(
                f"{fixture_id}: conformance path escapes CPI fixture root"
            )
        if not full_path.is_file():
            raise CorpusValidationError(f"{fixture_id}: fixture missing")
        if hashlib.sha256(full_path.read_bytes()).hexdigest() != expected_sha256:
            raise CorpusValidationError(f"{fixture_id}: fixture SHA-256 mismatch")


def replay_conformance_fixture(
    fixture: dict[str, Any],
    repo_root: Path,
) -> ReplayEntryResult:
    extractor = fixture.get("extractor_contract_version")
    if extractor != _RELEASE_EXTRACTOR:
        return ReplayEntryResult(
            fixture["fixture_id"],
            "SYNTHETIC_CONFORMANCE",
            "NOT_RUN",
            f"extractor not implemented by replay tool: {extractor}",
        )
    expected = fixture.get("expected_semantics") or {}
    try:
        actual = _actual_core4(
            repo_root / fixture["local_path"],
            fixture["reference_month"],
        )
    except Exception as exc:
        return ReplayEntryResult(
            fixture["fixture_id"],
            "SYNTHETIC_CONFORMANCE",
            "NEWLY_FAILED",
            f"{type(exc).__name__}: {exc}",
        )
    if expected.get("kind") == "CORE4" and actual == expected.get("values"):
        return ReplayEntryResult(
            fixture["fixture_id"],
            "SYNTHETIC_CONFORMANCE",
            "SEMANTIC_UNCHANGED",
        )
    return ReplayEntryResult(
        fixture["fixture_id"],
        "SYNTHETIC_CONFORMANCE",
        "UNEXPECTED_CHANGED",
        json.dumps(
            {"expected": expected.get("values"), "actual": actual},
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def inventory_status(entry: dict[str, Any]) -> str:
    if entry["materialization_status"] != "MATERIALIZED":
        return "REMOTE_ONLY"
    if not entry.get("extractor_contract_version"):
        return "BLOCKED_NO_EXTRACTOR"
    return "MATERIALIZED_PINNED"


def _actual_core4(path: Path, reference_month: str) -> dict[str, str]:
    year, month = map(int, reference_month.split("-"))
    bundle = extract_core4_from_release_html(
        path.read_bytes(),
        expected_reference_month=date(year, month, 1),
    )
    actual: dict[str, str] = {}
    for item in bundle.observations:
        if item.normalized_value is None:
            actual[item.observation_code] = item.assertion_state.value
        else:
            actual[item.observation_code] = format(
                item.normalized_value.normalize(),
                "f",
            )
    return actual


def replay_entry(entry: dict[str, Any], repo_root: Path) -> ReplayEntryResult:
    status = inventory_status(entry)
    if status != "MATERIALIZED_PINNED":
        return ReplayEntryResult(entry["corpus_id"], status, "NOT_RUN")

    extractor = entry["extractor_contract_version"]
    expected = entry.get("expected_semantics") or {}
    if extractor == _CANCELLATION_EXTRACTOR:
        year, month = map(int, entry["reference_month"].split("-"))
        try:
            candidate = parse_bls_revised_release_dates_html(
                (repo_root / entry["local_path"]).read_bytes(),
                expected_reference_month=date(year, month, 1),
                extractor_contract_version=extractor,
            )
        except Exception as exc:
            return ReplayEntryResult(
                entry["corpus_id"],
                status,
                "NEWLY_FAILED",
                f"{type(exc).__name__}: {exc}",
            )
        if (
            expected.get("kind") == "CANCELLATION"
            and candidate.schedule_status.value == expected.get("schedule_status")
        ):
            return ReplayEntryResult(
                entry["corpus_id"],
                status,
                "SEMANTIC_UNCHANGED",
            )
        return ReplayEntryResult(
            entry["corpus_id"],
            status,
            "UNEXPECTED_CHANGED",
            "cancellation semantics differ from pinned expectation",
        )

    if extractor != _RELEASE_EXTRACTOR:
        return ReplayEntryResult(
            entry["corpus_id"],
            "BLOCKED_NO_EXTRACTOR",
            "NOT_RUN",
            f"extractor not implemented by replay tool: {extractor}",
        )
    if expected.get("kind") != "CORE4":
        return ReplayEntryResult(
            entry["corpus_id"],
            status,
            "NEWLY_ACCEPTED",
            "materialized release has no pinned CORE4 expectation",
        )

    try:
        actual = _actual_core4(
            repo_root / entry["local_path"],
            entry["reference_month"],
        )
    except Exception as exc:
        return ReplayEntryResult(
            entry["corpus_id"],
            status,
            "NEWLY_FAILED",
            f"{type(exc).__name__}: {exc}",
        )

    expected_values = expected.get("values")
    if actual == expected_values:
        return ReplayEntryResult(
            entry["corpus_id"],
            status,
            "SEMANTIC_UNCHANGED",
        )
    return ReplayEntryResult(
        entry["corpus_id"],
        status,
        "UNEXPECTED_CHANGED",
        json.dumps(
            {"expected": expected_values, "actual": actual},
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def build_report(
    manifest: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    validate_manifest(manifest, repo_root)
    validate_conformance_fixtures(manifest, repo_root)
    results = [replay_entry(entry, repo_root) for entry in manifest["entries"]]
    conformance_results = [
        replay_conformance_fixture(fixture, repo_root)
        for fixture in manifest.get("conformance_fixtures", [])
    ]
    official_corpus_ready = all(
        (not entry.get("replay_required", False))
        or (
            result.inventory_status == "MATERIALIZED_PINNED"
            and result.semantic_status in _PASS_SEMANTIC
        )
        for entry, result in zip(manifest["entries"], results, strict=True)
    )
    conformance_ready = all(
        result.semantic_status in _PASS_SEMANTIC
        for result in conformance_results
    )
    release_gate_ready = official_corpus_ready and conformance_ready
    return {
        "schema_version": _SCHEMA_VERSION,
        "baseline": manifest["baseline"],
        "official_corpus_ready": official_corpus_ready,
        "conformance_ready": conformance_ready,
        "release_gate_ready": release_gate_ready,
        "counts": {
            "entries": len(results),
            "materialized_pinned": sum(
                result.inventory_status == "MATERIALIZED_PINNED"
                for result in results
            ),
            "remote_only": sum(
                result.inventory_status == "REMOTE_ONLY"
                for result in results
            ),
            "blocked_no_extractor": sum(
                result.inventory_status == "BLOCKED_NO_EXTRACTOR"
                for result in results
            ),
            "synthetic_conformance": len(conformance_results),
        },
        "results": [result.as_dict() for result in results],
        "conformance_results": [
            result.as_dict() for result in conformance_results
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("tests/fixtures/cpi_w1/corpus.json"),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--inventory-only",
        action="store_true",
        help="report incomplete materialization without enforcing release readiness",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    manifest = load_manifest(args.manifest)
    report = build_report(manifest, repo_root=repo_root)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if args.inventory_only:
        return 0
    return 0 if report["release_gate_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
