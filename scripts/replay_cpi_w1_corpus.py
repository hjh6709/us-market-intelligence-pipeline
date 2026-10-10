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

from src.cpi_w1_promotion_capabilities import (
    PromotionCapability,
    PromotionCapabilityRegistry,
)
from src.cpi_w1_release import (
    extract_core4_from_release_html,
    extract_release_envelope,
)
from src.cpi_w1_schedule import parse_bls_revised_release_dates_html


_SCHEMA_VERSION = "cpi-w1-corpus-v2"
_ALLOWED_MATERIALIZATION = {"MATERIALIZED", "REMOTE_ONLY"}
_PASS_SEMANTIC = {"SEMANTIC_UNCHANGED", "EXPECTED_CHANGED"}
_EXPECTED_DIFF_SCHEMA = "cpi-w1-expected-diffs-v2"
_REGISTRY_PATH = Path("config/cpi_w1_promotion_capabilities.json")
_EXPECTATION_KEYS = frozenset(
    {"promotion_capability_id", "replay_required", "expected_semantics"}
)


class CorpusValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ReplayEntryResult:
    corpus_id: str
    promotion_capability_id: str
    release_subject_digest: str
    extractor_contract_version: str
    artifact_sha256: str | None
    expected_semantics_digest: str
    actual_semantics_digest: str | None
    inventory_status: str
    semantic_status: str
    detail: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "corpus_id": self.corpus_id,
            "promotion_capability_id": self.promotion_capability_id,
            "release_subject_digest": self.release_subject_digest,
            "extractor_contract_version": self.extractor_contract_version,
            "artifact_sha256": self.artifact_sha256,
            "expected_semantics_digest": self.expected_semantics_digest,
            "actual_semantics_digest": self.actual_semantics_digest,
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


def _semantic_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_set_digest(values: list[dict[str, Any]]) -> str:
    canonical_items = sorted(
        (
            json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            for item in values
        )
    )
    return _semantic_digest(canonical_items)


def expected_diff_approvals_digest(approvals: list[dict[str, Any]]) -> str:
    return _canonical_set_digest(approvals)


def load_expected_diffs(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != _EXPECTED_DIFF_SCHEMA:
        raise CorpusValidationError("unsupported expected-diff schema_version")
    approvals = payload.get("approvals")
    if not isinstance(approvals, list):
        raise CorpusValidationError("expected-diff approvals must be a list")
    return approvals


def _approved_semantic_change(
    *,
    approvals: list[dict[str, Any]],
    entry: dict[str, Any],
    corpus_id: str | None = None,
    promotion_capability_id: str,
    release_subject_digest: str,
    extractor_contract_version: str,
    expected: Any,
    actual: Any,
    repo_root: Path | None = None,
) -> bool:
    corpus_id = corpus_id or entry.get("corpus_id") or entry.get("fixture_id")
    if not isinstance(corpus_id, str) or not corpus_id:
        raise CorpusValidationError("expected-diff subject id is missing")
    required = {
        "corpus_id": corpus_id,
        "artifact_sha256": entry.get("expected_sha256"),
        "promotion_capability_id": promotion_capability_id,
        "release_subject_digest": release_subject_digest,
        "extractor_contract_version": extractor_contract_version,
        "expected_semantics_sha256": _semantic_digest(expected),
        "actual_semantics_sha256": _semantic_digest(actual),
    }
    matches = []
    for approval in approvals:
        if all(approval.get(key) == value for key, value in required.items()):
            reason = approval.get("reason_code")
            review_ref = approval.get("review_ref")
            if (
                isinstance(reason, str)
                and reason
                and reason == reason.strip()
                and isinstance(review_ref, str)
                and review_ref
                and review_ref == review_ref.strip()
            ):
                from src.cpi_w1_review_artifact import load_review_artifact
                try:
                    review = load_review_artifact(
                        repo_root or Path(__file__).resolve().parents[1],
                        review_ref, approval.get("review_digest"),
                    )
                    if review.payload()["purpose"] != "EXPECTED_DIFF":
                        continue
                    review.require_bindings({key: value for key, value in required.items() if key != "corpus_id"})
                except (ValueError, OSError):
                    continue
                matches.append(approval)
    if len(matches) > 1:
        raise CorpusValidationError(
            f"duplicate expected-diff approvals for {corpus_id}"
        )
    return len(matches) == 1


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise CorpusValidationError("unsupported corpus schema_version")
    return payload


def _registry(repo_root: Path) -> PromotionCapabilityRegistry:
    return PromotionCapabilityRegistry.from_json(repo_root / _REGISTRY_PATH)


def _capability_expectations(
    item: dict[str, Any],
    registry: PromotionCapabilityRegistry,
    *,
    item_id: str,
) -> tuple[tuple[PromotionCapability, dict[str, Any]], ...]:
    expectations = item.get("capability_expectations")
    if not isinstance(expectations, list) or not expectations:
        raise CorpusValidationError(f"{item_id}: capability_expectations required")
    resolved: list[tuple[PromotionCapability, dict[str, Any]]] = []
    seen: set[str] = set()
    for expectation in expectations:
        if not isinstance(expectation, dict):
            raise CorpusValidationError(f"{item_id}: capability expectation must be an object")
        if frozenset(expectation) != _EXPECTATION_KEYS:
            raise CorpusValidationError(
                f"{item_id}: capability expectation keys must be exact"
            )
        capability_id = expectation["promotion_capability_id"]
        if not isinstance(capability_id, str) or capability_id in seen:
            raise CorpusValidationError(
                f"{item_id}: capability ids must be canonical and unique"
            )
        try:
            capability = registry.require(capability_id)
        except KeyError as exc:
            raise CorpusValidationError(
                f"{item_id}: unknown promotion capability {capability_id}"
            ) from exc
        if capability.lifecycle != "ACTIVE":
            raise CorpusValidationError(f"{item_id}: retired capability is not replayable")
        if capability.artifact_contract_kind != item.get("artifact_contract_kind"):
            raise CorpusValidationError(
                f"{item_id}: capability artifact contract mismatch"
            )
        if capability.source_contract_version != item.get("source_contract_version"):
            raise CorpusValidationError(
                f"{item_id}: capability source contract mismatch"
            )
        if not isinstance(expectation["replay_required"], bool):
            raise CorpusValidationError(f"{item_id}: replay_required must be boolean")
        if not isinstance(expectation["expected_semantics"], dict):
            raise CorpusValidationError(f"{item_id}: expected_semantics must be an object")
        seen.add(capability_id)
        resolved.append((capability, expectation))
    return tuple(resolved)


def corpus_snapshot_digest(
    manifest: dict[str, Any],
    *,
    repo_root: Path,
) -> str:
    registry = _registry(repo_root)
    vector: list[dict[str, Any]] = []
    for entry in manifest.get("entries", []):
        for capability, expectation in _capability_expectations(
            entry,
            registry,
            item_id=entry.get("corpus_id", "<missing>"),
        ):
            vector.append(
                {
                    "corpus_id": entry.get("corpus_id"),
                    "reference_month": entry.get("reference_month"),
                    "artifact_contract_kind": entry.get("artifact_contract_kind"),
                    "source_contract_version": entry.get("source_contract_version"),
                    "official_locator": entry.get("official_locator"),
                    "artifact_sha256": entry.get("expected_sha256"),
                    "materialization_status": entry.get("materialization_status"),
                    "exceptional_tags": sorted(entry.get("exceptional_tags", [])),
                    "promotion_capability_id": capability.promotion_capability_id,
                    "release_subject_digest": (
                        capability.release_subject.release_subject_digest
                    ),
                    "extractor_contract_version": (
                        capability.extractor_contract_version
                    ),
                    "promotion_family": capability.promotion_family,
                    "replay_required": expectation["replay_required"],
                    "expected_semantics_digest": _semantic_digest(
                        expectation["expected_semantics"]
                    ),
                }
            )
    return _canonical_set_digest(vector)


def replay_result_digest(results: list[dict[str, Any]]) -> str:
    fields = (
        "corpus_id",
        "promotion_capability_id",
        "release_subject_digest",
        "extractor_contract_version",
        "artifact_sha256",
        "inventory_status",
        "semantic_status",
        "expected_semantics_digest",
        "actual_semantics_digest",
    )
    return _canonical_set_digest(
        [{field: result.get(field) for field in fields} for result in results]
    )


def validate_manifest(manifest: dict[str, Any], repo_root: Path) -> None:
    registry = _registry(repo_root)
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
        for legacy_field in (
            "extractor_contract_version",
            "replay_required",
            "expected_semantics",
        ):
            if legacy_field in entry:
                raise CorpusValidationError(
                    f"{corpus_id}: legacy artifact-wide {legacy_field} is forbidden"
                )
        _capability_expectations(entry, registry, item_id=corpus_id)

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
            path_obj = Path(local_path)
            if path_obj.is_absolute() or ".." in path_obj.parts:
                raise CorpusValidationError(
                    f"{corpus_id}: official local_path must be repo-relative"
                )
            official_prefix = Path("tests/fixtures/cpi_w1/official")
            if path_obj.parts[: len(official_prefix.parts)] != official_prefix.parts:
                raise CorpusValidationError(
                    f"{corpus_id}: official local_path must stay under official corpus directory"
                )
            full_path = (repo_root / path_obj).resolve()
            allowed_root = (repo_root / official_prefix).resolve()
            if allowed_root not in full_path.parents:
                raise CorpusValidationError(
                    f"{corpus_id}: official local_path escapes corpus directory"
                )
            if full_path.is_symlink() or not full_path.is_file():
                raise CorpusValidationError(
                    f"{corpus_id}: local fixture must be a regular non-symlink file"
                )
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
    registry = _registry(repo_root)
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
        for legacy_field in ("extractor_contract_version", "expected_semantics"):
            if legacy_field in fixture:
                raise CorpusValidationError(
                    f"{fixture_id}: legacy artifact-wide {legacy_field} is forbidden"
                )
        _capability_expectations(fixture, registry, item_id=fixture_id)
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
    capability: PromotionCapability,
    expectation: dict[str, Any],
) -> ReplayEntryResult:
    return _replay_capability(
        fixture,
        item_id=fixture["fixture_id"],
        inventory="SYNTHETIC_CONFORMANCE",
        repo_root=repo_root,
        capability=capability,
        expectation=expectation,
        expected_diff_approvals=[],
    )


def inventory_status(entry: dict[str, Any]) -> str:
    if entry["materialization_status"] != "MATERIALIZED":
        return "REMOTE_ONLY"
    return "MATERIALIZED_PINNED"


def _actual_core4(
    path: Path, reference_month: str, *, extractor_contract_version: str
) -> dict[str, str]:
    year, month = map(int, reference_month.split("-"))
    bundle = extract_core4_from_release_html(
        path.read_bytes(),
        expected_reference_month=date(year, month, 1),
        extractor_contract_version=extractor_contract_version,
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


def _actual_release_envelope(
    path: Path, reference_month: str, *, extractor_contract_version: str
) -> dict[str, str]:
    year, month = map(int, reference_month.split("-"))
    candidate = extract_release_envelope(
        path.read_bytes(),
        expected_reference_month=date(year, month, 1),
        extractor_contract_version=extractor_contract_version,
    )
    return {
        "event_type": candidate.event_type,
        "reference_month": candidate.reference_month.strftime("%Y-%m"),
    }


def _result(
    item: dict[str, Any],
    item_id: str,
    capability: PromotionCapability,
    inventory: str,
    semantic_status: str,
    expected: Any,
    actual: Any = None,
    detail: str | None = None,
) -> ReplayEntryResult:
    return ReplayEntryResult(
        corpus_id=item_id,
        promotion_capability_id=capability.promotion_capability_id,
        release_subject_digest=capability.release_subject.release_subject_digest,
        extractor_contract_version=capability.extractor_contract_version,
        artifact_sha256=item.get("expected_sha256"),
        expected_semantics_digest=_semantic_digest(expected),
        actual_semantics_digest=(
            _semantic_digest(actual) if actual is not None else None
        ),
        inventory_status=inventory,
        semantic_status=semantic_status,
        detail=detail,
    )


def _replay_capability(
    item: dict[str, Any],
    *,
    item_id: str,
    inventory: str,
    repo_root: Path,
    capability: PromotionCapability,
    expectation: dict[str, Any],
    expected_diff_approvals: list[dict[str, Any]],
) -> ReplayEntryResult:
    expected = expectation["expected_semantics"]
    if inventory not in {"MATERIALIZED_PINNED", "SYNTHETIC_CONFORMANCE"}:
        return _result(item, item_id, capability, inventory, "NOT_RUN", expected)

    if expected.get("kind") == "UNVERIFIED_INVENTORY":
        return _result(
            item,
            item_id,
            capability,
            inventory,
            "NEWLY_ACCEPTED",
            expected,
            detail="materialized artifact has no pinned capability expectation",
        )
    try:
        dispatch = (
            capability.promotion_capability_id,
            capability.extractor_contract_version,
        )
        if dispatch == ("BLS_CPI_CORE4_HTML", "bls-cpi-core4-html-v1"):
            actual = _actual_core4(
                repo_root / item["local_path"],
                item["reference_month"],
                extractor_contract_version=capability.extractor_contract_version,
            )
            expected_value = expected.get("values")
            supported_kind = "CORE4"
        elif dispatch == ("BLS_CPI_RELEASE_ENVELOPE_HTML", "bls-cpi-release-envelope-html-v1"):
            actual = _actual_release_envelope(
                repo_root / item["local_path"],
                item["reference_month"],
                extractor_contract_version=capability.extractor_contract_version,
            )
            expected_value = expected.get("values")
            supported_kind = "RELEASE_ENVELOPE"
        elif dispatch == ("BLS_CPI_REVISED_RELEASE_DATES", "bls-cpi-revised-release-dates-v1"):
            year, month = map(int, item["reference_month"].split("-"))
            candidate = parse_bls_revised_release_dates_html(
                (repo_root / item["local_path"]).read_bytes(),
                expected_reference_month=date(year, month, 1),
                extractor_contract_version=capability.extractor_contract_version,
            )
            actual = {"schedule_status": candidate.schedule_status.value}
            expected_value = {"schedule_status": expected.get("schedule_status")}
            supported_kind = "CANCELLATION"
        else:
            return _result(
                item,
                item_id,
                capability,
                "BLOCKED_NO_EXTRACTOR",
                "NOT_RUN",
                expected,
                detail=f"capability/extractor not implemented by replay tool: {dispatch}",
            )
    except Exception as exc:
        return _result(
            item,
            item_id,
            capability,
            inventory,
            "NEWLY_FAILED",
            expected,
            detail=f"{type(exc).__name__}: {exc}",
        )

    if expected.get("kind") == supported_kind and actual == expected_value:
        return _result(
            item,
            item_id,
            capability,
            inventory,
            "SEMANTIC_UNCHANGED",
            expected_value,
            actual,
        )
    changed_status = (
        "EXPECTED_CHANGED"
        if _approved_semantic_change(
            approvals=expected_diff_approvals,
            entry=item,
            corpus_id=item_id,
            promotion_capability_id=capability.promotion_capability_id,
            release_subject_digest=capability.release_subject.release_subject_digest,
            extractor_contract_version=capability.extractor_contract_version,
            expected=expected_value,
            actual=actual,
            repo_root=repo_root,
        )
        else "UNEXPECTED_CHANGED"
    )
    return _result(
        item,
        item_id,
        capability,
        inventory,
        changed_status,
        expected_value,
        actual,
        detail=json.dumps(
            {"expected": expected_value, "actual": actual},
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def replay_entry(
    entry: dict[str, Any],
    repo_root: Path,
    capability: PromotionCapability,
    expectation: dict[str, Any],
    *,
    expected_diff_approvals: list[dict[str, Any]] | None = None,
) -> ReplayEntryResult:
    status = inventory_status(entry)
    return _replay_capability(
        entry,
        item_id=entry["corpus_id"],
        inventory=status,
        repo_root=repo_root,
        capability=capability,
        expectation=expectation,
        expected_diff_approvals=expected_diff_approvals or [],
    )


def build_report(
    manifest: dict[str, Any],
    *,
    repo_root: Path,
    expected_diff_approvals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    validate_manifest(manifest, repo_root)
    validate_conformance_fixtures(manifest, repo_root)
    registry = _registry(repo_root)
    entry_expectations = [
        (
            entry,
            capability,
            expectation,
        )
        for entry in manifest["entries"]
        for capability, expectation in _capability_expectations(
            entry,
            registry,
            item_id=entry["corpus_id"],
        )
    ]
    results = [
        replay_entry(
            entry,
            repo_root,
            capability,
            expectation,
            expected_diff_approvals=expected_diff_approvals,
        )
        for entry, capability, expectation in entry_expectations
    ]
    conformance_expectations = [
        (fixture, capability, expectation)
        for fixture in manifest.get("conformance_fixtures", [])
        for capability, expectation in _capability_expectations(
            fixture,
            registry,
            item_id=fixture["fixture_id"],
        )
    ]
    conformance_results = [
        replay_conformance_fixture(fixture, repo_root, capability, expectation)
        for fixture, capability, expectation in conformance_expectations
    ]
    official_corpus_ready = all(
        (not expectation["replay_required"])
        or (
            result.inventory_status == "MATERIALIZED_PINNED"
            and result.semantic_status in _PASS_SEMANTIC
        )
        for (_entry, _capability, expectation), result in zip(
            entry_expectations,
            results,
            strict=True,
        )
    )
    conformance_ready = all(
        result.semantic_status in _PASS_SEMANTIC
        for result in conformance_results
    )
    from src.cpi_w1_evidence_policy import CapabilityEvidencePolicyRegistry
    from src.cpi_w1_evidence_readiness import evaluate_capability_readiness

    policy_path = repo_root / "config/cpi_w1_capability_evidence_policies.json"
    policies = (CapabilityEvidencePolicyRegistry.from_json(policy_path, registry)
                if policy_path.is_file() else CapabilityEvidencePolicyRegistry(()))
    readiness = evaluate_capability_readiness(
        registry, policies, manifest,
        [result.as_dict() for result in results],
        [result.as_dict() for result in conformance_results],
        expected_diff_approvals or [],
        repo_root=repo_root,
    )
    official_corpus_ready = all(row.status == "READY" for row in readiness)
    evidence_requirements_satisfied = official_corpus_ready and conformance_ready
    return {
        "schema_version": _SCHEMA_VERSION,
        "baseline": manifest["baseline"],
        "official_corpus_ready": official_corpus_ready,
        "conformance_ready": conformance_ready,
        "evidence_requirements_satisfied": evidence_requirements_satisfied,
        "capability_readiness": [row.as_dict() for row in readiness],
        "counts": {
            "entries": len(manifest["entries"]),
            "capability_results": len(results),
            "materialized_pinned": sum(
                entry["materialization_status"] == "MATERIALIZED"
                for entry in manifest["entries"]
            ),
            "remote_only": sum(
                entry["materialization_status"] == "REMOTE_ONLY"
                for entry in manifest["entries"]
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
        "--expected-diffs",
        type=Path,
        default=Path("tests/fixtures/cpi_w1/expected-diffs.json"),
    )
    parser.add_argument(
        "--inventory-only",
        action="store_true",
        help="report incomplete materialization without enforcing release readiness",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    manifest = load_manifest(args.manifest)
    approvals = load_expected_diffs(args.expected_diffs)
    report = build_report(
        manifest,
        repo_root=repo_root,
        expected_diff_approvals=approvals,
    )
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if args.inventory_only:
        return 0
    return 0 if report["evidence_requirements_satisfied"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
