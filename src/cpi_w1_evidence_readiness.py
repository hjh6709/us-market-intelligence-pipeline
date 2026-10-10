"""Capability-scoped corpus identity and fail-closed release evidence accounting."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
import re

from src.cpi_w1_evidence_policy import canonical_evidence_bytes


_PASS = {"SEMANTIC_UNCHANGED", "EXPECTED_CHANGED"}


def _digest(value):
    return hashlib.sha256(canonical_evidence_bytes(value)).hexdigest()


def _scoped(items, capability_id, identity_keys):
    selected = [dict(item) for item in items if item.get("promotion_capability_id") == capability_id]
    identities = [tuple(item.get(key) for key in identity_keys) for item in selected]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate capability evidence identity")
    return sorted(selected, key=canonical_evidence_bytes)


def capability_replay_result_digest(results, capability_id):
    return _digest(_scoped(results, capability_id, ("corpus_id", "extractor_contract_version")))


def capability_expected_diff_digest(approvals, capability_id):
    return _digest(_scoped(approvals, capability_id, (
        "corpus_id", "artifact_sha256", "extractor_contract_version",
        "expected_semantics_sha256", "actual_semantics_sha256")))


def _applicable(entries, capability_id):
    for entry in entries:
        matching = [e for e in entry.get("capability_expectations", []) if e.get("promotion_capability_id") == capability_id]
        if len(matching) > 1:
            raise ValueError("duplicate capability expectation")
        if matching:
            yield entry, matching[0]


def _expected_value(expectation):
    semantics = expectation["expected_semantics"]
    if semantics.get("kind") in {"CORE4", "RELEASE_ENVELOPE"}:
        return semantics.get("values")
    if semantics.get("kind") == "CANCELLATION":
        return {"schedule_status": semantics.get("schedule_status")}
    return None


def _verified_result(entry, expectation, result, capability, inventory, approvals, repo_root):
    """Rejoin replay proof to its current pinned evaluation, never trust a PASS label."""
    expected = _expected_value(expectation)
    if expected is None or result is None:
        return False
    artifact_hash = entry.get("expected_sha256")
    if not isinstance(artifact_hash, str) or re.fullmatch(r"[0-9a-f]{64}", artifact_hash) is None:
        return False
    if (result.get("promotion_capability_id") != capability.promotion_capability_id
        or result.get("release_subject_digest") != capability.release_subject.release_subject_digest
        or result.get("extractor_contract_version") != capability.extractor_contract_version
        or result.get("artifact_sha256") != artifact_hash
        or result.get("inventory_status") != inventory
        or result.get("expected_semantics_digest") != _digest(expected)):
        return False
    if result.get("semantic_status") == "SEMANTIC_UNCHANGED":
        return result.get("actual_semantics_digest") == _digest(expected)
    if result.get("semantic_status") != "EXPECTED_CHANGED":
        return False
    from src.cpi_w1_review_artifact import load_review_artifact
    bindings = {"release_subject_digest": capability.release_subject.release_subject_digest,
        "promotion_capability_id": capability.promotion_capability_id, "artifact_sha256": artifact_hash,
        "extractor_contract_version": capability.extractor_contract_version,
        "expected_semantics_sha256": _digest(expected),
        "actual_semantics_sha256": result.get("actual_semantics_digest")}
    matches = []
    for approval in approvals:
        if approval.get("corpus_id") != result["corpus_id"] or any(approval.get(k) != v for k, v in bindings.items()):
            continue
        try:
            review = load_review_artifact(repo_root, approval.get("review_ref"), approval.get("review_digest"))
            if review.payload()["purpose"] != "EXPECTED_DIFF":
                continue
            review.require_bindings(bindings)
        except (ValueError, OSError):
            continue
        matches.append(approval)
    return len(matches) == 1


def capability_corpus_snapshot_digest(manifest, registry, policy):
    capability = registry.require(policy.promotion_capability_id)
    vector = []
    for group, id_key in (("entries", "corpus_id"), ("conformance_fixtures", "fixture_id")):
        for entry, expectation in _applicable(manifest.get(group, []), capability.promotion_capability_id):
            vector.append({"group": group, "id": entry[id_key], "reference_month": entry.get("reference_month"),
                "artifact_contract_kind": entry["artifact_contract_kind"],
                "source_contract_version": entry["source_contract_version"],
                "locator": entry.get("official_locator"), "artifact_sha256": entry.get("expected_sha256"),
                "materialization_status": entry.get("materialization_status"),
                "review": entry.get("review"), "exceptional_tags": sorted(entry.get("exceptional_tags", [])),
                "replay_required": expectation["replay_required"],
                "expected_semantics_digest": _digest(expectation["expected_semantics"])})
    if len({(v["group"], v["id"]) for v in vector}) != len(vector):
        raise ValueError("duplicate corpus identity")
    dependencies = []
    if policy.payload()["coverage_mode"] == "HISTORICAL_MONTHLY_BASELINE":
        for entry, expectation in _applicable(manifest.get("entries", []), "BLS_CPI_REVISED_RELEASE_DATES"):
            if "EXPLICIT_CANCELLATION" in entry.get("exceptional_tags", []):
                dependency_cap = registry.require("BLS_CPI_REVISED_RELEASE_DATES")
                dependencies.append({"id": entry["corpus_id"], "month": entry["reference_month"],
                    "hash": entry.get("expected_sha256"), "state": entry["materialization_status"],
                    "extractor": dependency_cap.extractor_contract_version,
                    "release_subject_digest": dependency_cap.release_subject.release_subject_digest,
                    "artifact_contract_kind": entry["artifact_contract_kind"],
                    "source_contract_version": entry["source_contract_version"],
                    "review": entry.get("review"), "exceptional_tags": sorted(entry.get("exceptional_tags", [])),
                    "replay_required": expectation["replay_required"],
                    "expected": expectation["expected_semantics"], "locator": entry["official_locator"]})
    return _digest({"capability": capability.promotion_capability_id,
        "subject": capability.release_subject.release_subject_digest,
        "extractor": capability.extractor_contract_version,
        "coverage_rule": policy.payload()["coverage_rule"],
        "corpus": sorted(vector, key=canonical_evidence_bytes),
        "dependencies": sorted(dependencies, key=canonical_evidence_bytes)})


@dataclass(frozen=True)
class CapabilityEvidenceReadiness:
    promotion_capability_id: str
    policy_present: bool
    required_evidence_obligations: tuple[str, ...]
    applicable_official: int
    materialized_pinned: int
    replay_passed: int
    conformance_passed: int
    unresolved_expected_diffs: int
    missing_evidence_classes: tuple[str, ...]
    missing_reference_months: tuple[str, ...]
    status: str
    blocking_reasons: tuple[str, ...]

    def as_dict(self):
        return asdict(self)


def _months(start, end):
    year, month = map(int, start.split("-"))
    while f"{year:04d}-{month:02d}" <= end:
        yield f"{year:04d}-{month:02d}"
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def evaluate_capability_readiness(registry, policies, manifest, replay_results, conformance_results, approvals, *, repo_root=None):
    if not registry.active():
        raise ValueError("required ACTIVE capability set is empty")
    rows = []
    repo_root = repo_root or Path(__file__).resolve().parents[1]
    for capability in registry.active():
        cid = capability.promotion_capability_id
        relevant = list(_applicable(manifest.get("entries", []), cid))
        replay = _scoped(replay_results, cid, ("corpus_id", "extractor_contract_version"))
        conformance = _scoped(conformance_results, cid, ("corpus_id", "extractor_contract_version"))
        replay_by_id = {r["corpus_id"]: r for r in replay}
        verified = [(e, x) for e, x in relevant if e.get("materialization_status") == "MATERIALIZED"
            and _verified_result(e, x, replay_by_id.get(e["corpus_id"]), capability,
                                 "MATERIALIZED_PINNED", approvals, repo_root)]
        # Replay independently reads and hashes actual bytes. Inventory metadata alone never counts.
        materialized = sum(e.get("materialization_status") == "MATERIALIZED" and e.get("expected_sha256") is not None for e, _ in relevant)
        cf_entries = list(_applicable(manifest.get("conformance_fixtures", []), cid))
        cf_by_id = {r["corpus_id"]: r for r in conformance}
        cf_pass = sum(_verified_result(e, x, cf_by_id.get(e["fixture_id"]), capability,
                                      "SYNTHETIC_CONFORMANCE", [], repo_root) for e, x in cf_entries)
        reasons, missing_classes, missing_months, obligations = [], [], [], []
        try:
            policy = policies.require(cid)
        except KeyError:
            policy = None
            reasons.append("POLICY_MISSING")
        if not verified:
            reasons.append("OFFICIAL_EVIDENCE_MISSING")
        if policy is not None:
            p = policy.payload()
            obligations = p["required_official_evidence_classes"] + p["required_exceptional_cases"]
            if not policy.complete:
                reasons.append("POLICY_INCOMPLETE")
            kinds = {e["artifact_contract_kind"] for e, _ in verified}
            missing_classes = sorted(set(p["required_official_evidence_classes"]) - kinds)
            if missing_classes:
                reasons.append("EVIDENCE_CLASSES_MISSING")
            rule = p["coverage_rule"]
            observed_months = {e["reference_month"] for e, _ in verified}
            if p["coverage_mode"] == "HISTORICAL_MONTHLY_BASELINE":
                # A declared cancellation alone never exempts its month. Evidence must replay as canceled.
                canceled = set()
                for e, x in _applicable(manifest.get("entries", []), "BLS_CPI_REVISED_RELEASE_DATES"):
                    if e["materialization_status"] == "MATERIALIZED" and x["expected_semantics"].get("schedule_status") == "CANCELED":
                        dependency = registry.require("BLS_CPI_REVISED_RELEASE_DATES")
                        if any(r.get("corpus_id") == e["corpus_id"]
                               and _verified_result(e, x, r, dependency, "MATERIALIZED_PINNED", approvals, repo_root)
                               and r.get("actual_semantics_digest") == _digest({"schedule_status": "CANCELED"}) for r in replay_results):
                            canceled.add(e["reference_month"])
                missing_months = sorted(set(_months(rule["start_reference_month"], rule["end_reference_month"])) - observed_months - canceled)
            elif p["coverage_mode"] == "EXPLICIT_HISTORICAL_EXCEPTIONS":
                missing_months = sorted(set(rule["reference_months"]) - observed_months)
            if missing_months:
                reasons.append("REFERENCE_MONTH_COVERAGE_MISSING")
            # Missing exceptional proof is never filled with a synthetic fixture or an unverified tag.
            tags = {tag for e, _ in verified for tag in e.get("exceptional_tags", [])}
            required_tags = set(p["required_exceptional_cases"]) - {"DISCOVERED_CHANGED_BYTES", "RELEVANT_CORRECTIONS"}
            if required_tags - tags:
                reasons.append("EXCEPTIONAL_CASES_MISSING")
            # V2 conformance classes are registry capability identities, not caller labels.
            proven_cf_classes = {cid} if cf_pass else set()
            if (set(p["required_conformance_classes"]) - proven_cf_classes
                or cf_pass == 0 or cf_pass != len(cf_entries) or cf_pass != len(conformance)):
                reasons.append("CONFORMANCE_MISSING_OR_FAILED")
        unresolved = sum(r.get("semantic_status") not in _PASS for r in replay)
        if unresolved:
            reasons.append("REPLAY_MISSING_OR_FAILED")
        if len(verified) != len(relevant):
            reasons.append("OFFICIAL_COVERAGE_UNVERIFIED")
        rows.append(CapabilityEvidenceReadiness(cid, policy is not None, tuple(obligations), len(relevant),
            materialized, len(verified), cf_pass, unresolved, tuple(missing_classes), tuple(missing_months),
            "NOT_READY" if reasons else "READY", tuple(sorted(set(reasons)))))
    return tuple(rows)
