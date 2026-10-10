"""Capability-scoped corpus identity and fail-closed release evidence accounting."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib

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
        "prior_expected_semantics_digest", "new_actual_semantics_digest")))


def _applicable(entries, capability_id):
    for entry in entries:
        matching = [e for e in entry.get("capability_expectations", []) if e.get("promotion_capability_id") == capability_id]
        if len(matching) > 1:
            raise ValueError("duplicate capability expectation")
        if matching:
            yield entry, matching[0]


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


def evaluate_capability_readiness(registry, policies, manifest, replay_results, conformance_results, approvals):
    if not registry.active():
        raise ValueError("required ACTIVE capability set is empty")
    rows = []
    for capability in registry.active():
        cid = capability.promotion_capability_id
        relevant = list(_applicable(manifest.get("entries", []), cid))
        replay = _scoped(replay_results, cid, ("corpus_id", "extractor_contract_version"))
        conformance = _scoped(conformance_results, cid, ("corpus_id", "extractor_contract_version"))
        passed_results = {r["corpus_id"]: r for r in replay if r.get("semantic_status") in _PASS
            and r.get("inventory_status") == "MATERIALIZED_PINNED"
            and r.get("extractor_contract_version") == capability.extractor_contract_version}
        verified = [(e, x) for e, x in relevant if e.get("materialization_status") == "MATERIALIZED"
            and isinstance(e.get("expected_sha256"), str) and len(e["expected_sha256"]) == 64
            and e["corpus_id"] in passed_results
            and passed_results[e["corpus_id"]].get("artifact_sha256") == e["expected_sha256"]
            and x["expected_semantics"].get("kind") != "UNVERIFIED_INVENTORY"]
        # Replay independently reads and hashes actual bytes. Inventory metadata alone never counts.
        materialized = sum(e.get("materialization_status") == "MATERIALIZED" and e.get("expected_sha256") is not None for e, _ in relevant)
        cf_pass = sum(r.get("semantic_status") in _PASS and r.get("extractor_contract_version") == capability.extractor_contract_version for r in conformance)
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
                        if any(r.get("corpus_id") == e["corpus_id"] and r.get("promotion_capability_id") == "BLS_CPI_REVISED_RELEASE_DATES"
                               and r.get("inventory_status") == "MATERIALIZED_PINNED" and r.get("semantic_status") in _PASS for r in replay_results):
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
            if cf_pass == 0 or cf_pass != len(conformance):
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
