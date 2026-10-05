#!/usr/bin/env python3
"""Controlled CPI W1 collection/orchestration entry point.

This module captures official bytes and schedules only release-gate-eligible
promotion work. It does not execute canonical promotion or legacy CPI upserts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg

from src.cpi_w1_artifacts import FilesystemArtifactStore
from src.cpi_w1_authorization import ExecutorProvenanceV1
from src.cpi_w1_contracts import PromotionFamily
from src.cpi_w1_promotion_capabilities import PromotionCapabilityRegistry
from src.cpi_w1_promoter import promotion_work_key
from src.cpi_w1_release_gate import CpiW1ExtractorReleaseGate
from src.cpi_w1_repository import CpiW1Repository
from src.cpi_w1_source import (
    BlsCpiSourceClient,
    BlsCpiSourceContract,
    SourceFailureKind,
    SourceFetchError,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE_CONTRACT_PATH = ROOT / "config/cpi_w1_source_contract.json"
RELEASE_GATE_PATH = ROOT / "config/cpi_w1_extractor_release_gate.json"
CAPABILITY_REGISTRY_PATH = ROOT / "config/cpi_w1_promotion_capabilities.json"
EVIDENCE_SNAPSHOT_PATH = ROOT / "config/cpi_w1_evidence_snapshots.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".cpi-w1-artifacts"

_SOURCE_REVISION = "cpi-w1-source-v1"
_COLLECT_JOB_CONTRACT = "cpi-w1-collector-v1"
_PROMOTE_JOB_CONTRACT = "cpi-w1-promoter-v1"
_COLLECT_WORKLOAD_ARTIFACT_DIGEST = hashlib.sha256(
    Path(__file__).read_bytes()
).hexdigest()
_PROMOTE_WORKLOAD_ARTIFACT_DIGEST = hashlib.sha256(
    (ROOT / "src/cpi_w1_promoter.py").read_bytes()
).hexdigest()
_COLLECT_EXECUTOR = ExecutorProvenanceV1(
    source_revision=_SOURCE_REVISION,
    workload_artifact_digest=_COLLECT_WORKLOAD_ARTIFACT_DIGEST,
    job_contract_version=_COLLECT_JOB_CONTRACT,
)

@dataclass(frozen=True)
class CollectionResult:
    run_id: UUID | None
    work_item_id: UUID | None
    artifact_id: UUID | None
    outcome: str
    promotion_status: str
    promotion_work_ids: tuple[UUID, ...] = ()
    blocked_reason_code: str | None = None
    deferred_capability_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PromotionScheduleResult:
    status: str
    work_item_ids: tuple[UUID, ...] = ()
    deferred_capability_ids: tuple[str, ...] = ()
    blocked_reason_code: str | None = None


class CpiW1CollectorOrchestrator:
    def __init__(
        self,
        *,
        repository: CpiW1Repository,
        source_contract: BlsCpiSourceContract,
        capability_registry: PromotionCapabilityRegistry,
        release_gate: CpiW1ExtractorReleaseGate,
        artifact_store: FilesystemArtifactStore,
        source_client: BlsCpiSourceClient | None = None,
    ) -> None:
        self.repository = repository
        self.source_contract = source_contract
        self.capability_registry = capability_registry
        self.release_gate = release_gate
        self.artifact_store = artifact_store
        self.source_client = source_client or BlsCpiSourceClient(source_contract)

    @staticmethod
    def _fingerprint_file(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _config_fingerprint(self) -> str:
        material = {
            "capability_registry": self._fingerprint_file(
                CAPABILITY_REGISTRY_PATH
            ),
            "evidence_snapshots": self._fingerprint_file(EVIDENCE_SNAPSHOT_PATH),
            "release_gate": self._fingerprint_file(RELEASE_GATE_PATH),
            "source_contract": self._fingerprint_file(SOURCE_CONTRACT_PATH),
        }
        encoded = json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _schedule_promotions(
        self,
        connection: Any,
        *,
        artifact_id: UUID,
        artifact_contract_kind: str,
        source_contract_version: str,
        run_mode: str,
        replay_of_run_id: UUID | None = None,
        only_extractor_version: str | None = None,
    ) -> PromotionScheduleResult:
        capabilities = self.capability_registry.active_for_artifact(
            source_code="BLS",
            artifact_contract_kind=artifact_contract_kind,
            source_contract_version=source_contract_version,
        )
        if only_extractor_version is not None:
            capabilities = tuple(
                capability
                for capability in capabilities
                if capability.extractor_contract_version == only_extractor_version
            )
        if not capabilities:
            return PromotionScheduleResult(
                status="NO_PROMOTION_CONTRACT",
                blocked_reason_code="CAPABILITY_NOT_REVIEWED",
            )

        if run_mode == "REPLAY" and replay_of_run_id is None:
            raise ValueError("REPLAY promotion scheduling requires replay_of_run_id")
        if run_mode != "REPLAY" and replay_of_run_id is not None:
            raise ValueError("only REPLAY promotion scheduling accepts replay_of_run_id")

        targets = self.repository.artifact_promotion_targets(
            connection,
            artifact_id=artifact_id,
        )
        planned = []
        deferred_ids = []
        blocked_reasons = []
        eligible_capability_count = 0
        for capability in capabilities:
            subject = capability.release_subject
            decision = self.release_gate.decision(
                capability.promotion_capability_id,
                subject.release_subject_digest,
            )
            if not decision.eligible:
                if (
                    decision.evidence_snapshot_digest is None
                    or decision.gate_decision_digest is None
                    or decision.review_ref is None
                ):
                    raise RuntimeError(
                        "checked-in blocked capability decision lacks durable evidence"
                    )
                self.repository.record_promotion_deferred(
                    connection,
                    artifact_id=artifact_id,
                    promotion_capability_id=capability.promotion_capability_id,
                    release_subject_digest=subject.release_subject_digest,
                    extractor_contract_version=(
                        capability.extractor_contract_version
                    ),
                    evidence_snapshot_digest=decision.evidence_snapshot_digest,
                    gate_decision_digest=decision.gate_decision_digest,
                    reason_code=decision.reason_code,
                    review_ref=decision.review_ref,
                    gate_fingerprint=self.release_gate.gate_fingerprint,
                )
                deferred_ids.append(capability.promotion_capability_id)
                blocked_reasons.append(decision.reason_code)
                continue
            eligible_capability_count += 1
            for reference_month in targets:
                family = PromotionFamily(capability.promotion_family)
                planned.append(
                    (
                        capability,
                        promotion_work_key(
                            family,
                            artifact_id,
                            capability.extractor_contract_version,
                            reference_month,
                        ),
                    )
                )

        if not planned:
            if eligible_capability_count:
                return PromotionScheduleResult(
                    status=(
                        "NO_TARGET_BINDING_WITH_DEFERRED_CAPABILITIES"
                        if deferred_ids
                        else "NO_TARGET_BINDING"
                    ),
                    deferred_capability_ids=tuple(deferred_ids),
                    blocked_reason_code="ARTIFACT_TARGET_NOT_BOUND",
                )
            if deferred_ids:
                return PromotionScheduleResult(
                    status="BLOCKED_BY_RELEASE_GATE",
                    deferred_capability_ids=tuple(deferred_ids),
                    blocked_reason_code=(
                        blocked_reasons[0]
                        if len(set(blocked_reasons)) == 1
                        else "MULTIPLE_CAPABILITIES_BLOCKED"
                    ),
                )
            raise RuntimeError("capability evaluation produced no scheduling outcome")

        work_keys = tuple(work_key for _, work_key in planned)
        with connection.transaction():
            self.repository.lock_promotion_scheduling(connection, artifact_id)
            existing = self.repository.existing_promotion_work_items(
                connection,
                artifact_id=artifact_id,
                work_keys=work_keys,
            )
            missing = tuple(key for key in work_keys if key not in existing)
            if not missing:
                return PromotionScheduleResult(
                    status=(
                        "ALREADY_SCHEDULED_WITH_DEFERRED_CAPABILITIES"
                        if deferred_ids
                        else "ALREADY_SCHEDULED"
                    ),
                    work_item_ids=tuple(existing[key] for key in work_keys),
                    deferred_capability_ids=tuple(deferred_ids),
                )

            workset_digest = hashlib.sha256(
                "\n".join(sorted(missing)).encode("utf-8")
            ).hexdigest()
            replay_token = (
                f":replay-of:{replay_of_run_id}"
                if replay_of_run_id is not None
                else ""
            )
            promote_run_id = self.repository.create_run(
                connection,
                execution_scope="ECONOMIC_PROMOTE",
                job_type="CPI_W1_PROMOTE",
                trigger_type="ARTIFACT_HANDOFF",
                run_mode=run_mode,
                trigger_idempotency_key=(
                    f"artifact:{run_mode}:{artifact_id}:workset:{workset_digest}"
                    f"{replay_token}"
                ),
                scheduled_for=None,
                replay_of_run_id=replay_of_run_id,
                source_revision=_SOURCE_REVISION,
                workload_artifact_digest=_PROMOTE_WORKLOAD_ARTIFACT_DIGEST,
                job_contract_version=_PROMOTE_JOB_CONTRACT,
                config_fingerprint=self._config_fingerprint(),
            )
            capability_by_key = {
                work_key: capability for capability, work_key in planned
            }
            for work_key in missing:
                capability = capability_by_key[work_key]
                existing[work_key] = self.repository.create_work_item(
                    connection,
                    run_id=promote_run_id,
                    execution_scope="ECONOMIC_PROMOTE",
                    work_key=work_key,
                    input_artifact_id=artifact_id,
                    release_subject=capability.release_subject,
                )
        return PromotionScheduleResult(
            status=("PARTIALLY_SCHEDULED" if deferred_ids else "SCHEDULED"),
            work_item_ids=tuple(existing[key] for key in work_keys),
            deferred_capability_ids=tuple(deferred_ids),
        )

    def collect_locator(
        self,
        connection: Any,
        *,
        locator_key: str,
        run_mode: str,
        trigger_idempotency_key: str,
        reference_months: tuple[date, ...],
        dry_run: bool = False,
    ) -> CollectionResult:
        if run_mode not in {"LIVE", "BACKFILL"}:
            raise ValueError("network collection supports LIVE or BACKFILL only")
        locator = self.source_contract.locators.get(locator_key)
        if locator is None:
            raise KeyError(f"unknown CPI source locator: {locator_key}")

        if dry_run:
            return CollectionResult(
                run_id=None,
                work_item_id=None,
                artifact_id=None,
                outcome="DRY_RUN",
                promotion_status="NOT_EVALUATED",
            )

        run_id = self.repository.create_run(
            connection,
            execution_scope="ECONOMIC_COLLECT",
            job_type="CPI_W1_COLLECT",
            trigger_type="CLI",
            run_mode=run_mode,
            trigger_idempotency_key=trigger_idempotency_key,
            scheduled_for=None,
            replay_of_run_id=None,
            source_revision=_SOURCE_REVISION,
            workload_artifact_digest=locator_key,
            job_contract_version=_COLLECT_JOB_CONTRACT,
            config_fingerprint=self._config_fingerprint(),
        )
        collection_work_key = f"COLLECT:{locator_key}:{run_id}"
        work_id = self.repository.create_work_item(
            connection,
            run_id=run_id,
            execution_scope="ECONOMIC_COLLECT",
            work_key=collection_work_key,
        )
        claim = self.repository.claim_work_item(
            connection,
            execution_scope="ECONOMIC_COLLECT",
            executor=_COLLECT_EXECUTOR,
            work_key_prefix=collection_work_key,
        )
        if claim is None or claim.work_item_id != work_id:
            raise RuntimeError("collector could not obtain its deterministic work item")

        try:
            captured = self.source_client.fetch(locator)
        except SourceFetchError as exc:
            if exc.kind is SourceFailureKind.RETRYABLE:
                self.repository.retry_claim(
                    connection,
                    claim,
                    reason_code="SOURCE_RETRYABLE",
                    retry_after_seconds=60,
                )
                return CollectionResult(
                    run_id=run_id,
                    work_item_id=work_id,
                    artifact_id=None,
                    outcome="RETRY_SCHEDULED",
                    promotion_status="NOT_APPLICABLE",
                    blocked_reason_code="SOURCE_RETRYABLE",
                )
            if exc.kind is SourceFailureKind.NOT_FOUND:
                self.repository.terminalize_claim(
                    connection,
                    claim,
                    outcome="DATA_NOT_AVAILABLE",
                    reason_code="SOURCE_NOT_FOUND",
                )
                self.repository.finalize_run_if_complete(connection, run_id)
                return CollectionResult(
                    run_id=run_id,
                    work_item_id=work_id,
                    artifact_id=None,
                    outcome="DATA_NOT_AVAILABLE",
                    promotion_status="NOT_APPLICABLE",
                    blocked_reason_code="SOURCE_NOT_FOUND",
                )
            self.repository.terminalize_claim(
                connection,
                claim,
                outcome="QUARANTINED",
                reason_code="SOURCE_CONTRACT_FAILURE",
            )
            self.repository.finalize_run_if_complete(connection, run_id)
            return CollectionResult(
                run_id=run_id,
                work_item_id=work_id,
                artifact_id=None,
                outcome="QUARANTINED",
                promotion_status="NOT_APPLICABLE",
                blocked_reason_code="SOURCE_CONTRACT_FAILURE",
            )

        artifact_id = uuid4()
        digest = hashlib.sha256(captured.body).hexdigest()
        stored = self.artifact_store.put(artifact_id, captured.body, digest)
        database_committed = False
        try:
            with connection.transaction():
                db_artifact_id = self.repository.record_source_artifact(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    source_code="BLS",
                    artifact_contract_kind=locator.artifact_contract_kind,
                    source_contract_version=self.source_contract.contract_version,
                    locator_key=captured.locator_key,
                    retrieval_url=captured.final_url,
                    content_sha256=digest,
                    content_type=captured.headers.get(
                        "content-type",
                        "application/octet-stream",
                    ).split(";", 1)[0].strip().lower(),
                    captured_at=captured.captured_at,
                    storage_uri=stored.storage_uri,
                    storage_generation=stored.storage_generation,
                )
                if db_artifact_id != artifact_id:
                    raise RuntimeError("artifact identity did not converge")
                self.repository.record_artifact_promotion_targets(
                    connection,
                    claim,
                    artifact_id=artifact_id,
                    reference_months=reference_months,
                )
                self.repository.terminalize_claim_in_transaction(
                    connection,
                    claim,
                    outcome="SUCCEEDED",
                )
            database_committed = True
        finally:
            if not database_committed:
                self.artifact_store.delete_uncommitted(stored)

        promotion = self._schedule_promotions(
            connection,
            artifact_id=artifact_id,
            artifact_contract_kind=locator.artifact_contract_kind,
            source_contract_version=self.source_contract.contract_version,
            run_mode=run_mode,
        )
        self.repository.finalize_run_if_complete(connection, run_id)
        return CollectionResult(
            run_id=run_id,
            work_item_id=work_id,
            artifact_id=artifact_id,
            outcome="SUCCEEDED",
            promotion_status=promotion.status,
            promotion_work_ids=promotion.work_item_ids,
            blocked_reason_code=promotion.blocked_reason_code,
            deferred_capability_ids=promotion.deferred_capability_ids,
        )

    def reconcile_promotions(
        self,
        connection: Any,
        *,
        only_artifact_id: UUID | None = None,
        only_extractor_version: str | None = None,
    ) -> list[CollectionResult]:
        if (only_artifact_id is None) != (only_extractor_version is None):
            raise ValueError(
                "replay reconciliation requires artifact and extractor together"
            )
        if only_artifact_id is None:
            rows = connection.execute(
                """
                SELECT a.artifact_id, a.artifact_contract_kind,
                       a.source_contract_version, w.run_id
                  FROM source_artifacts a
                  JOIN ingestion_attempts att
                    ON att.attempt_id = a.created_by_attempt_id
                  JOIN ingestion_work_items w
                    ON w.work_item_id = att.work_item_id
                  JOIN ingestion_runs r
                    ON r.run_id = w.run_id
                   AND r.execution_scope = w.execution_scope
                   AND r.data_domain = w.data_domain
                 WHERE a.data_domain='ECONOMIC'
                   AND a.source_code='BLS'
                   AND r.job_type='CPI_W1_COLLECT'
                   AND r.job_contract_version=%s
                 ORDER BY a.created_at, a.artifact_id
                """,
                (_COLLECT_JOB_CONTRACT,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT a.artifact_id, a.artifact_contract_kind,
                       a.source_contract_version, w.run_id
                  FROM source_artifacts a
                  JOIN ingestion_attempts att
                    ON att.attempt_id = a.created_by_attempt_id
                  JOIN ingestion_work_items w
                    ON w.work_item_id = att.work_item_id
                  JOIN ingestion_runs r
                    ON r.run_id = w.run_id
                   AND r.execution_scope = w.execution_scope
                   AND r.data_domain = w.data_domain
                 WHERE a.artifact_id=%s
                   AND a.data_domain='ECONOMIC'
                   AND a.source_code='BLS'
                   AND r.job_type='CPI_W1_COLLECT'
                   AND r.job_contract_version=%s
                """,
                (only_artifact_id, _COLLECT_JOB_CONTRACT),
            ).fetchall()
            if not rows:
                raise KeyError(f"CPI source artifact not found: {only_artifact_id}")
        results: list[CollectionResult] = []
        for (
            artifact_id,
            artifact_contract_kind,
            source_contract_version,
            capture_run_id,
        ) in rows:
            promotion = self._schedule_promotions(
                connection,
                artifact_id=artifact_id,
                artifact_contract_kind=artifact_contract_kind,
                source_contract_version=source_contract_version,
                run_mode=("REPLAY" if only_artifact_id is not None else "BACKFILL"),
                replay_of_run_id=(
                    capture_run_id if only_artifact_id is not None else None
                ),
                only_extractor_version=only_extractor_version,
            )
            if only_extractor_version is not None and promotion.status == "NO_PROMOTION_CONTRACT":
                raise ValueError(
                    "replay extractor does not match artifact contract kind"
                )
            results.append(
                CollectionResult(
                    run_id=None,
                    work_item_id=None,
                    artifact_id=artifact_id,
                    outcome="RECONCILED",
                    promotion_status=promotion.status,
                    promotion_work_ids=promotion.work_item_ids,
                    blocked_reason_code=promotion.blocked_reason_code,
                    deferred_capability_ids=promotion.deferred_capability_ids,
                )
            )

        run_rows = connection.execute(
            """
            SELECT run_id
              FROM ingestion_runs
             WHERE data_domain='ECONOMIC'
               AND job_type IN ('CPI_W1_COLLECT', 'CPI_W1_PROMOTE')
               AND job_contract_version IN (%s, %s)
               AND state <> 'TERMINAL'
            """,
            (_COLLECT_JOB_CONTRACT, _PROMOTE_JOB_CONTRACT),
        ).fetchall()
        for (run_id,) in run_rows:
            self.repository.finalize_run_if_complete(connection, run_id)
        return results


def _parse_reference_month(value: str) -> date:
    if len(value) != 7 or value[4] != "-":
        raise SystemExit("reference month must use YYYY-MM")
    try:
        return date.fromisoformat(f"{value}-01")
    except ValueError as exc:
        raise SystemExit("reference month must use YYYY-MM") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Controlled CPI W1 collector")
    parser.add_argument("--mode", choices=("live", "backfill", "replay"), required=True)
    parser.add_argument("--locator", default="CURRENT_CPI_RELEASE_HTML")
    parser.add_argument("--trigger-idempotency-key")
    parser.add_argument("--reference-month", action="append", default=[])
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reconcile-promotions", action="store_true")
    parser.add_argument("--replay-artifact-id")
    parser.add_argument("--replay-extractor-version")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.mode == "replay":
        if not args.replay_artifact_id or not args.replay_extractor_version:
            raise SystemExit(
                "replay requires --replay-artifact-id and --replay-extractor-version"
            )
        if not args.reconcile_promotions:
            raise SystemExit("replay mode is reconciliation-only in W1")
        if args.reference_month:
            raise SystemExit("replay uses persisted artifact targets")
    elif args.replay_artifact_id or args.replay_extractor_version:
        raise SystemExit("replay arguments are forbidden outside replay mode")
    elif args.reconcile_promotions and args.reference_month:
        raise SystemExit("reconciliation uses persisted artifact targets")
    elif not args.reconcile_promotions and not args.reference_month:
        raise SystemExit("live/backfill collection requires --reference-month")
    parsed_months = tuple(_parse_reference_month(value) for value in args.reference_month)
    if len(parsed_months) != len(set(parsed_months)):
        raise SystemExit("reference months must be unique")
    if not args.reconcile_promotions and not args.trigger_idempotency_key and not args.dry_run:
        raise SystemExit("collection requires --trigger-idempotency-key")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    contract = BlsCpiSourceContract.from_json(SOURCE_CONTRACT_PATH)
    capability_registry = PromotionCapabilityRegistry.from_json(
        CAPABILITY_REGISTRY_PATH
    )
    gate = CpiW1ExtractorReleaseGate.from_json(
        RELEASE_GATE_PATH,
        capability_registry_path=CAPABILITY_REGISTRY_PATH,
        evidence_snapshot_path=EVIDENCE_SNAPSHOT_PATH,
    )
    repository = CpiW1Repository()
    store = FilesystemArtifactStore(args.artifact_root)

    if args.dry_run:
        if args.locator not in contract.locators:
            raise SystemExit(f"unknown CPI source locator: {args.locator}")
        return 0

    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as connection:
        orchestrator = CpiW1CollectorOrchestrator(
            repository=repository,
            source_contract=contract,
            capability_registry=capability_registry,
            release_gate=gate,
            artifact_store=store,
        )
        if args.reconcile_promotions:
            orchestrator.reconcile_promotions(
                connection,
                only_artifact_id=(
                    UUID(args.replay_artifact_id)
                    if args.mode == "replay"
                    else None
                ),
                only_extractor_version=(
                    args.replay_extractor_version
                    if args.mode == "replay"
                    else None
                ),
            )
            return 0
        orchestrator.collect_locator(
            connection,
            locator_key=args.locator,
            run_mode=args.mode.upper(),
            trigger_idempotency_key=args.trigger_idempotency_key,
            reference_months=tuple(
                _parse_reference_month(value) for value in args.reference_month
            ),
            dry_run=False,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
