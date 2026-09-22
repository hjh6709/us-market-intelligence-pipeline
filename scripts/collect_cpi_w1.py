#!/usr/bin/env python3
"""Controlled CPI W1 collection/orchestration entry point.

This module captures official bytes and schedules only release-gate-eligible
promotion work. It does not execute canonical promotion or legacy CPI upserts.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg

from src.cpi_w1_artifacts import FilesystemArtifactStore
from src.cpi_w1_contracts import PromotionFamily
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
DEFAULT_ARTIFACT_ROOT = ROOT / ".cpi-w1-artifacts"

_SOURCE_REVISION = "cpi-w1-source-v1"
_COLLECT_JOB_CONTRACT = "cpi-w1-collector-v1"
_PROMOTE_JOB_CONTRACT = "cpi-w1-promoter-v1"

_EXTRACTOR_BY_ARTIFACT_KIND = {
    "CPI_SCHEDULE_HTML": "bls-cpi-schedule-html-v1",
    "BLS_GLOBAL_ICS": "bls-cpi-global-ics-v1",
    "BLS_REVISED_RELEASE_DATES_HTML": "bls-cpi-revised-release-dates-v1",
    "CPI_RELEASE_HTML": "bls-cpi-release-html-v1",
    "CPI_TABLE1_XLSX": "bls-cpi-table1-xlsx-v1",
    "CPI_CORRECTION_HTML": "bls-cpi-correction-html-v1",
}

_FAMILIES_BY_ARTIFACT_KIND = {
    "CPI_SCHEDULE_HTML": (PromotionFamily.CPI_SCHEDULE_ASSERTION_PROMOTE,),
    "BLS_GLOBAL_ICS": (PromotionFamily.CPI_SCHEDULE_ASSERTION_PROMOTE,),
    "BLS_REVISED_RELEASE_DATES_HTML": (
        PromotionFamily.CPI_SCHEDULE_ASSERTION_PROMOTE,
    ),
    "CPI_RELEASE_HTML": (
        PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
        PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
    ),
    "CPI_TABLE1_XLSX": (
        PromotionFamily.CPI_CORROBORATING_REPRESENTATION_PROMOTE,
        PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
    ),
    "CPI_CORRECTION_HTML": (
        PromotionFamily.CPI_CORRECTION_NOTICE_PROMOTE,
        PromotionFamily.CPI_CORRECTION_OBSERVATION_PROMOTE,
    ),
}


@dataclass(frozen=True)
class CollectionResult:
    run_id: UUID | None
    work_item_id: UUID | None
    artifact_id: UUID | None
    outcome: str
    promotion_status: str
    promotion_work_ids: tuple[UUID, ...] = ()
    blocked_reason_code: str | None = None


class CpiW1CollectorOrchestrator:
    def __init__(
        self,
        *,
        repository: CpiW1Repository,
        source_contract: BlsCpiSourceContract,
        release_gate: CpiW1ExtractorReleaseGate,
        artifact_store: FilesystemArtifactStore,
        source_client: BlsCpiSourceClient | None = None,
    ) -> None:
        self.repository = repository
        self.source_contract = source_contract
        self.release_gate = release_gate
        self.artifact_store = artifact_store
        self.source_client = source_client or BlsCpiSourceClient(source_contract)

    @staticmethod
    def _fingerprint_file(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _config_fingerprint(self) -> str:
        combined = (
            self._fingerprint_file(SOURCE_CONTRACT_PATH)
            + ":"
            + self._fingerprint_file(RELEASE_GATE_PATH)
        )
        return hashlib.sha256(combined.encode("ascii")).hexdigest()

    def _schedule_promotions(
        self,
        connection: Any,
        *,
        artifact_id: UUID,
        artifact_contract_kind: str,
        run_mode: str,
    ) -> tuple[str, tuple[UUID, ...], str | None]:
        extractor = _EXTRACTOR_BY_ARTIFACT_KIND.get(artifact_contract_kind)
        families = _FAMILIES_BY_ARTIFACT_KIND.get(artifact_contract_kind)
        if extractor is None or families is None:
            return "NO_PROMOTION_CONTRACT", (), "EXTRACTOR_NOT_REVIEWED"

        decision = self.release_gate.decision(extractor)
        if not decision.eligible:
            self.repository.record_promotion_deferred(
                connection,
                artifact_id=artifact_id,
                extractor_contract_version=extractor,
                reason_code=decision.reason_code or "EXTRACTOR_NOT_REVIEWED",
                review_ref=decision.review_ref or "NO_REVIEW_REFERENCE",
            )
            return "BLOCKED_BY_RELEASE_GATE", (), decision.reason_code

        promote_run_id = self.repository.create_run(
            connection,
            execution_scope="ECONOMIC_PROMOTE",
            job_type="CPI_W1_PROMOTE",
            trigger_type="ARTIFACT_HANDOFF",
            run_mode=run_mode,
            trigger_idempotency_key=f"artifact:{artifact_id}:{extractor}",
            scheduled_for=None,
            replay_of_run_id=None,
            source_revision=_SOURCE_REVISION,
            workload_artifact_digest=str(artifact_id),
            job_contract_version=_PROMOTE_JOB_CONTRACT,
            config_fingerprint=self._config_fingerprint(),
        )
        work_ids: list[UUID] = []
        for family in families:
            work_id = self.repository.create_work_item(
                connection,
                run_id=promote_run_id,
                execution_scope="ECONOMIC_PROMOTE",
                work_key=promotion_work_key(family, artifact_id, extractor),
                input_artifact_id=artifact_id,
            )
            work_ids.append(work_id)
        return "SCHEDULED", tuple(work_ids), None

    def collect_locator(
        self,
        connection: Any,
        *,
        locator_key: str,
        run_mode: str,
        trigger_idempotency_key: str,
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
                self.repository.terminalize_claim_in_transaction(
                    connection,
                    claim,
                    outcome="SUCCEEDED",
                )
            database_committed = True
        finally:
            if not database_committed:
                self.artifact_store.delete_uncommitted(stored)

        promotion_status, promotion_work_ids, blocked_reason = self._schedule_promotions(
            connection,
            artifact_id=artifact_id,
            artifact_contract_kind=locator.artifact_contract_kind,
            run_mode=run_mode,
        )
        self.repository.finalize_run_if_complete(connection, run_id)
        return CollectionResult(
            run_id=run_id,
            work_item_id=work_id,
            artifact_id=artifact_id,
            outcome="SUCCEEDED",
            promotion_status=promotion_status,
            promotion_work_ids=promotion_work_ids,
            blocked_reason_code=blocked_reason,
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
                SELECT artifact_id, artifact_contract_kind
                  FROM source_artifacts
                 WHERE data_domain='ECONOMIC'
                   AND source_code='BLS'
                 ORDER BY created_at, artifact_id
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT artifact_id, artifact_contract_kind
                  FROM source_artifacts
                 WHERE artifact_id=%s
                   AND data_domain='ECONOMIC'
                   AND source_code='BLS'
                """,
                (only_artifact_id,),
            ).fetchall()
            if not rows:
                raise KeyError(f"CPI source artifact not found: {only_artifact_id}")
        results: list[CollectionResult] = []
        for artifact_id, artifact_contract_kind in rows:
            expected_extractor = _EXTRACTOR_BY_ARTIFACT_KIND.get(
                artifact_contract_kind
            )
            if (
                only_extractor_version is not None
                and expected_extractor != only_extractor_version
            ):
                raise ValueError(
                    "replay extractor does not match artifact contract kind"
                )
            status, work_ids, reason = self._schedule_promotions(
                connection,
                artifact_id=artifact_id,
                artifact_contract_kind=artifact_contract_kind,
                run_mode="BACKFILL",
            )
            results.append(
                CollectionResult(
                    run_id=None,
                    work_item_id=None,
                    artifact_id=artifact_id,
                    outcome="RECONCILED",
                    promotion_status=status,
                    promotion_work_ids=work_ids,
                    blocked_reason_code=reason,
                )
            )

        run_rows = connection.execute(
            """
            SELECT run_id
              FROM ingestion_runs
             WHERE data_domain='ECONOMIC'
               AND state <> 'TERMINAL'
            """
        ).fetchall()
        for (run_id,) in run_rows:
            self.repository.finalize_run_if_complete(connection, run_id)
        return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Controlled CPI W1 collector")
    parser.add_argument("--mode", choices=("live", "backfill", "replay"), required=True)
    parser.add_argument("--locator", default="CURRENT_CPI_RELEASE_HTML")
    parser.add_argument("--trigger-idempotency-key")
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
    elif args.replay_artifact_id or args.replay_extractor_version:
        raise SystemExit("replay arguments are forbidden outside replay mode")
    if not args.reconcile_promotions and not args.trigger_idempotency_key and not args.dry_run:
        raise SystemExit("collection requires --trigger-idempotency-key")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    contract = BlsCpiSourceContract.from_json(SOURCE_CONTRACT_PATH)
    gate = CpiW1ExtractorReleaseGate.from_json(RELEASE_GATE_PATH)
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
            dry_run=False,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
