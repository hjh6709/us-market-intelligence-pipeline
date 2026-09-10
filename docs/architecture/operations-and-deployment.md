# Operational, storage and deployment contract

Status: target contract with explicit current gaps. None of the three target operational DAGs below is implemented by migration 009.

## Three operational Airflow DAGs

`event_catalog_refresh_pipeline` owns official schedule refresh and change detection, official numeric observations, pre-release external consensus snapshots, durable event markers and verified calendar refresh.

`market_intelligence_incremental_pipeline` owns per-metric maturity discovery, provider research-bar collection, quality decisions, PIT macro features/regimes, reaction metrics, comparables and freshness.

`paper_execution_reconciliation_pipeline` owns approved active experiments, order/fill/position reconciliation, uncertain POST resolution, exit maturity and Paper outcomes. Broker POST is never a normal Airflow retry target.

Telemetry separates `PipelineRunOutcome`, `WorkItemOutcome`, `reason_code/reason_detail`, counts and contract versions. Maturity is per metric, so `POST_5M` can be ready while S+7 remains `NOT_YET_MATURE`. Freshness follows source availability; provider safety lag is `DATA_NOT_AVAILABLE`, not provider failure or coverage loss.

## Storage and raw validation

Provider-aggregated research bars are curated in PostgreSQL `market_bars`. Raw SIP trades belong in immutable Parquet/object storage. Kafka is bounded transport/replay. Spark is an on-demand validation/reconstruction engine and writes only `validation_reconstructed_bars` plus run evidence.

Target reusable raw layout:

```text
raw/sip/trades/
  trading_date=YYYY-MM-DD/
    symbol=SPY/
      session=REGULAR/
        part-0000.parquet
```

Existing event-oriented raw archives remain immutable and readable; the 7.36M archive is not rewritten. Target validation workloads remain W1 announcement burst, W2 opening hour, W3 full session and W4 multi-session recovery. Raw-tick collection for all 202×10×3 sessions is not required merely to manufacture scale.

Migration 009 and the current Spark PostgreSQL sink implement an immutable `validation_runs` parent containing run ID, workload ID, processor version, checkpoint namespace, and optional source-manifest identity. Reconstructed bars are append-only: identical replay is idempotent and conflicting content under the same run/bar identity raises a determinism conflict. Streaming checkpoints are isolated by validation run and processor version. This is a local foundation; managed object storage and always-on workers remain target-only.

The migration serializes validation-run and reconstructed-bar decisions with transaction-scoped advisory locks. Concurrent identical requests return one durable fact; concurrent different bar content raises `VALIDATION_BAR_DETERMINISM_CONFLICT` rather than exposing a timing-dependent unique-key failure.

## Market-data validation

Current provider collection verifies requested interval/symbol and pagination termination. Stricter provider conflict handling remains target-only. Validation evidence is never updated to hide a raw-reconstruction conflict. Corporate-action policy for multi-session persistence is governed by [research-contracts.md](research-contracts.md).

## Serving and deployment

Current local FastAPI/browser serving remains implementation truth. It reads legacy events, provider research bars, legacy impacts/context and legacy strategy results. It does not consume raw-derived validation bars or the normalized foundation yet.

The target topology separates public read-only research resources, authenticated operational/Paper views and on-demand heavy validation workers, backed by managed PostgreSQL and immutable object storage. Authentication, authorization, secrets management, TLS, backups, observability and deployment automation remain target-only. No public production deployment is asserted.

## Paper experiments

Current Paper behavior is a guarded, manually approved Alpaca Paper order-intent journal. The target lineage is `strategy_hypothesis -> paper_experiment -> paper_order -> paper_fill -> paper_position/reconciliation -> paper_outcome`.

At approval, freeze hypothesis version, risk-policy version, planned parameters, market/session context, approval time and request identity. Client order identity is deterministic, such as `paper:{experiment_id}:entry:v1`. For an uncertain POST, journal intent first and reconcile by durable identity; never blind-retry POST.

Paper-only structural enforcement means a hard-coded/allowlisted Paper hostname, separate credential namespace, live-host rejection and tests proving a live endpoint cannot be selected. Research output never submits an order, and live execution remains out of scope.
