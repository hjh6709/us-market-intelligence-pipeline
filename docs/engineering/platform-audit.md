# Platform audit — 2026-09-09

Audited baseline: `ef8de1184d2b4f9080799a4ea8bde4470a49d999`

This audit treats the repository as the **U.S. Economic Event Market Intelligence
Platform**. Its primary product is a reproducible data platform. Research and the
isolated Alpaca Paper sandbox are downstream consumers; neither is allowed to
change the meaning of stored data.

## Executive result

The implemented foundation is credible: source-specific ingestion, UTC event
alignment, point-in-time macro selection, explicit PostgreSQL business keys,
idempotent upserts, bounded Airflow work, Kafka/Spark replay validation, durable
run checks, a read-only research API, and a paper-only order journal all have code
and tests. The main completion gap is product access to that foundation. Pipeline
telemetry and paper operations exist in PostgreSQL/Python but are not yet exposed
through stable serving APIs or the web application.

The most important invariant already implemented is:

```text
provider collection integrity != observed price-bar coverage
```

A successful sparse response is a successful collection with a coverage warning,
not a failed collection. No UI or API may collapse these dimensions.

## A. Data sources

| Source | Producer and timestamp semantics | Consumer | Stored destination |
| --- | --- | --- | --- |
| BLS CPI / Employment manifests | Repository manifests preserve official New York wall time and normalized UTC `released_at` | economic-event ingestion and Airflow selection | `economic_events` |
| BEA PCE manifest | Official release timestamp, normalized to UTC | same | `economic_events` |
| Federal Reserve FOMC manifest | Official statement timestamp, normalized to UTC | same | `economic_events` |
| Alpaca historical stock bars | Explicit `[start,end)` RFC3339 bounds, `feed=sip` or `iex`, paginated until a null token | market-context backfill | `market_bars` with `source=alpaca` |
| Archived Alpaca SIP trades | Provider trade time in a deterministic Parquet manifest | Kafka replay, then Spark event-time validation/aggregation | `market_bars` with replay source |
| FRED/ALFRED | Observation date plus real-time vintage interval; values must have been known at release time | macro-context backfill | `macro_observations`, `macro_event_contexts` |

The provider-bar research path and archived-trade validation path are separate.
The latter proves replay, partitioning, streaming aggregation, idempotency, and
recovery for a bounded dataset; it did not create the full 202 × 10 research set.

## B. Business keys and retry semantics

| Table | Enforced key | Write/retry behavior |
| --- | --- | --- |
| `economic_events` | PK `economic_event_id`; unique event type, reference period, release time, source | deterministic upsert |
| `market_bars` | symbol, start, timeframe, source, feed | upsert; a rerun updates the same semantic bar |
| `macro_event_contexts` | event ID, series ID | upsert of the release-time context |
| `macro_event_impacts` | event, symbol, source, feed, window, analysis version | deterministic ID and versioned upsert |
| `event_strategy_results` | event, symbol, strategy name, strategy version | deterministic ID and versioned upsert |
| `pipeline_runs` | pipeline run ID | run registration is idempotent for the same ID |
| `pipeline_work_items` | run, event, symbol, stage | retry updates attempts/status rather than adding a duplicate |
| `pipeline_run_checks` | run, event, symbol, stage, check | later success can resolve an earlier alert |
| `paper_order_intents` | scope, request ID; unique client order ID | journal is committed before POST; reused request IDs cannot mutate intent or cause a second POST |

Risk: analysis identities are currently duplicated as constants in analytics and
serving modules. They are pinned (not arbitrary latest), but must be surfaced in
API metadata and kept in one contract to prevent silent version drift.

## C. Time correctness

- Economic release manifests convert named local time zones to UTC.
- Market bars require timezone-aware timestamps and minute bars must be on-grid.
- The provider collection request is `[T-60,T+121)`, covering 181 candidate minute
  timestamps. The research chart contract is `[T-60,T+120)`, a 180-minute display.
- Analytical windows are half-open and retain the existing 90% coverage policy.
- Daily context selects observed trading sessions rather than calendar-day filling.
- `data_cutoff` is timezone-aware and future bounds are clamped to provider
  availability; unavailable future sessions remain explicitly classified.
- ALFRED selection rejects observations first known after the release.

Risk: the existing event-list query casts `TIMESTAMPTZ` to `date` without fixing
the database session timezone. The public API must define its date filters as UTC
calendar bounds and query explicit timestamp ranges.

## D. Data-quality semantics

The current market-context result carries collection status separately from 1m,
daily, derived 3m/5m, and overall observed coverage. `fetch_all_bars()` terminates
only after the provider returns no next-page token; a remaining token at
`max_pages` raises instead of silently truncating. HTTP, timeout, malformed JSON,
bad symbols, invalid OHLC, and off-grid timestamps surface as errors. A successful
empty response remains distinguishable from transport failure by completion
metadata and coverage classification.

Derived bars use only observed source bars. They retain
`source_bar_count/expected_bar_count` and never forward-fill prices. The analysis
layer retains its independent 90% rule.

Required presentation mapping:

| Condition | Work/run meaning | Quality meaning |
| --- | --- | --- |
| request and pagination completed; dense bars | success | pass |
| request and pagination completed; sparse bars | success | warn/partial |
| market closed or future unavailable | data not available | warn |
| transport, truncation, malformed data, or persistence failure | failure | fail/open alert |

## E. Airflow

The market-context DAG validates typed parameters, uses bounded Alpaca/PostgreSQL
pools, maps one task per release while batching symbols, records per-symbol work
items/checks, retries twice, verifies accepted counts and open alerts, and closes a
successful run only after verification. Config hash, cutoff, code version, attempt
count, errors, and alert resolution are durable.

Risk: failures outside the collection task's explicit exception path can leave a
registered run in `RUNNING`. A DAG-level terminal callback or reconciliation rule
is required before the UI can imply every registered run is terminal.

## F. Raw Kafka/Spark validation path

The archived SIP record is wrapped in a canonical envelope. Its deterministic
identity includes provider trade ID, exchange, symbol, and event time, avoiding
the previously observed cross-exchange false collision. Kafka delivery offsets
and counts are bounded; Spark uses event time, a watermark, deduplication, sale
condition policy, and final 1m upserts.

Verified historical evidence must remain scoped:

- 7,360,804 archived SIP trades in the dedicated load/recovery run;
- corrected identity collision count 0 and PostgreSQL duplicate count 0;
- published = consumed = Spark input for that validated run;
- it is not a claim of full 202 × 10 raw-trade ingestion.

## G. Kafka partitioning

The v1 symbol-only key produced an approximately 97.5% maximum partition share.
The v2 event/release/symbol/15-minute-segment key produced approximately 33.9% on
the separate 118,118-event replay test. The latter percentage must not be attached
to the 7.36m run without new evidence.

## H. Failure and recovery

- Spark heap pressure was corrected by disk-backed persistence and explicit
  release of reused intermediate data.
- PostgreSQL outage recovery reruns deterministic input and relies on business-key
  upserts; it is not a claim of database high availability.
- The API 503 drill uses a local controlled endpoint; it is not a real Alpaca
  outage. Failure/open-alert and success/resolved transitions are persisted.
- Paper POST has no automatic retry. A journal row is durable before the broker
  boundary, and uncertain outcomes require GET reconciliation.

## I. Serving traceability

Current research flow is traceable as follows:

```text
economic_events + market_bars + macro_event_contexts
  -> macro_event_impacts (multi_event_sip_v1)
  -> event_strategy_results (pre60_momentum_post60/v1)
  -> PostgresServingRepository
  -> ServingService
  -> FastAPI JSON
  -> dashboard
```

The existing API pins source/feed and versions, but does not return those identities
as first-class metadata. Pipeline and paper repositories/routes are absent. Phase 1
contracts require every aggregate to declare its source table, semantic unit, and
version/filter identity; later UI work may only display returned values.

## J. Paper isolation

The broker URL is fixed to `https://paper-api.alpaca.markets`; redirects are off.
Only BUY, limit, DAY, regular-hours orders are emitted. Quantity is 1–10 and notional
is at most USD 1,000. A new submit/cancel requires explicit enablement. Research
objects are not accepted by the paper service. Recovery reads journal entries and
uses GET only; it cannot submit. Reusing a request ID with another intent fails.

The existing position comparison intentionally reports `BASELINE_REQUIRED` and
`position_reconciliation_verified=false`. The web product must not manufacture a
risk-control or reconciliation guarantee around it.

## Findings and disposition

### Completion update — 2026-09-09

The audit findings below describe the inspected baseline. Subsequent commits on the completion branch now centralize and expose source/version identities, use explicit UTC date bounds, provide Overview/Research/Pipelines/Paper APIs and pages, and expose manual Paper review, submission, status refresh and cancellation. The remaining correctness risk is stale `RUNNING` Airflow terminal reconciliation; OpenLineage remains optional. This update does not rewrite the historical baseline findings.

### Correctness risks

1. Centralize and expose analysis/source identities to prevent serving drift.
2. Replace timezone-dependent date casts with explicit UTC timestamp bounds.
3. Make abandoned `RUNNING` pipeline runs visibly stale and define terminal
   reconciliation before claiming universal finalization.

### Stale semantics

1. Course-era `COMPLETE` records must be labelled historical where the old coverage
   contract applied.
2. Root planning documents still call implemented capabilities unimplemented.
3. Evidence generations contain different daily selection totals; the portfolio
   surface must use the current canonical evidence and retain old numbers only in
   archived evidence context.

### Product gaps

1. No Overview product API/page.
2. No pipeline telemetry API/page.
3. No historical, cross-asset, or research-quality API/page.
4. No web access to the safe Paper subsystem.

### Optional enhancements

OpenLineage may be added only after the core application and documentation pass.
Marquez is optional and must never be required by the core compose stack.
Prometheus/Grafana and additional data platforms are outside this deadline.
