# Product API contracts

Status: Phase 1 contract, audited 2026-09-09.

All product endpoints use `/api/v1`. Decimal values remain JSON strings where
binary floating-point conversion could change stored meaning. Timestamps are UTC
RFC3339 strings. Lists have deterministic ordering and explicit limits. Errors use
`{"detail": "..."}` without database, broker, or credential details.

## Shared provenance and quality contract

Research and aggregate responses include a `provenance` object when versioned data
is involved:

```json
{
  "source": "alpaca",
  "feed": "sip",
  "analysis_version": "multi_event_sip_v1",
  "strategy_name": "pre60_momentum_post60",
  "strategy_version": "v1"
}
```

Collection and coverage are separate fields. `PARTIAL` observed coverage never
changes a completed provider request into a failed request. API status vocabulary:

- collection: `COMPLETE`, `PARTIAL`, `FAILED` as produced by collection code;
- observed quality: `COMPLETE`, `PARTIAL`, `NO_MARKET_DATA`, and existing bounded
  unavailable values;
- check: `PASS`, `WARN`, `FAIL`;
- alert: `NONE`, `OPEN`, `RESOLVED`.

The API never computes a missing operational count from unrelated table counts.
Unknown/unrecorded values are `null`, not zero.

## Overview

`GET /api/v1/overview`

Returns dataset scope, event-type counts, supported symbols, recent events, the
fixed baseline summary, and a compact latest-pipeline health summary. Each metric
declares a semantic unit such as `releases`, `event_symbol_intervals`, or
`selected_event_bar_rows`; unlike units are never summed.

## Research

Existing compatible routes remain supported:

- `GET /api/v1/events`
- `GET /api/v1/events/{event_id}/symbols`
- `GET /api/v1/events/{event_id}/symbols/{symbol}`
- `GET /api/v1/events/{event_id}/symbols/{symbol}/bars?timeframe=1m|3m|5m`
- `GET /api/v1/strategy/summary`

New product routes:

- `GET /api/v1/research/historical?event_type=&symbol=&window=` compares stored
  impact rows for the same event type and symbol.
- `GET /api/v1/research/cross-asset?event_id=&window=&metric=return|relative`
  compares the ten supported assets from stored impact rows.
- `GET /api/v1/research/quality?event_id=&symbol=` returns collection checks and
  observed/derived/daily quality without merging their meanings.

Date filters on event lists are UTC calendar dates and are implemented as explicit
`[start,end)` UTC timestamp bounds.

## Pipelines

- `GET /api/v1/pipelines/overview` returns latest run plus bounded recent runs and
  aggregates only counts that exist in `pipeline_*` rows.
- `GET /api/v1/pipelines/runs?dag_id=&status=&limit=&offset=` returns deterministic
  newest-first summaries.
- `GET /api/v1/pipelines/runs/{pipeline_run_id}` returns run metadata, status
  groups, bounded work items, checks, attempts, errors, and duration.
  Query parameters are `limit` (default 100, range 1–500), `work_offset`
  (default 0), and `check_offset` (default 0). Both offsets must be nonnegative.
  `work_items` and `checks` stay arrays for compatibility; the corresponding
  `work_items_page` and `checks_page` objects each declare `total`, `limit`,
  `offset`, and `has_more`. A 2,020-item run therefore returns at most 100 items
  by default and explicitly reports the remaining pages. Offsets beyond the
  total return empty arrays without changing that total. Unknown runs return 404.
- `GET /api/v1/pipelines/quality?pipeline_run_id=` returns separate collection,
  observed coverage, derived coverage, daily coverage, duplicate, and PIT checks.
- `GET /api/v1/pipelines/lineage` returns static, code-reviewed project-level nodes
  and edges plus dataset identities. It does not claim row-level lineage.

`WARN` checks do not increment failure counts. A sparse successful collection is
shown as `SUCCEEDED` plus quality warnings. A currently running record may be shown
as stale based on age, but the API must not mutate it during a read.

## Paper Execution

Paper routes are a separate journey and never accept event IDs, research signals,
or strategy results.

- `GET /api/v1/paper/account` performs safe account and clock reads.
- `GET /api/v1/paper/orders` reads the bounded local journal using the server's
  pinned `ALPACA_PAPER_ACCOUNT_ID`, without constructing a broker client or
  requesting the remote account. A broker outage does not hide local orders.
  Remote operations verify that credentials identify the pinned account before
  acting. Missing local scope is a configuration error, not an empty journal.
- `POST /api/v1/paper/orders/review` validates an `OrderIntent` without broker POST.
- `POST /api/v1/paper/orders` requires the reviewed intent, an explicit confirmation,
  and server-side Paper UI enablement; it delegates to the existing journal-first
  service.
- `POST /api/v1/paper/orders/{request_id}/reconcile` performs GET lookup only.
- `POST /api/v1/paper/orders/{request_id}/cancel` requires explicit confirmation.
- `POST /api/v1/paper/recovery` runs bounded GET-only recovery and exposes the
  opening-baseline limitation.

The server never accepts broker credentials from a browser. Credentials come only
from server configuration. Live endpoints, SELL, market orders, extended hours,
quantity above 10, notional above USD 1,000, and automatic POST retry remain
unsupported.

## Compatibility and implementation rule

An endpoint is documented as available in the root README only after route,
repository/service behavior, failure behavior, and response schema tests pass.
This document is the contract for Phases 2–4, not evidence that unimplemented routes
already work.
