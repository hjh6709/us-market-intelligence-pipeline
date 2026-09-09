# Economic Event Intelligence & Strategy Validation Platform contract

Status: canonical target contract. Baseline revision: `63633a50c85c88e507be458067ecf6706220f813` (2026-09-10).

This document defines the approved destination. It does not claim that every component is implemented. For implementation truth, read [current-system.md](current-system.md) and [current-vs-target.md](../engineering/current-vs-target.md). Dated evidence remains true only for the run it records.

Configuration boundaries are owned by [the configuration contract](../configuration/README.md). The additive foundation decision is recorded in [ADR 0001](../adr/0001-additive-event-session-foundation.md).

## Product identity and boundaries

The platform preserves economic release facts, aligns them to verified U.S. trading sessions, measures market reactions, validates explicit strategy hypotheses and serves reproducible evidence. The independent raw-SIP plane validates replay, deduplication and recovery; it is not the source of the 202×10 research dataset.

Research output never submits an order. A Paper experiment must be created and explicitly approved before Alpaca Paper can receive an order. Live brokerage execution is outside the target.

## Target flow

```text
official release observations + point-in-time consensus
  -> derived surprise with lineage
  -> verified trading-session plan and event markers
  + canonical Alpaca SIP 1m/1d market data
  -> collection / market / coverage / eligibility quality axes
  -> reaction metrics, regimes, comparables, hypotheses, simulations
  -> PostgreSQL curated records with provenance
  -> FastAPI + browser views
  -> explicit Paper experiment (optional, isolated, Paper only)

immutable raw SIP archive -> Kafka bounded replay -> Spark validation
  -> validated 1m bars and run evidence -> PostgreSQL
```

Interactive target diagram: [target-platform.html](../diagrams/target-platform.html). Its nodes tagged `CURRENT`, `EVOLVING`, or `P1 FOUNDATION` are still governed by the current/target matrix, not by visual proximity.

## Hard invariants

1. Official actual values, consensus snapshots and derived surprise are separate records with separate source and observation times.
2. Release observations are append-only by revision identity; corrections do not overwrite history.
3. All market windows come from a verified exchange calendar and explicit event markers. Calendar-day offsets are not trading-session offsets.
4. `collection_status`, `market_status`, `coverage_status`, and `analysis_eligibility` are distinct axes. A successful empty response is not automatically a failed collection.
5. Reaction metrics declare anchor, interval, price field, session policy and metric version. Legacy `PRE_60M`/`POST_*` outputs remain legacy baselines.
6. Every derived record carries source, feed, algorithm/version and run lineage sufficient to reproduce it.
7. Raw provider payloads and raw trade archives are immutable; Kafka is transport, not source of truth.
8. PostgreSQL curated writes use deterministic business keys. This is not a blanket exactly-once guarantee.
9. Research hypotheses, simulations and Paper experiments have different identifiers and lifecycles.
10. Paper submission requires explicit user approval, a Paper-only endpoint and reconciliation. Research signals cannot call it.

## Documentation precedence

1. Executable schema, code and tests at the referenced revision.
2. This target contract plus the topic contracts linked from [the documentation hub](../README.md).
3. [Current implementation](current-system.md) and [current-versus-target matrix](../engineering/current-vs-target.md).
4. Dated evidence, which proves only the captured run.
5. Superseded design and presentation documents, which are historical context only.
