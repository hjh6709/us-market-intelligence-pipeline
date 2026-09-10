# Economic Event Intelligence & Strategy Validation Platform contract

Status: canonical target contract. Baseline revision: `63633a50c85c88e507be458067ecf6706220f813` (2026-09-10). Corrective artifact revision is recorded in the dated verification receipt after commit.

This document defines the approved destination. It does not claim that every component is implemented. For implementation truth, read [current-system.md](current-system.md) and [current-vs-target.md](../engineering/current-vs-target.md). Dated evidence remains true only for the run it records.

Configuration boundaries are owned by [the configuration contract](../configuration/README.md). The additive foundation decision is recorded in [ADR 0001](../adr/0001-additive-event-session-foundation.md).

## Product identity and boundaries

The platform preserves official economic release facts, keeps external consensus snapshots separate, derives point-in-time-safe surprises, aligns events to verified U.S. trading sessions, measures market reactions, validates explicit strategy hypotheses and serves reproducible evidence. The independent raw-SIP plane validates replay, deduplication and recovery; it is not the source of the 202×10 research dataset.

Research output never submits an order. A Paper experiment must be created and explicitly approved before Alpaca Paper can receive an order. Live brokerage execution is outside the target.

## Target flow

```text
official economic event -> official numeric observations
external consensus snapshots -> point-in-time consensus selector
  -> derived surprise with lineage
  -> verified trading-session plan and event markers
  + canonical Alpaca SIP 1m/1d market data
  -> collection / market / coverage / eligibility quality axes
  -> reaction metrics, regimes, comparables, hypotheses, simulations
  -> PostgreSQL curated records with provenance
  -> FastAPI + browser views
  -> explicit Paper experiment (optional, isolated, Paper only)

immutable raw SIP archive -> Kafka bounded replay -> Spark validation
  -> raw-derived validation bars + run evidence
  -> physically separate PostgreSQL validation storage
```

Interactive target diagram: [target-platform.html](../diagrams/target-platform.html). Its nodes tagged `CURRENT`, `EVOLVING`, or `P1 FOUNDATION` are still governed by the current/target matrix, not by visual proximity.

## Hard invariants

1. Official actual values and external consensus snapshots are separate records with source-appropriate timestamps. Provider display labels are mapped to canonical `observation_code` values and units.
2. An upcoming canonical event can exist with `scheduled_at`, nullable `released_at`, and an append-only lifecycle version. Legacy `economic_events` fields remain compatibility-only.
3. Release observations are append-only by deterministic `(event, observation_code, revision)` identity. `published_at`, `first_observed_at`, and `ingested_at` are not interchangeable.
4. Canonical event-time surprise uses the initial official observation and latest valid external consensus strictly before the event's primary marker, with identical units and verified arithmetic.
5. `S0` is the first regular trading session able to absorb the primary marker. Post-market and closed-day releases use the next session; FOMC `STATEMENT` is primary and `PRESS_CONFERENCE` is secondary.
6. Run outcome, work-item outcome, session/interval type, coverage, analysis eligibility, reason code, and per-metric maturity are distinct semantics.
7. Reaction metrics declare category, marker, endpoints, prices, clipping, tolerance, maturity, applicability and version. Legacy `PRE_60M`/`POST_*` outputs remain legacy baselines.
8. Provider-aggregated research bars live in `market_bars`; raw-derived validation bars live in `validation_reconstructed_bars`. Research serving reads the former only.
9. Raw provider payloads and raw trade archives are immutable; Kafka is transport, not source of truth.
10. PostgreSQL curated writes use deterministic business keys. This is not a blanket exactly-once guarantee.
11. Research hypotheses, simulations and Paper experiments have different identifiers and lifecycles.
12. Paper submission requires explicit user approval, a Paper-only endpoint and reconciliation. Research signals cannot call it.

## Three truth types

- **Implementation truth** answers what runs now and is owned by executable code/migrations/tests plus [current-system.md](current-system.md).
- **Target architecture truth** answers what new implementation must conform to and is owned by this contract, topic contracts, configuration contracts and ADRs. Current code cannot silently redefine it.
- **Evidence truth** proves only the dated run it captured. It neither upgrades current capability nor validates target-only nodes.

[Current-vs-target](../engineering/current-vs-target.md) is the gap ledger. Superseded design and presentation files remain historical context only.
