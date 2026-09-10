# Economic Event Intelligence & Strategy Validation Platform contract

Status: canonical contract. Its migration/pure-contract subset is implemented as a **foundation only**; adapters, backfill, v2 reaction calculation, and serving integration remain target-only. Baseline revision: `63633a50c85c88e507be458067ecf6706220f813` (2026-09-10). The final corrective revision is recorded in the dated verification receipt.

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

1. Canonical event type is exactly `CPI | EMPLOYMENT | PCE | FOMC`; official observations and consensus must satisfy the exact event/code/unit ontology in [data-contracts.md](data-contracts.md).
2. Lifecycle and marker corrections are append-only consecutive revisions. Current means the highest valid revision; old facts are never updated.
3. Release observation identity is `(event, observation_code, revision_number)`. Polling time is excluded; conflicting revision type, value, unit, publication time, source, source revision, or payload hash is rejected.
4. Consensus selection takes an explicit provider and chooses that provider's latest snapshot strictly before the current primary marker. Canonical surprise history is immutable; its current projection additionally requires the exact current marker and currently selected provider snapshot. It uses an initial official observation, exact unit/arithmetic, and keeps standardized surprise null.
5. `S0` is the first regular trading session able to absorb the current primary marker. Post-market and closed-day releases use the next verified session; S+N counts the same calendar snapshot. Parent-timezone local dates are enforced in Python and PostgreSQL, and FOMC statement and press conference keep distinct marker identities.
6. Run outcome, work-item outcome, session/interval type, coverage, analysis eligibility, reason code, and per-metric maturity are distinct semantics.
7. Price reaction metrics declare typed category, marker, endpoints, prices, clipping, tolerance, maturity, applicability and version. Activity metrics declare exact typed calculations without fake price fields. Legacy `PRE_60M`/`POST_*` outputs remain legacy baselines.
8. Provider-aggregated research bars live in `market_bars`; raw-derived validation bars live in `validation_reconstructed_bars`. Immutable `validation_runs` freezes run/process/checkpoint lineage. Research serving reads provider bars only.
9. Raw provider payloads and raw trade archives are immutable; Kafka is transport, not source of truth.
10. PostgreSQL canonical immutable-record boundaries serialize same-identity decisions: identical content converges on one fact and different content raises a domain conflict. Legacy curated writes use deterministic business keys. This is not a blanket exactly-once guarantee.
11. Research hypotheses, simulations and Paper experiments have different identifiers and lifecycles.
12. Paper submission requires explicit user approval, a Paper-only endpoint and reconciliation. Research signals cannot call it.

## Three truth types

- **Implementation truth** answers what runs now and is owned by executable code/migrations/tests plus [current-system.md](current-system.md).
- **Target architecture truth** answers what new implementation must conform to and is owned by this contract, topic contracts, configuration contracts and ADRs. Current code cannot silently redefine it.
- **Evidence truth** proves only the dated run it captured. It neither upgrades current capability nor validates target-only nodes.

[Current-vs-target](../engineering/current-vs-target.md) is the gap ledger. Superseded design and presentation files remain historical context only.
