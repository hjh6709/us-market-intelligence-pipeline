# Configuration contract

Status: `CURRENT_IMPLEMENTATION` configuration compatibility plus
`HISTORICAL_SUPERSEDED` target notes. Executable variable names and behavior remain
owned by the current code and tests; future CPI W1 and Runtime/Security configuration
must follow the authority routed for those concerns by
[the authority index](../architecture/AUTHORITY.md). This document does not create
or override active target semantics.

This page records configuration boundaries for the **Economic Event Intelligence & Strategy Validation Platform** at the 2026-09-10 baseline.

## Current configuration

| Boundary | Current source | Safety rule |
| --- | --- | --- |
| PostgreSQL | `DATABASE_URL` or the repository's split PostgreSQL variables | credentials stay outside Git; each migration documents its own replay guarantees |
| Alpaca market data | Alpaca data credentials and feed selection | research bars use the documented SIP source/version; future data is not silently substituted |
| FRED/ALFRED | provider API key and series catalog | point-in-time selection keeps vintage/provenance fields |
| Kafka/Spark validation | Docker/CLI settings and checked-in topic/checkpoint defaults | this is the archived raw-trade validation plane, not the source of the 202×10 aggregate-bar study |
| Alpaca Paper | Paper-only base URL, account identity and explicit enable flag | no live endpoint; browser review and exact confirmation precede POST; research signals do not submit orders |

Variable names and startup commands remain owned by the executable README and `.env.example`. This contract does not publish secret values, account identifiers or provider payloads.

## Historical migration-009 foundation configuration

The new verified-session planner accepts configuration as explicit typed input rather than reading process environment:

- market code with trusted calendar timezone (`US_EQUITIES` -> `America/New_York`); listing venue is separate;
- verified session open/close instants;
- event release instant;
- optional statement and press-conference markers.

The planner does **not** fetch an exchange calendar and is not wired into legacy analytics. Calendar source, content-addressed snapshot/generation identity and holiday/early-close provenance must be supplied by a later operational adapter before its output is treated as production data. A caller cannot substitute an arbitrary IANA timezone.

The shared quality vocabulary and `event_session_reaction_v2` metric definitions are code contracts in `src/platform_contracts.py`. Coverage thresholds, endpoint tolerances and maturity rules remain versioned research policy; they must not be inferred from provider request success.

## Historical target-only configuration

The following configuration families are preserved as 2026-09-10 design context,
not current runtime capability or approved CPI W1/runtime target:

- consensus provider and snapshot cutoff policy;
- canonical release-observation adapters and revision policy;
- comparable-event filters and macro-regime version;
- target reaction horizons through `S0_CLOSE_TO_S+7_CLOSE`;
- operational Airflow freshness/maturity policy;
- object-storage retention and immutable raw-manifest policy;
- deployed auth, TLS, backups and managed service topology;
- strategy-experiment-to-Paper lineage and risk limits.

Adding one of these settings does not make the subsystem implemented. A setting becomes current only with an adapter, tests, migration where needed, and evidence linked from the canonical topic contract.

## Truth boundaries

Executable settings describe current implementation only to the extent they are
consumed by the code and tests at the commit being inspected. Active target semantics
come only from the document to which
[the authority index](../architecture/AUTHORITY.md) routes the relevant concern as
`TARGET_CANONICAL`; this configuration document cannot promote its historical
target notes into active target authority. Dated evidence proves only the captured
run. `docs/engineering/current-vs-target.md` is a `STATUS_LEDGER`, not semantic
authority or current executable truth.

Migration 009 can be replayed on a database that already has its corrected schema. It is not a claim that every historical migration is universally rerunnable, nor that an already-applied draft migration 009 can be upgraded in place after population.
