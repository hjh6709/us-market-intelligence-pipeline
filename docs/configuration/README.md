# Configuration contract

This page owns configuration boundaries for the **Economic Event Intelligence & Strategy Validation Platform**. It distinguishes configuration already consumed by the repository from target configuration that has no operational adapter yet.

## Current configuration

| Boundary | Current source | Safety rule |
| --- | --- | --- |
| PostgreSQL | `DATABASE_URL` or the repository's split PostgreSQL variables | credentials stay outside Git; migrations are additive and rerunnable |
| Alpaca market data | Alpaca data credentials and feed selection | research bars use the documented SIP source/version; future data is not silently substituted |
| FRED/ALFRED | provider API key and series catalog | point-in-time selection keeps vintage/provenance fields |
| Kafka/Spark validation | Docker/CLI settings and checked-in topic/checkpoint defaults | this is the archived raw-trade validation plane, not the source of the 202×10 aggregate-bar study |
| Alpaca Paper | Paper-only base URL, account identity and explicit enable flag | no live endpoint; browser review and exact confirmation precede POST; research signals do not submit orders |

Variable names and startup commands remain owned by the executable README and `.env.example`. This contract does not publish secret values, account identifiers or provider payloads.

## P1 foundation configuration

The new verified-session planner accepts configuration as explicit typed input rather than reading process environment:

- exchange name and IANA timezone;
- verified session open/close instants;
- event release instant;
- optional statement and press-conference markers.

The planner does **not** fetch an exchange calendar and is not wired into legacy analytics. Calendar provider, calendar version and holiday/early-close provenance must be supplied by a later operational adapter before its output is treated as production data.

The shared quality vocabulary and `event_session_reaction_v1` metric version are code constants in `src/platform_contracts.py`. Thresholds for observed coverage remain versioned research policy; they must not be inferred from provider request success.

## Target-only configuration

The following configuration families are approved target architecture, not current runtime capability:

- consensus provider and snapshot cutoff policy;
- canonical release-observation adapters and revision policy;
- comparable-event filters and macro-regime version;
- target reaction horizons through `POST_S1_CLOSE`;
- operational Airflow freshness/maturity policy;
- object-storage retention and immutable raw-manifest policy;
- deployed auth, TLS, backups and managed service topology;
- strategy-experiment-to-Paper lineage and risk limits.

Adding one of these settings does not make the subsystem implemented. A setting becomes current only with an adapter, tests, migration where needed, and evidence linked from the canonical topic contract.

## Precedence

For disagreements, follow `docs/architecture/platform-contract.md`, then the relevant canonical topic contract, then this configuration contract. Dated evidence and archived assignment documents never override current code or these contracts.
