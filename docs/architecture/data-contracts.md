# Canonical data contracts

Status: **foundation implemented, adapters not implemented**. Migration 009 creates and enforces the normalized foundation. It does not populate it or connect it to current serving. Legacy rows remain historical evidence under [the compatibility ledger](../engineering/current-vs-target.md).

## Event universe, ontology, and lifecycle

The only canonical event types are `CPI`, `EMPLOYMENT`, `PCE`, and `FOMC`. The observation registry permits exactly these relationships:

| Event | Observation codes |
| --- | --- |
| CPI | `CPI_HEADLINE_MOM`, `CPI_HEADLINE_YOY`, `CPI_CORE_MOM`, `CPI_CORE_YOY` |
| EMPLOYMENT | `NFP_PAYROLL_CHANGE`, `UNEMPLOYMENT_RATE`, `AVERAGE_HOURLY_EARNINGS_MOM`, `AVERAGE_HOURLY_EARNINGS_YOY` |
| PCE | `PCE_HEADLINE_MOM`, `PCE_HEADLINE_YOY`, `PCE_CORE_MOM`, `PCE_CORE_YOY` |
| FOMC | `FED_TARGET_LOWER`, `FED_TARGET_UPPER`, `FED_TARGET_MIDPOINT`, `FED_RATE_CHANGE_BP` |

Both official observations and consensus snapshots reference this ontology; mismatched event/code/unit combinations are rejected by PostgreSQL.

`economic_event_lifecycle_versions` is an append-only, consecutive version chain. Current lifecycle is the highest version. The only allowed transitions are:

```text
SCHEDULED   -> RESCHEDULED | RELEASED | CANCELED
RESCHEDULED -> RESCHEDULED | RELEASED | CANCELED
RELEASED    -> CORRECTED
CORRECTED   -> CORRECTED
```

The same lifecycle identity and material fact is idempotent. Conflicting material under the same identity is rejected. `first_observed_at` is polling metadata, not economic identity.

Legacy `economic_events.value_source`, `quality_status`, `forecast`, `actual`, and `surprise` remain compatibility-only. Existing ingestion is not a normalized official-source adapter.

## Official observations

`economic_release_observations` contains numeric official facts. Identity is `(economic_event_id, observation_code, revision_number)`: revision zero is `INITIAL`; positive revisions are `REVISION` or `CORRECTION`.

For the same identity, a change to any of the following is a conflict:

```text
revision_type, value, unit, published_at, source,
source_revision_id, payload_sha256
```

`first_observed_at` is deliberately excluded from conflict identity. `published_at` is official availability time; `first_observed_at` is when this platform saw it; `ingested_at` is persistence time. Statements and documents are outside this numeric table.

## Consensus and surprise

Consensus is provider-local. Its selector signature is exactly:

```text
select_canonical_prerelease_consensus(event, observation_code, provider)
```

It selects only that provider's latest `snapshot_at` strictly before the **current primary marker**. It never blends providers or creates a composite. The same consensus identity/content is idempotent even if polling metadata changes; different content conflicts. `first_observed_at < snapshot_at` is invalid.

Canonical surprise accepts the initial official observation and the selected provider snapshot for the same event/code/unit, and records the exact current primary marker used at derivation time. The initial observation must not be published before that marker, and stored arithmetic must equal `actual - consensus`. Revised actuals, cross-event inputs, noncanonical snapshots, incompatible units, and wrong arithmetic are rejected. Because no normalization contract exists, `standardized_surprise` must be `NULL`.

`economic_surprises` is immutable historical derived-fact storage. `current_canonical_surprises` is the dynamic current projection: it returns a historical row only while its marker is still the current primary marker and its provider-specific consensus is still the latest snapshot strictly before that marker. Marker corrections and later eligible consensus backfills therefore do not delete history; they make stale derivations disappear from the current projection.

## Marker revisions and trading sessions

Canonical event identity fields (`economic_event_id`, `event_type`, `reference_period`, `official_source`) are immutable; `official_source_url` remains a replaceable locator. `economic_event_markers` is an append-only revision chain per event and marker kind. Corrections insert a new consecutive revision; old rows remain. `current_economic_event_markers` selects the highest revision of each kind. The current primary cannot be demoted, and event-level transaction serialization prevents two concurrent primary kinds from both committing. Consensus selection and the pure session planner consume current markers only.

`calendar_snapshots` is an immutable parent with `calendar_snapshot_id`, `market_code`, `exchange_timezone`, `calendar_source`, `generated_at`, and `payload_sha256`. The v1 market/timezone pair is exactly `US_EQUITIES` / `America/New_York`. Corrections create a new snapshot rather than updating an old one. `trading_sessions` references the parent and stores only session date, open, close and day type (`REGULAR` or `EARLY_CLOSE`); closed dates have no session row. PostgreSQL and the pure planner both require the open and close to resolve to `session_date` in the parent snapshot timezone.

## Market-bar provenance and validation lineage

`market_bars` contains provider-aggregated research bars. `validation_reconstructed_bars` contains raw-derived validation bars. The stores are physically separate and `market_bar_origin_comparison` is comparison-only; current research serving reads `market_bars`.

`validation_runs` freezes `validation_run_id`, `workload_id`, `processor_version`, `checkpoint_namespace`, and optional `source_manifest_identity`. Reusing a run ID with different metadata conflicts. Each reconstructed bar is append-only:

```text
same run + same bar identity + same content -> idempotent
same run + same bar identity + different content -> determinism conflict
```

Spark batch ID is execution metadata and does not authorize mutation of existing evidence.

Observation, consensus, validation-run and reconstructed-bar record boundaries take transaction-scoped advisory locks before read-then-insert decisions. Concurrent identical facts return the same stored identity; concurrent different content reaches the same explicit source/run/determinism conflict taxonomy instead of leaking a raw unique-constraint race.

## Quality scopes

Run outcome, work-item outcome, market interval, session-day type, coverage, eligibility, reason, and metric maturity are separate. The exact executable vocabulary is in `src/platform_contracts.py`; representative valid and invalid combinations are documented in [research-contracts.md](research-contracts.md).

Raw-payload claims require a durable URI and SHA-256 when a payload is retained. A hash alone is content identity, not proof of immutable object storage.
