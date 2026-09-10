# Canonical data contracts

Status: target contract. Migration 009 supplies an empty schema foundation; no normalized production adapters or backfill are implemented. Current legacy serving remains unchanged.

## Event lifecycle and compatibility

`canonical_economic_events` is the stable target identity. It can optionally link to a historical `economic_events` row, but it does not inherit the legacy row's lifecycle or scalar value semantics.

`economic_event_lifecycle_versions` is append-only and records `scheduled_at`, nullable `released_at`, `event_status`, source publication time, first system observation time and payload hash. States are `SCHEDULED`, `RELEASED`, `RESCHEDULED`, `CANCELED`, and `CORRECTED`. This permits an upcoming event before release and preserves schedule changes as new versions.

The following legacy `economic_events` fields remain compatibility-only during Phase A: `value_source`, `quality_status`, `forecast`, `actual`, and `surprise`. Existing ingestion still writes historical catalog rows and must not be described as the normalized lifecycle adapter. In particular, `value_source='fred'` is not normalized actual provenance.

## Observation registry and official revisions

`economic_observation_registry` owns canonical identity and unit. Display labels from an official provider must map explicitly to a registered `observation_code`; display text is never a business key. The initial registry covers CPI headline/core MoM/YoY, payrolls, unemployment, hourly earnings, PCE headline/core MoM/YoY and Federal Reserve target/rate-change facts.

`economic_release_observations` contains numeric official facts only. Statements, implementation notes, SEP files and other text/document artifacts belong to a future `event_documents` domain, not this table.

Official revision identity is `(economic_event_id, observation_code, revision_number)`. `revision_number` is the platform's internal sequence: zero is `INITIAL`; positive numbers are `REVISION` or `CORRECTION`. `source_revision_id` is optional because official providers do not always publish one. The write boundary has two outcomes:

```text
same identity + same source/value/unit/payload hash -> return existing ID
same identity + conflicting fact                    -> CONFLICTING_SOURCE_FACT
```

Timestamp vocabulary is explicit:

- `published_at`: when the official source made the revision available;
- `first_observed_at`: when this platform first saw it;
- `ingested_at`: PostgreSQL persistence time.

Polling time is not part of the business identity and cannot duplicate an economic fact.

## External consensus and canonical surprise

`economic_consensus_snapshots` is an external-provider domain. `snapshot_at` is the source-time availability used for event-time PIT selection; `provider_updated_at` records a provider correction when supplied; `first_observed_at` and `ingested_at` remain system times. Post-release snapshots may be retained for audit.

`select_canonical_prerelease_consensus(event, observation_code)` chooses the latest snapshot whose `snapshot_at` is strictly before the event's primary marker. Canonical `economic_surprises` accepts only:

```text
initial official observation
- latest valid pre-release external consensus
= stored surprise_value
```

Both inputs must reference the same event, `observation_code`, and canonical unit. Revised actuals, post-release consensus, non-latest eligible snapshots, incompatible units, and incorrect arithmetic are rejected. Any later revised-surprise research requires a different semantic name and version.

## Trading sessions and markers

`trading_sessions` stores `market_code`, local `session_date`, UTC open/close instants, `session_day_type`, trusted timezone, calendar source, content-addressed calendar snapshot identity and generation time. For v1, `US_EQUITIES` is bound to `America/New_York`; listing venue is not the market timezone authority.

One session plan consumes a single market, timezone, calendar source and snapshot. Open and close must map back to `session_date` in the trusted local timezone. Plans are ordered, unique and non-overlapping and record `planner_version`.

`economic_event_markers` has durable marker IDs and exactly one primary marker per event. For ordinary releases, `RELEASE` is primary. For FOMC, `STATEMENT` is primary and `PRESS_CONFERENCE` is secondary; a synthetic duplicate `RELEASE` marker is not added.

## Market-bar provenance

`market_bars` contains provider-aggregated canonical research bars used by legacy research and serving. `validation_reconstructed_bars` contains raw-derived validation bars keyed by validation run, processor version and bar identity. The stores are physically separate, so one origin cannot overwrite the other.

`market_bar_origin_comparison` is a comparison-only view that exposes `PROVIDER_AGGREGATE` and `RAW_RECONSTRUCTED`. Research queries do not use this union. `condition_policy` is a trade-policy field, not a hidden origin discriminator, and provider rows retain historical `spark_batch_id=-1` only as legacy storage compatibility.

## Quality, provenance and lifecycle

Run outcome, work-item outcome, market interval, coverage and analysis eligibility are separate scopes. Each record uses `reason_code` plus `reason_detail`; metric maturity may add `eligible_at`. The canonical vocabulary is defined in `src/platform_contracts.py`.

Raw payload claims require a durable URI and SHA-256 when a payload is retained. A hash alone proves content identity but not that an immutable payload archive exists. Destructive lifecycle automation remains disabled until retention policy is approved.
