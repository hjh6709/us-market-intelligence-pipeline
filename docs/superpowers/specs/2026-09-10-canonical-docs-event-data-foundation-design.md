# Canonical docs and event-data foundation design

## Scope

Correct the unpopulated migration 009 foundation without rewriting legacy rows. Legacy `economic_events` remains the Phase-A serving model. Canonical event identity, lifecycle versions, numeric official observations, external consensus snapshots, point-in-time surprise and raw-derived validation bars are additive and physically isolated.

## Canonical identities

- Lifecycle: `(economic_event_id, lifecycle_version)`, with nullable `released_at` until `RELEASED` or `CORRECTED`.
- Official observation: `(economic_event_id, observation_code, revision_number)`; polling time is not identity.
- Consensus: `(economic_event_id, observation_code, provider, snapshot_at)`.
- Event-time surprise: initial official observation minus the latest compatible-unit consensus strictly before the primary marker.
- Provider research bars remain in `market_bars`; raw-derived validation bars use `validation_reconstructed_bars` with run and processor lineage.

`published_at`, `provider_updated_at`, `first_observed_at`, and `ingested_at` preserve different facts. Observation codes and canonical units come from the registry; documents and statement text are outside this numeric domain.

## Failure semantics

Same official revision plus the same value/hash is idempotent. Conflicting content is `CONFLICTING_SOURCE_FACT`. Canonical surprise rejects cross-event/code lineage, revised actuals, post-release or stale pre-release consensus, unit mismatch and incorrect arithmetic. Canonical facts are append-only.

## Compatibility

No legacy row is reinterpreted or dual-written. Normalized adapters, canonical serving and eventual deprecation are later phases recorded in `current-vs-target.md`.
