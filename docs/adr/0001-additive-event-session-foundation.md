# ADR 0001: Add event/session foundations without rewriting legacy results

- Status: accepted
- Date: 2026-09-10
- Baseline: `63633a50c85c88e507be458067ecf6706220f813`

## Context

Legacy tables combine event identity, historical-first lifecycle fields and selected release values, while legacy reaction results use release-relative windows. Historical evidence depends on those schemas and metrics. Reinterpreting or backfilling them during the architecture pass would mix verified past output with an unverified target model.

## Decision

Correct the still-unpopulated migration `009_event_session_foundations.sql` before merge. Add a stable canonical event identity plus append-only lifecycle versions, a canonical observation-code/unit registry, deterministic official revision identity, external consensus snapshots, PIT-safe surprise enforcement, durable primary/secondary markers and verified trading sessions. Physically separate provider-aggregated research bars from raw-derived validation bars.

The pure planner normalizes aware instants to UTC, trusts only the market calendar timezone, consumes one calendar snapshot and defines S0 as the first regular session able to absorb the primary marker. Quality/maturity scopes and the complete reaction v2 vocabulary are new foundation interfaces. Do not wire normalized facts or target metrics into current ingestion, legacy analysis, serving or Paper in this change.

## Consequences

- Historical rows and evidence remain immutable and retain their original meaning.
- Upcoming target events can exist before release without weakening the historical legacy table.
- Official polling is idempotent by economic revision; conflicting facts fail explicitly.
- Canonical event-time surprise cannot use a revised actual or post-release consensus and must match exact arithmetic and unit.
- Provider research serving cannot consume or overwrite raw-derived validation bars.
- No canonical event, observation, consensus, surprise, calendar, target reaction or validation-bar backfill is performed by this change.
- Existing databases that applied the earlier unmerged draft of migration 009 require an empty-foundation reset; no populated in-place draft upgrade is claimed.
- Production adapters and any future backfill require independent design, TDD and dated evidence.
