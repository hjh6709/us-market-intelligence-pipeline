# ADR 0001: Add event/session foundations without rewriting legacy results

- Status: accepted
- Date: 2026-09-10
- Baseline: `63633a50c85c88e507be458067ecf6706220f813`

## Context

Legacy tables combine event identity with selected release values, and legacy reaction results use release-relative windows. Historical evidence depends on those schemas and metrics. Reinterpreting or backfilling them during the architecture pass would mix verified past output with an unverified target model.

## Decision

Add normalized release observations, consensus snapshots, surprise rows, verified trading sessions and event markers in migration `009_event_session_foundations.sql`. Point-in-time fact tables reject update/delete, and surprise foreign keys require the referenced actual and consensus to match the same event and metric. Add a pure session planner and quality/reaction vocabulary behind new interfaces. Do not wire these foundations into current ingestion, legacy impact computation, strategy output, serving views or Paper execution in this change.

## Consequences

- Historical rows and evidence remain immutable and retain their original meaning.
- The new tables establish keys, provenance and referential integrity for later adapters.
- The planner can be tested for holidays, early closes and release phases without network access.
- No observation, consensus, surprise, calendar or target reaction data is populated by this change.
- A later migration/backfill must be independently designed, tested and evidenced.
