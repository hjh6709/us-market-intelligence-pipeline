# Canonical docs and event-data foundation design

## Scope

Replace contradictory architecture claims with a precedence-controlled current/target contract, then add compatible storage foundations for immutable release observations, consensus snapshots and derived surprise lineage.

## Current and target

Current migration `002` stores event identity and value fields together. It remains readable for compatibility. Target tables are additive and reference `economic_events`; no historical evidence or current row is rewritten.

## Interfaces and failure rules

- observation identity: event, metric, revision, observation time;
- consensus identity: event, metric, provider, observation time;
- surprise identity: actual observation, consensus snapshot, algorithm version;
- missing source URL, observed time, value lineage or invalid timestamps fail insertion;
- payloads are referenced by SHA-256; secrets and raw payloads are not documentation artifacts.

## Verification

Migration contract tests assert keys, foreign keys, time constraints, lineage and idempotent DDL. Documentation self-review scans superseded pages and ensures current/target labels are present.

## Delivery status

Selected P0/P1: canonical docs, additive migration, contract tests. Operational source ingestion and backfill are not selected.
