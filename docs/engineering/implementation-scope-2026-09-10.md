# Selected P0/P1 implementation scope — 2026-09-10

## Selection rule

Only changes that are additive, independently testable, do not mutate historical data and cannot alter current presentation results were selected before implementation.

## Selected

1. P0 canonical architecture, data, research, operations and deployment contracts.
2. P0 baseline and contradiction audit plus six independent subsystem specs/plans.
3. P0 evidence-linked Archify target diagram with deterministic and browser validation.
4. P1 additive migration for observations, consensus, surprise lineage, trading sessions and event markers.
5. P1 pure verified-session planner and shared quality/reaction vocabulary.

## Explicitly not selected

- no official release/consensus source adapter or backfill;
- no exchange-calendar network provider or operational calendar DAG;
- no rewrite of legacy `economic_events`, impacts, strategy results or evidence;
- no target reaction computation, comparable search or strategy re-run;
- no research-to-order automation, live order path or public deployment;
- no auth/TLS/backup/managed-infrastructure claim.

## Acceptance gates

The selected slice is acceptable only if targeted tests show RED then GREEN, the full suite remains green, migrations `001..009` apply twice to a disposable PostgreSQL instance, the diagram passes 9/9 checks and browser containment, and the final report separates implemented, historical and target facts.
