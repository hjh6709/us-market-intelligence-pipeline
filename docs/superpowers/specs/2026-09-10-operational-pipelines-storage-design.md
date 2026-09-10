STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION

# Operational pipelines and storage design

## Scope

Move from historical/manual DAGs to exactly three primary operational DAGs: `event_catalog_refresh_pipeline`, `market_intelligence_incremental_pipeline`, and `paper_execution_reconciliation_pipeline`. Kafka remains bounded transport; provider research and raw validation storage remain distinct.

## Target interfaces

Each DAG emits run, work-item and check records with source identity, attempt, counts, version and classified failure. Immutable raw objects carry URI, SHA-256 and partition manifest. PostgreSQL stores curated records and lineage. Event maturity is independent of task success.

## Failure rules and tests

Provider, validation, persistence, orchestration and unavailable-by-design states are distinct. Retry safety is declared per task. Tests cover duplicate schedules, partial pagination, stale sources, late corrections and raw-object hash mismatch.

## Delivery status

Documentation and plan only. Existing DAG schedules and historical evidence are unchanged.
