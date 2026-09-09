# Operational, storage and deployment contract

Status: target contract with explicit current gaps.

## Operational Airflow

Target DAG families are event-catalog refresh, release-observation ingestion, consensus snapshots, trading-session refresh, market bars, point-in-time macro features, reactions, comparable sets and strategy validation. Every task emits a durable run/work-item/check record with input identity, attempt, status, counts, error class and version.

Event maturity progresses independently through `SCHEDULED`, `RELEASE_OBSERVED`, `MARKET_DATA_READY`, `RESEARCH_READY`, and optional `PAPER_EVALUATED`. Retries are safe only where idempotency is defined. Broker POST is never an Airflow retry target.

Freshness is measured against each source's expected availability, not a single wall-clock SLA. Failures are classified as provider, contract/validation, persistence, orchestration or unavailable-by-design.

## Storage

Immutable raw payloads and raw SIP partitions belong in object storage with content hashes and partition manifests. PostgreSQL holds queryable curated facts, quality, lineage, telemetry and journals. Kafka is bounded transport/replay. Spark is an on-demand validation engine; neither is the durable source of truth.

## Serving and deployment

Current local FastAPI/browser serving remains a valid implementation. The target topology separates:

- public read-only research resources;
- authenticated operational views and explicit Paper controls;
- on-demand heavy validation workers;
- managed PostgreSQL and immutable object storage.

Authentication, authorization, secrets management, TLS, backups, observability and deployment automation are target requirements, not current claims. No public production deployment is asserted.

## Paper experiments

The current manual order-intent journal is retained. The target adds `paper_experiments`, `paper_orders`, `paper_fills`, `paper_positions`, reconciliation events and outcomes. An experiment references a frozen hypothesis/version and risk envelope. Manual approval is auditable; live endpoints remain structurally forbidden.
