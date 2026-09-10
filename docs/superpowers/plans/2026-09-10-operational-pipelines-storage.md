STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION

# Operational pipelines and storage implementation plan

**Goal:** provide exactly three primary operational DAGs, durable telemetry, freshness and immutable raw storage while retaining separate research and validation planes. Migration numbers and interfaces below are placeholders.

### Task 1: Persist freshness, maturity and failure classes

Files: `db/migrations/013_operational_telemetry.sql`, `src/pipeline_run_tracking.py`, `src/failure_classification.py`, `tests/test_operational_telemetry.py`, `tests/test_failure_classification.py`.

Interface: `record_attempt(work_identity, attempt, counts, failure_class)` and `evaluate_event_maturity(event_id, required_inputs, cutoff)`. Maturity is separate from task status.

1. Add failing tests for duplicate schedules, monotonic attempts, source counts and `PROVIDER/VALIDATION/PERSISTENCE/ORCHESTRATION/UNAVAILABLE_BY_DESIGN` classes.
2. Run `.venv/bin/python -m unittest tests.test_operational_telemetry tests.test_failure_classification -v` and retain RED output.
3. Add migration `013` plus minimal repository/classifier changes; do not retrofit unprovable fields into old runs.
4. Run targeted tests, apply migrations `001..013` twice to disposable PostgreSQL, query a fixed fixture run and run the full suite.
5. Commit `feat(ops): add freshness maturity and failure telemetry`.

### Task 2: Add the three operational DAGs

Files sketch: `dags/event_catalog_refresh_pipeline.py`, `dags/market_intelligence_incremental_pipeline.py`, `dags/paper_execution_reconciliation_pipeline.py`, `tests/test_operational_dag_contracts.py`, `docs/architecture/operations-and-deployment.md`.

Interface: each DAG validates a typed config, registers one run, uses deterministic work identities, emits counts/checks and has declared retry safety. No DAG calls broker POST.

1. Add failing import/graph tests for task boundaries, pools, pagination guards and absence of broker adapters.
2. Run `.venv/bin/python -m unittest tests.test_operational_dag_contracts -v` and retain RED output.
3. Implement one bounded catalog/observation/calendar gold path before enabling downstream reaction orchestration.
4. Run DAG import tests, `airflow dags test` with fixtures, telemetry queries and the full suite.
5. Commit separately as `feat(airflow): add source-specific foundation DAGs` and `feat(airflow): add reaction orchestration`.

### Task 3: Register immutable raw objects

Files: `db/migrations/014_raw_object_manifests.sql`, `src/raw_object_manifest.py`, `tests/test_raw_object_manifest.py`, `tests/test_raw_validation_boundary.py`, `docs/architecture/operations-and-deployment.md`.

Interface: `register_raw_object(uri, sha256, bytes, source, partition, observed_at) -> manifest_id`; Kafka receives bounded references/envelopes and is never the source of truth.

1. Add failing tests for invalid hashes, conflicting URI/hash, missing partitions and proof that research provider bars do not inherit raw-replay lineage.
2. Run `.venv/bin/python -m unittest tests.test_raw_object_manifest tests.test_raw_validation_boundary -v` and retain RED output.
3. Add manifest storage and hash verification against a local object-store fixture; keep Spark on-demand.
4. Run targeted tests, hash-mismatch integration, duplicate replay and full suite.
5. Commit `feat(storage): add immutable raw object manifests`.
