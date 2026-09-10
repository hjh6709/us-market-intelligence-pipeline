STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION

# Serving, deployment and product implementation plan

**Goal:** deploy the validated read product and isolate authenticated operations and Paper experiment controls. Migration numbers and interfaces below are placeholders; local FastAPI/browser serving remains the only implemented claim.

### Task 1: Serve canonical research resources

Files: `src/serving_schemas.py`, `src/serving_repository.py`, `src/serving_service.py`, `src/serving_api.py`, `tests/test_serving_api.py`, `tests/test_serving_repository.py`.

Interface: read-only `/api/v1/events/{id}/facts`, `/sessions`, `/quality`, `/reactions`, and `/provenance`; unavailable and ineligible states are typed payloads rather than fabricated zeroes.

1. Add failing API/repository tests for exact observation IDs, versions, quality axes, pagination and unavailable results.
2. Run `.venv/bin/python -m unittest tests.test_serving_api tests.test_serving_repository -v` and retain RED output.
3. Add only read resources over populated canonical tables; preserve legacy endpoints and block N+1 queries.
4. Run targeted contract tests, query-count checks, browser read journeys and full suite.
5. Commit `feat(serving): expose canonical event research resources`.

### Task 2: Separate authenticated roles

Files: `src/auth.py`, `src/audit_log.py`, `db/migrations/015_access_audit.sql`, `tests/test_authorization.py`, `tests/test_paper_web_safety.py`, `docs/architecture/operations-and-deployment.md`.

Interface: `READER`, `OPERATOR`, and `PAPER_EXECUTOR` permissions; denied requests are audited without storing secrets. Startup rejects any non-Paper broker base URL.

1. Add failing deny-by-default tests for anonymous writes, cross-role access, CSRF/replay and live endpoint configuration.
2. Run `.venv/bin/python -m unittest tests.test_authorization tests.test_paper_web_safety -v` and retain RED output.
3. Add minimal middleware, audit persistence and server-side role checks; browser state never carries credentials.
4. Run targeted tests, disabled-broker browser tests, migration apply-twice and full suite.
5. Commit `feat(security): isolate read operations and Paper roles`.

### Task 3: Add a deployable serving runtime

Files: `deploy/serving/Dockerfile`, `deploy/serving/compose.yaml`, `deploy/serving/README.md`, `tests/test_serving_image.py`, `tests/test_deployment_contract.py`, `docs/architecture/operations-and-deployment.md`, `docs/configuration/README.md`.

Interface: serving image contains API/UI dependencies only; `/health` is liveness and `/ready` verifies required dependencies. Spark, Kafka and Airflow are absent from the public image.

1. Add failing image-contract tests for runtime dependency separation, non-root user, health/readiness and required secret references.
2. Run `.venv/bin/python -m unittest tests.test_serving_image tests.test_deployment_contract -v` and retain RED output.
3. Add the smallest local deployment manifest first; managed PostgreSQL/object storage, TLS and backup configuration remain environment-owned.
4. Build the image, run API/browser smoke tests, perform a disposable backup/restore drill and run the full suite.
5. Commit `build(serving): add isolated deployable runtime`.
