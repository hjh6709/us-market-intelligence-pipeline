# Canonical docs and event-data foundation implementation plan

**Goal:** establish authoritative present/target documentation and additive event-fact lineage without changing existing research results.

**Architecture:** preserve `economic_events` for compatibility; add normalized append-only tables referencing it. Canonical docs use explicit state labels and route superseded pages to the new contract.

**Tech:** Markdown, PostgreSQL DDL, Python `unittest`.

**Status:** executable corrective plan for PR #36.

### Task 1: Freeze baseline and precedence

Files: `docs/engineering/baseline-audit-2026-09-10.md`, `docs/architecture/platform-contract.md`, `docs/README.md`.

1. Record fetched SHA and pre-change suite result.
2. Write identity, planes, invariants and precedence.
3. Add superseded banners to legacy architecture/data/vision/presentation pages.
4. Run `rg -n "production.ready|fully complete|automated trading" README.md docs --glob '*.md'` and classify every match.
5. Run the Markdown relative-link check documented in `docs/evidence/architecture-pass-2026-09-10/verification.json` and commit `docs(architecture): establish canonical platform contracts`.

### Task 2: Specify normalized event lineage

Files: `docs/architecture/data-contracts.md`, `db/migrations/009_event_session_foundations.sql`, `tests/test_event_session_foundation_migration.py`.

Interface: `record_economic_release_observation(event_id, observation_code, revision_number, source_revision_id, revision_type, value, unit, published_at, first_observed_at, source, source_url, payload_sha256) -> bigint`.

- [ ] Write shape and actual-PostgreSQL tests for upcoming lifecycle, canonical code/unit, deterministic revision identity, idempotent same fact, explicit conflict, later revision and append-only mutation.
- [ ] RED: run `RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=... python -m unittest tests.integration.test_event_session_foundation_postgres -v`; an unimplemented constraint must fail for the asserted reason.
- [ ] Minimally correct migration 009 only; do not alter migrations 001–008 or populate the new tables.
- [ ] Add PIT-surprise tests for strict latest pre-release consensus, initial actual, compatible units, exact arithmetic and cross-event/code rejection.
- [ ] GREEN: rerun the exact integration command and `python -m unittest tests.test_event_session_foundation_migration -v`.
- [ ] Apply migrations 001–009 twice to fresh PostgreSQL 17.
- [ ] Commit boundary: `fix(schema): correct canonical event fact identities`.

### Task 3: Isolate bar provenance

Files: `src/postgres.py`, `src/historical_bars.py`, `src/spark_market_processor.py`, `tests/test_postgres.py`, `tests/test_historical_bars.py`, `tests/test_spark_market_processor.py`, `tests/integration/test_postgres_market_bars.py`.

Interface: `upsert_validation_bars(connection, rows)`; `checkpoint_paths(root, validation_run_id, processor_version)`.

- [ ] RED: assert provider and raw-derived sinks target distinct tables and same market identity survives in both.
- [ ] Minimal implementation: keep provider aggregates in `market_bars`; add run/version/checkpoint lineage to `validation_reconstructed_bars`.
- [ ] GREEN: run the exact affected unit/integration modules.
- [ ] Commit boundary: `fix(storage): isolate raw-derived validation bars`.

### Task 4: Verify and commit

1. Run `.venv/bin/python -m unittest discover -s tests` and `node --test tests/research_ui.cjs`.
2. Write additive evidence under `docs/evidence/architecture-pass-2026-09-10/`.
3. Run `git diff --check`, JSON parsing and Markdown relative-link verification.
4. Commit `test(platform): record architecture pass verification`.
