# Canonical docs and event-data foundation implementation plan

**Goal:** establish authoritative present/target documentation and additive event-fact lineage without changing existing research results.

**Architecture:** preserve `economic_events` for compatibility; add normalized append-only tables referencing it. Canonical docs use explicit state labels and route superseded pages to the new contract.

**Tech:** Markdown, PostgreSQL DDL, Python `unittest`.

### Task 1: Freeze baseline and precedence

Files: `docs/engineering/baseline-audit-2026-09-10.md`, `docs/architecture/platform-contract.md`, `docs/README.md`.

1. Record fetched SHA and pre-change suite result.
2. Write identity, planes, invariants and precedence.
3. Add superseded banners to legacy architecture/data/vision/presentation pages.
4. Run `rg -n "production.ready|fully complete|automated trading" README.md docs --glob '*.md'` and classify every match.
5. Run the Markdown relative-link check documented in `docs/evidence/architecture-pass-2026-09-10/verification.json` and commit `docs(architecture): establish canonical platform contracts`.

### Task 2: Specify normalized event lineage

Files: `docs/architecture/data-contracts.md`, `db/migrations/009_event_session_foundations.sql`, `tests/test_event_session_foundation_migration.py`.

1. Write tests that require observation, consensus and surprise tables, immutable identities, source timestamps, SHA-256 checks, exact event/metric foreign keys and update/delete rejection.
2. Run `.venv/bin/python -m unittest tests.test_event_session_foundation_migration -v` and confirm RED.
3. Add idempotent DDL only; do not alter migration `002`.
4. Re-run the targeted test and confirm GREEN.
5. Apply migrations `001..009` twice to a disposable PostgreSQL database and inspect constraints; execute rejected mutation and cross-event surprise fixtures.
6. Commit `feat(schema): add immutable event fact foundations`.

### Task 3: Verify and commit

1. Run `.venv/bin/python -m unittest discover -s tests` and `node --test tests/research_ui.cjs`.
2. Write additive evidence under `docs/evidence/architecture-pass-2026-09-10/`.
3. Run `git diff --check`, JSON parsing and Markdown relative-link verification.
4. Commit `test(platform): record architecture pass verification`.
