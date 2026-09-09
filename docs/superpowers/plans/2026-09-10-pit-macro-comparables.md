# Point-in-time macro and comparables implementation plan

**Goal:** produce leakage-safe features, regimes and comparable sets after event/session foundations are populated. This plan is not selected for the 2026-09-10 implementation slice.

### Task 1: Persist versioned research inputs

Files: `db/migrations/010_research_feature_foundations.sql`, `tests/test_research_feature_migration.py`, `docs/architecture/research-contracts.md`, `docs/configuration/README.md`.

Interface: `macro_feature_sets`, `macro_feature_values`, `macro_regime_assignments`, `comparable_runs`, and `comparable_members`; every value references source observation IDs, cutoff, algorithm version and pipeline run.

1. Add migration-source tests for keys, FKs, cutoff, input lineage and immutable run identity.
2. Run `.venv/bin/python -m unittest tests.test_research_feature_migration -v`; new tests must fail because migration `010` is absent.
3. Add only the five tables, constraints and indexes; do not backfill or modify legacy contexts.
4. Re-run the targeted command, apply migrations `001..010` twice to disposable PostgreSQL, then run `.venv/bin/python -m unittest discover -s tests`.
5. Commit `feat(research): add PIT feature and comparable schema`.

### Task 2: Compute point-in-time features and regimes

Files: `src/macro_features.py`, `tests/test_macro_features.py`, `docs/research/methodology.md`, `docs/configuration/README.md`.

Interface: `build_feature_vector(*, event_id, cutoff, observations, feature_version) -> FeatureVector` and `classify_regime(vector, regime_version) -> RegimeAssignment`.

1. Add failing tests with a later-revision trap, unavailable inputs, stable ordering and repeat-run hash.
2. Run `.venv/bin/python -m unittest tests.test_macro_features -v` and retain RED output.
3. Implement pure selection using only `observed_at <= cutoff`; return typed unavailable inputs instead of imputation.
4. Run the targeted test and full suite; verify the current event release cannot enter its own pre-event regime.
5. Commit `feat(research): compute leakage-safe macro features`.

### Task 3: Select reproducible comparables

Files: `src/comparable_events.py`, `tests/test_comparable_events.py`, `src/repositories/comparable_repository.py`, `tests/test_comparable_repository.py`.

Interface: `select_comparables(*, subject, candidates, metric, top_k, run_id) -> ComparableSet`, with deterministic `(distance, event_id)` tie ordering and typed `INSUFFICIENT_UNIVERSE`.

1. Add failing tests for filter versions, deterministic ties, insufficient candidates and exact member persistence.
2. Run `.venv/bin/python -m unittest tests.test_comparable_events tests.test_comparable_repository -v` and retain RED output.
3. Implement pure selection and one transactional repository write; do not add UI or DAG code.
4. Run targeted tests, migration integration and full suite; compare repeated-run member hashes.
5. Commit `feat(research): add reproducible comparable event sets`.
