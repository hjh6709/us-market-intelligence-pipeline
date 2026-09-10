# PR #36 Corrective Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct PR #36's event, observation, surprise, session, quality, reaction, bar-provenance, and compatibility semantics without adding operational product features.

**Architecture:** Keep legacy serving and historical evidence unchanged. Replace the still-empty migration 009 foundation with a normalized target lifecycle and append-only fact model, drive session planning from durable primary/secondary markers over one trusted U.S.-equities calendar generation, and physically isolate raw-derived validation bars from provider-aggregated research bars. Pure Python contracts make the target semantics testable before adapters exist.

**Tech Stack:** Python 3.13, `unittest`/pytest, PostgreSQL 17, psycopg 3, SQL migrations, Archify 2.17.

**Spec:** `/Users/hanjeonghyun/Downloads/CODEX_PR36_CORRECTIVE_REVIEW_2026-09-10.md`

## Global Constraints

- Work only on `architecture-contract-2026-09-10` in `.worktrees/platform-contract-2026-09-10`.
- Do not merge PR #36.
- Preserve migrations 001–008 and all historical evidence.
- Migration 009 is safe to correct because its new foundation tables are unpopulated and the PR is unmerged.
- Current legacy serving remains unchanged; normalized adapters, backfills, reaction computation, comparables, simulation, Paper expansion, DAGs, frontend, and deployment are out of scope.
- Every behavioral correction follows RED → minimal implementation → targeted GREEN.

---

### Task 1: Quality, maturity, and reaction contracts

**Files:**
- Modify: `tests/test_platform_contracts.py`
- Modify: `src/platform_contracts.py`

**Interfaces:**
- Produces: distinct `PipelineRunOutcome`, `WorkItemOutcome`, `SessionDayType`, `MarketIntervalType`, `CoverageStatus`, `AnalysisEligibility`, and `ReasonCode` enums.
- Produces: `QualityAssessment(work_item_outcome, market_interval_type, coverage_status, analysis_eligibility, reason_code, reason_detail, eligible_at)`.
- Produces: complete `ReactionMetric`, `REACTION_METRIC_DEFINITIONS`, and `REACTION_METRIC_VERSION` without legacy pre-event drift.

- [ ] Add literal tests for Good Friday, provider safety lag, sparse premarket, and future maturity.
- [ ] Run `pytest -q tests/test_platform_contracts.py` and record the expected missing/wrong-contract failures.
- [ ] Replace the coarse enums and add validation that keeps provider availability, coverage, applicability, and maturity independent.
- [ ] Define the complete announcement/session/persistence/activity reaction vocabulary and metadata fields required by the review contract.
- [ ] Run `pytest -q tests/test_platform_contracts.py` and require zero failures.

### Task 2: Verified-session and marker semantics

**Files:**
- Modify: `tests/test_trading_sessions.py`
- Modify: `src/trading_sessions.py`

**Interfaces:**
- Consumes: canonical `SessionDayType` and trusted `US_EQUITIES -> America/New_York` mapping.
- Produces: `EventMarker(marker_id, kind, role, at)` and `plan_event_session(markers, sessions, planner_version)`.
- Produces: `EventSessionPlan` whose S0 is the first regular session able to absorb the primary marker.

- [ ] Add tests for premarket, regular, post-market, closed day, early close, DST, timezone trust, calendar source/snapshot consistency, local-date consistency, duplicate markers, and FOMC marker roles.
- [ ] Run `pytest -q tests/test_trading_sessions.py` and record the expected failures, including the existing after-hours behavior.
- [ ] Normalize any aware instant to UTC, enforce trusted market metadata and one calendar generation, and make marker identity/role explicit.
- [ ] Map post-market/closed releases to the next verified S0; keep release-day session separately.
- [ ] Run `pytest -q tests/test_trading_sessions.py` and require zero failures.

### Task 3: Event lifecycle, observation identity, and PIT-safe surprise schema

**Files:**
- Modify: `tests/test_event_session_foundation_migration.py`
- Create: `tests/integration/test_event_session_foundation_postgres.py`
- Modify: `db/migrations/009_event_session_foundations.sql`

**Interfaces:**
- Produces: normalized lifecycle rows that can exist before release and optionally link to legacy `economic_events`.
- Produces: canonical observation registry keyed by `observation_code` and canonical unit.
- Produces: deterministic official revision identity with `published_at`, `first_observed_at`, and `ingested_at`.
- Produces: canonical pre-release consensus selection and surprise enforcement against the primary marker, initial actual, compatible unit, and exact arithmetic.

- [ ] Add schema-shape tests for lifecycle state, observation codes, timestamp vocabulary, calendar snapshot, marker role, and physical validation-bar isolation.
- [ ] Add PostgreSQL behavioral tests for upcoming events, idempotent same-revision writes, conflicting-revision rejection, later revision preservation, append-only facts, post-release consensus exclusion, revised-actual exclusion, unit mismatch, arithmetic mismatch, calendar consistency, and marker uniqueness/role.
- [ ] Run the targeted tests against the current migration and record RED.
- [ ] Rewrite only migration 009 with the normalized schema, constraints, partial indexes, triggers, and narrow helper functions/views required by those tests.
- [ ] Run the targeted unit tests and PostgreSQL integration tests and require zero failures.

### Task 4: Physically isolate research and validation bars

**Files:**
- Modify: `tests/test_postgres.py`
- Modify: `tests/integration/test_postgres_market_bars.py`
- Modify: `tests/integration/test_kafka_spark_postgres.py`
- Modify: `src/postgres.py`
- Modify: `src/historical_bars.py`
- Modify: `src/spark_sip_trade_batch.py`
- Modify: `src/pipeline_serving.py`

**Interfaces:**
- Provider aggregate path writes only `market_bars`.
- Spark/raw replay first records immutable `validation_runs(validation_run_id, workload_id, processor_version, checkpoint_namespace, source_manifest_identity)`, then writes only append-only `validation_reconstructed_bars` using run + bar identity.
- `market_bar_origin_comparison` is comparison-only; legacy research queries continue reading `market_bars`.

- [ ] Add tests proving provider and raw-derived identities cannot overwrite one another, legacy research reads provider storage only, and validation can compare both origins.
- [ ] Run targeted tests and record RED against the shared-table sink.
- [ ] Split provider SQL from validation recording, route Spark through the immutable run parent and append-only validation-bar function, and update lineage labels without a mutating compatibility UPSERT.
- [ ] Run targeted tests and PostgreSQL integration tests and require zero failures.
- [ ] If the historical DB is reachable, run the bounded `condition_policy/spark_batch_id` audit; otherwise record that historical collision was not asserted.

### Task 5: Canonical documentation, plans, and diagram semantics

**Files:**
- Modify: `docs/architecture/platform-contract.md`
- Modify: `docs/architecture/data-contracts.md`
- Modify: `docs/architecture/research-contracts.md`
- Modify: `docs/architecture/operations-and-deployment.md`
- Modify: `docs/configuration/README.md`
- Modify: `docs/adr/0001-additive-event-session-foundation.md`
- Modify: `docs/engineering/current-vs-target.md`
- Modify: `docs/superpowers/specs/2026-09-10-*.md`
- Modify: `docs/superpowers/plans/2026-09-10-*.md`
- Modify: `docs/diagrams/target-platform.architecture.json`
- Regenerate: `docs/diagrams/target-platform.html` and visual evidence sidecars.

**Interfaces:**
- Implementation truth, target truth, and dated evidence truth remain distinct.
- A/B plans remain executable; C–F are labeled `STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION`.

- [ ] Correct official/external source boundaries, lifecycle, revision/PIT, session/S0, quality/maturity, reaction, macro/comparable/simulation, DAG, Paper, storage, and compatibility text.
- [ ] Update the diagram with explicit `CURRENT`, `SCHEMA FOUNDATION`, `PURE LOGIC FOUNDATION`, and `TARGET ONLY` labels and source citations to migration 009 and `src/trading_sessions.py`.
- [ ] Run Archify validate, deliver, and visual-check, then separately inspect source grounding and rendered light/dark screenshots.
- [ ] Run contradiction, placeholder, Markdown-link, and historical-banner scans.

### Task 6: Full verification and semantic gate

**Files:**
- Modify: `docs/evidence/architecture-pass-2026-09-10/verification.json`
- Modify: `docs/evidence/architecture-pass-2026-09-10/README.md`

- [ ] Run the full Python suite and record pass/skip/fail/duration.
- [ ] Run `node --test tests/research_ui.cjs`.
- [ ] On fresh PostgreSQL 17, apply migrations 001–009 twice and run all foundation behavior tests.
- [ ] Run `python -m compileall`, `git diff --check`, JSON parsing, Markdown links, and secret scan.
- [ ] Answer all 18 semantic completion questions YES/NO with evidence; any architecture-affecting NO is a merge blocker.
- [ ] Commit and push the corrective head, wait for GitHub CI, and do not call the PR merge-ready while CI is pending or failing.
- [ ] Stop after the corrected foundation; do not begin target feature implementation.
