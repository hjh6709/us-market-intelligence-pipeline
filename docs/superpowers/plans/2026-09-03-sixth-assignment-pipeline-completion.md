# Sixth Assignment Pipeline Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect the verified 77-event × 10-symbol market-context collector to Airflow, persist run-level quality checks and alerts, demonstrate a safe failure/recovery path, and make the sixth-assignment documentation match executable evidence.

**Architecture:** Keep the existing raw-trade Kafka/Spark replay DAG unchanged as the representative raw validation path. Add a separate parameterized market-context backfill DAG whose mapped unit is one event and one symbol, with PostgreSQL run/work/check records as the source of truth and XCom carrying summaries only. A failed work item records an open alert and fails closed; a retry Upserts the same business keys and resolves the alert after coverage verification.

**Tech Stack:** Python 3.13, Airflow 3.3 TaskFlow/Dynamic Task Mapping, Alpaca Historical Bars API, PostgreSQL, PySpark 4.2, unittest, JSON evidence, SVG/PNG documentation

**Spec:** `docs/superpowers/specs/2026-09-03-pipeline-to-backtest-design.md`

## Global Constraints

- Do not claim that the 77 × 10 batch collector already runs through Airflow until an actual DAG run is recorded.
- Keep raw-trade Kafka/Spark replay separate from provider-bar backfill.
- Airflow XCom may contain identifiers, counts, paths, checksums and statuses, but never raw trade or bar arrays.
- The default mapped work unit is one `economic_event_id + symbol` and retries must not increase final business-key row counts.
- Use the existing ten-symbol `config/market_universe.json` without expanding it during this plan.
- Preserve `COMPLETE`, `PARTIAL`, `MARKET_CLOSED`, `NO_MARKET_DATA` and `FUTURE_SESSION_UNAVAILABLE` semantics; never invent missing prices.
- A fallback may use only an archive whose request scope, row count and checksum are verified; otherwise fail closed.
- Never commit API keys, database URLs, raw Parquet, Airflow metadata DBs or screenshots containing secrets.
- Preserve unrelated user changes in README, presentation and prior-assignment files.

---

## File Structure

- `db/migrations/006_pipeline_runs.sql`: run, work-item and quality-check persistence.
- `src/pipeline_run_tracking.py`: typed run/work/check records and PostgreSQL Upsert/query helpers.
- `src/market_context_backfill.py`: reusable selection, one-event/one-symbol collection, derivation, coverage and summary logic extracted from the CLI.
- `scripts/collect_market_event_context.py`: thin CLI adapter over `market_context_backfill`.
- `dags/market_context_backfill_pipeline.py`: parameterized mapped Airflow DAG.
- `scripts/configure_airflow_pools.py`: idempotently creates the four local Airflow pools through the Airflow CLI.
- `scripts/run_pipeline_alert_drill.py`: safe deterministic API-failure and retry drill using a local fake client and PostgreSQL checks.
- `scripts/evidence/sixth_assignment_summary.sql`: stage counts, unresolved alerts, coverage and duplicate checks.
- `tests/test_pipeline_run_tracking.py`: SQL contract and status-transition tests.
- `tests/test_market_context_backfill.py`: one-work-item collection, partial coverage and retry tests.
- `tests/test_market_context_backfill_dag.py`: DAG parameters, mapping order, pools and XCom contract tests.
- `tests/test_pipeline_alert_drill.py`: forced failure, open alert, retry and resolved alert tests.
- `docs/diagrams/pipeline-architecture.svg` and `.png`: current implemented paths and proposed later analysis boundary.
- `docs/load-recovery-assignment.md`: sixth-assignment checklist, actual execution evidence and remaining work.
- `docs/09.03_대본.md`: four-minute presentation script tied to exact document sections.
- `README.md`: project entry point and link to the sixth-assignment document only; detailed evidence remains in the assignment document.

---

### Task 1: Persist Pipeline Runs, Work Items and Alerts

**Files:**
- Create: `db/migrations/006_pipeline_runs.sql`
- Create: `src/pipeline_run_tracking.py`
- Create: `tests/test_pipeline_run_tracking.py`
- Modify: `tests/test_macro_migration.py`

**Interfaces:**
- Produces: `PipelineRun`, `PipelineWorkItem`, `PipelineCheck` dataclasses.
- Produces: `start_pipeline_run(database_url, run) -> None`.
- Produces: `mark_work_item(database_url, item) -> None`.
- Produces: `record_pipeline_check(database_url, check) -> None`.
- Produces: `finish_pipeline_run(database_url, pipeline_run_id, status) -> None`.
- Produces: `canonical_config_hash(config: Mapping[str, object]) -> str`.

- [ ] **Step 1: Write migration and transition contract tests**

```python
def test_migration_declares_run_work_and_check_business_keys(self):
    migration = Path("db/migrations/006_pipeline_runs.sql").read_text()
    self.assertIn("CREATE TABLE IF NOT EXISTS pipeline_runs", migration)
    self.assertIn("config_hash TEXT NOT NULL", migration)
    self.assertIn("UNIQUE (pipeline_run_id, economic_event_id, symbol, stage)", migration)
    self.assertIn("alert_status TEXT NOT NULL", migration)

def test_config_hash_ignores_mapping_order(self):
    self.assertEqual(
        canonical_config_hash({"symbols": ["SPY"], "feed": "sip"}),
        canonical_config_hash({"feed": "sip", "symbols": ["SPY"]}),
    )
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `.venv/bin/python -m unittest tests.test_pipeline_run_tracking tests.test_macro_migration -v`

Expected: failure because migration and module do not exist.

- [ ] **Step 3: Add the migration and tracking helpers**

The migration must define:

```sql
pipeline_runs(
  pipeline_run_id TEXT PRIMARY KEY,
  dag_id TEXT NOT NULL,
  config_json JSONB NOT NULL,
  config_hash TEXT NOT NULL,
  data_cutoff TIMESTAMPTZ NOT NULL,
  code_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ
)

pipeline_work_items(
  pipeline_run_id TEXT REFERENCES pipeline_runs,
  economic_event_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','DATA_NOT_AVAILABLE')),
  attempt_count INTEGER NOT NULL,
  manifest_path TEXT,
  input_count BIGINT,
  output_count BIGINT,
  error_code TEXT,
  error_message TEXT,
  PRIMARY KEY (pipeline_run_id, economic_event_id, symbol, stage)
)

pipeline_run_checks(
  pipeline_run_id TEXT REFERENCES pipeline_runs,
  economic_event_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  stage TEXT NOT NULL,
  check_name TEXT NOT NULL,
  expected_value TEXT,
  actual_value TEXT,
  status TEXT NOT NULL CHECK (status IN ('PASS','WARN','FAIL')),
  alert_status TEXT NOT NULL CHECK (alert_status IN ('NONE','OPEN','RESOLVED')),
  checked_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pipeline_run_id, economic_event_id, symbol, stage, check_name)
)
```

All error messages must pass through a redactor that removes PostgreSQL credentials and API-key query parameters before Upsert.

- [ ] **Step 4: Run focused tests and the migration tests**

Run: `.venv/bin/python -m unittest tests.test_pipeline_run_tracking tests.test_macro_migration -v`

Expected: all tests pass.

- [ ] **Step 5: Commit the tracking foundation**

```bash
git add db/migrations/006_pipeline_runs.sql src/pipeline_run_tracking.py tests/test_pipeline_run_tracking.py tests/test_macro_migration.py
git commit -m "feat: track pipeline work and quality alerts"
```

### Task 2: Extract One-Event/One-Symbol Market Context Processing

**Files:**
- Create: `src/market_context_backfill.py`
- Create: `tests/test_market_context_backfill.py`
- Modify: `scripts/collect_market_event_context.py`

**Interfaces:**
- Consumes: `EconomicRelease`, `HistoricalBar`, existing `fetch_all_bars`, `upsert_historical_bars`, `aggregate_derived_bars`, `upsert_derived_bars`.
- Produces: `MarketContextWorkItem(event_id, event_type, release_date, released_at, symbol, feed)`.
- Produces: `MarketContextResult(event_id, symbol, session_1m_rows, derived_3m_rows, derived_5m_rows, daily_rows, daily_before, daily_event, daily_after, coverage_status, pages, fallback_used)`.
- Produces: `select_market_context_work(config) -> list[MarketContextWorkItem]`.
- Produces: `collect_market_context_work_item(item, client, database_url, provider_available_until) -> MarketContextResult`.

- [ ] **Step 1: Write work-item and collection tests**

```python
def test_selection_builds_one_work_item_per_event_and_symbol(self):
    work = select_market_context_work(config_for_one_event(["SPY", "TLT"]))
    self.assertEqual([(x.event_type, x.symbol) for x in work], [("FOMC", "SPY"), ("FOMC", "TLT")])

def test_one_work_item_preserves_partial_daily_coverage(self):
    result = collect_market_context_work_item(
        item=fomc_tlt_item(), client=fake_bars_client(after_sessions=2),
        database_url="postgresql://test", provider_available_until=AVAILABLE,
        bar_writer=fake_writer,
    )
    self.assertEqual((result.daily_before, result.daily_event, result.daily_after), (7, 1, 2))
    self.assertEqual(result.coverage_status, "FUTURE_SESSION_UNAVAILABLE")
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `.venv/bin/python -m unittest tests.test_market_context_backfill -v`

Expected: failure because `src.market_context_backfill` does not exist.

- [ ] **Step 3: Implement the reusable processor**

Use two provider requests per work item: one 181-minute `1Min` request and one buffered `1Day` request. Derive 3m and 5m only from selected 1m rows. Return counts and coverage; do not write raw arrays to the result. Classify:

```python
if daily.complete:
    coverage = "COMPLETE"
elif release.release_date > provider_available_until.date() - timedelta(days=12):
    coverage = "FUTURE_SESSION_UNAVAILABLE"
elif daily.event_session == 0 and not session_rows:
    coverage = "MARKET_CLOSED"
else:
    coverage = "PARTIAL"
```

The existing CLI must call the new module while preserving its current JSON keys so the already published evidence remains interpretable.

- [ ] **Step 4: Run processor and existing market-context tests**

Run: `.venv/bin/python -m unittest tests.test_market_context_backfill tests.test_market_event_context tests.test_derived_bars -v`

Expected: all tests pass.

- [ ] **Step 5: Commit the reusable processing boundary**

```bash
git add src/market_context_backfill.py scripts/collect_market_event_context.py tests/test_market_context_backfill.py
git commit -m "refactor: expose event symbol market backfill"
```

### Task 3: Add the Parameterized Market Context Airflow DAG

**Files:**
- Create: `dags/market_context_backfill_pipeline.py`
- Create: `scripts/configure_airflow_pools.py`
- Create: `tests/test_market_context_backfill_dag.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 1 tracking helpers and Task 2 work-item processor.
- Produces: Airflow DAG `market_context_backfill_pipeline`.
- Produces: Params `event_types`, `release_from`, `release_to`, `symbols`, `feed`, `data_cutoff`.
- Produces task chain: `validate_run_config -> register_run -> build_work_items -> collect_market_context.expand -> verify_run -> finish_run`.

- [ ] **Step 1: Write DAG import and contract tests**

```python
def test_dag_maps_event_symbol_work_with_bounded_resources(self):
    dag = import_module("dags.market_context_backfill_pipeline").market_context_backfill_pipeline
    self.assertEqual(dag.max_active_runs, 1)
    self.assertEqual(set(dag.params), {"event_types", "release_from", "release_to", "symbols", "feed", "data_cutoff"})
    mapped = dag.get_task("collect_market_context")
    self.assertEqual(mapped.pool, "alpaca_api_pool")
    self.assertEqual(mapped.max_active_tis_per_dag, 4)
```

Also assert that task return annotations are summaries and the DAG never returns `HistoricalBar` collections.

- [ ] **Step 2: Run the DAG test and verify it fails**

Run: `.venv/bin/python -m unittest tests.test_market_context_backfill_dag -v`

Expected: module import failure.

- [ ] **Step 3: Implement the DAG and pool configuration**

`build_work_items` returns dictionaries containing only scalar values. `collect_market_context` reconstructs the dataclass, marks `RUNNING`, calls the processor and marks `SUCCEEDED`. On exception it records a `FAIL` check with `alert_status='OPEN'`, marks the work item `FAILED`, then re-raises so Airflow shows a failed task.

`verify_run` queries PostgreSQL rather than trusting mapped XCom totals. It checks:

```text
selected work items = succeeded + data-not-available
no FAILED work items
no OPEN FAIL checks
market_bars business-key duplicates = 0
```

`scripts/configure_airflow_pools.py` runs idempotent Airflow CLI commands for `alpaca_api_pool=2`, `fred_api_pool=1`, `spark_pool=1`, `postgres_write_pool=2`.

- [ ] **Step 4: Run DAG and full non-service unit tests**

Run: `.venv/bin/python -m unittest tests.test_market_context_backfill_dag tests.test_airflow_market_replay tests.test_market_replay_dag -v`

Expected: all tests pass.

- [ ] **Step 5: Commit the Airflow backfill DAG**

```bash
git add dags/market_context_backfill_pipeline.py scripts/configure_airflow_pools.py tests/test_market_context_backfill_dag.py .github/workflows/ci.yml
git commit -m "feat: orchestrate market context backfills"
```

### Task 4: Implement and Prove Alert, Retry and Idempotent Recovery

**Files:**
- Create: `scripts/run_pipeline_alert_drill.py`
- Create: `tests/test_pipeline_alert_drill.py`
- Create: `scripts/evidence/sixth_assignment_summary.sql`
- Create: `docs/evidence/sixth-assignment/README.md`
- Create after execution: `docs/evidence/sixth-assignment/alert-failure.json`
- Create after execution: `docs/evidence/sixth-assignment/alert-recovery.json`
- Create after execution: `docs/evidence/sixth-assignment/integrity.txt`

**Interfaces:**
- Consumes: tracking helpers and one-event/one-symbol processor.
- Produces: `run_alert_drill(database_url, output_dir, fake_client) -> tuple[Path, Path]`.
- Produces: public redacted JSON with run ID, work item, failure type, alert transition, row counts and duplicate count.

- [ ] **Step 1: Write the failure/recovery test**

```python
def test_api_failure_opens_alert_and_retry_resolves_without_duplicates(self):
    failure, recovery = run_alert_drill(
        database=fake_tracking_store(),
        first_client=always_503_client(),
        retry_client=fixture_bars_client(),
    )
    self.assertEqual(failure["work_status"], "FAILED")
    self.assertEqual(failure["alert_status"], "OPEN")
    self.assertEqual(recovery["work_status"], "SUCCEEDED")
    self.assertEqual(recovery["alert_status"], "RESOLVED")
    self.assertEqual(recovery["business_key_duplicates"], 0)
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `.venv/bin/python -m unittest tests.test_pipeline_alert_drill -v`

Expected: module or function not found.

- [ ] **Step 3: Implement the deterministic drill and SQL**

The first client must always raise a sanitized `HTTPError 503`; it must not contact Alpaca. The retry uses a committed synthetic fixture or an already verified local archive and clearly labels which one. The script must exit non-zero if the alert does not transition `OPEN -> RESOLVED`, if final duplicates are non-zero, or if a credential-like string appears in JSON.

The SQL must return run status, expected/succeeded/failed work items, open alerts, 1m/3m/5m/1d counts, coverage counts and business-key duplicates.

- [ ] **Step 4: Run the drill against local PostgreSQL and export evidence**

Run:

```bash
docker compose up -d postgres
.venv/bin/python scripts/run_pipeline_alert_drill.py --env-file .env --output-dir docs/evidence/sixth-assignment
docker compose exec -T postgres psql -U market -d market -f /workspace/scripts/evidence/sixth_assignment_summary.sql
```

Expected: first execution `FAILED/OPEN`, retry `SUCCEEDED/RESOLVED`, duplicate count 0. If the compose mount differs, run the same SQL through local `psql` and record the exact command in the evidence README.

- [ ] **Step 5: Commit alert and recovery evidence**

```bash
git add scripts/run_pipeline_alert_drill.py scripts/evidence/sixth_assignment_summary.sql tests/test_pipeline_alert_drill.py docs/evidence/sixth-assignment
git commit -m "test: prove pipeline alert and recovery"
```

### Task 5: Execute a Small Real Airflow Backfill and Capture Counts

**Files:**
- Create after execution: `docs/evidence/sixth-assignment/airflow-run.json`
- Create after execution: `docs/evidence/sixth-assignment/airflow-task-states.txt`
- Create after execution: `docs/evidence/sixth-assignment/postgres-summary.txt`
- Modify: `docs/evidence/sixth-assignment/README.md`

**Interfaces:**
- Consumes: `market_context_backfill_pipeline` and local secret `.env`.
- Produces: one actual run for a confirmed event and two symbols, plus a second same-input run proving idempotency.

- [ ] **Step 1: Validate the DAG without external calls**

Run:

```bash
AIRFLOW_HOME="$PWD/airflow-runtime" .venv/bin/airflow dags list-import-errors
AIRFLOW_HOME="$PWD/airflow-runtime" .venv/bin/airflow dags show market_context_backfill_pipeline
```

Expected: no import error for the new DAG.

- [ ] **Step 2: Configure pools and run one confirmed event with two symbols**

Use `FOMC`, release date `2026-07-29`, symbols `SPY` and `TLT`, feed `sip`, cutoff `2026-09-03T00:00:00Z`. Trigger through `airflow dags test` or the local Airflow UI and save the run ID and task states.

- [ ] **Step 3: Query exact database results**

Record 1m/3m/5m/1d rows, work-item states, coverage, checks and duplicates for the run. Do not copy raw prices to Git.

- [ ] **Step 4: Run the same configuration again**

Compare business-key counts and a canonical hash over `(symbol, bar_start, timeframe, source, feed, OHLCV, trade_count, vwap)`. Expected: same row count and hash, zero business-key duplicates.

- [ ] **Step 5: Commit only redacted summaries**

```bash
git add docs/evidence/sixth-assignment
git commit -m "docs: record Airflow context backfill evidence"
```

### Task 6: Align Architecture, Assignment Document, README and Presentation

**Files:**
- Modify: `docs/diagrams/pipeline-architecture.svg`
- Regenerate: `docs/diagrams/pipeline-architecture.png`
- Modify: `docs/load-recovery-assignment.md`
- Create: `docs/09.03_대본.md`
- Modify carefully: `README.md`
- Modify: `tests/test_assignment_docs.py`

**Interfaces:**
- Consumes: actual JSON/TXT evidence from Tasks 4 and 5.
- Produces: a reader-first assignment document and four-minute script with no unverified claims.

- [ ] **Step 1: Write documentation contract tests**

Assert that the assignment document contains:

```text
현재 실제 실행 / 다음 구현 분리
기준 118,118 / 부하 7,360,804 / 저장 22,260
77회 × 10종목 provider bar의 별도 경로
1m 117,566 / 3m 43,184 / 5m 26,883 / daily unique 8,740
OPEN -> RESOLVED alert evidence
Airflow event+symbol work item
미구현: Kafka v2 partition comparison, full event impact, backtest
```

Also assert the README links to `docs/load-recovery-assignment.md` but does not duplicate the long evidence tables.

- [ ] **Step 2: Run the documentation test and verify it fails**

Run: `.venv/bin/python -m unittest tests.test_assignment_docs -v`

Expected: failure on new sixth-assignment requirements.

- [ ] **Step 3: Rewrite the submission narrative around five questions**

The document order must be:

1. What was the normal input and result?
2. What larger input was processed and where?
3. What failed and why?
4. Where was it restarted and how was integrity proved?
5. What is currently connected end-to-end and what remains?

The diagram must show two implemented paths:

```text
Raw trades -> Parquet -> Kafka -> Spark -> PostgreSQL
Official events -> Airflow -> Alpaca bars -> 1m/3m/5m/1d -> PostgreSQL
```

The dashed future boundary contains event metrics, impact, baseline and backtest. The presentation script references exact sections and uses the same counts as evidence JSON.

- [ ] **Step 4: Render the diagram and verify text/visual output**

Run the repository's existing SVG-to-PNG rendering command documented in `docs/diagrams/README.md`, then inspect the PNG for clipped labels and readable font size. Run `tests.test_assignment_docs` again.

- [ ] **Step 5: Commit the submission documents**

```bash
git add README.md docs/load-recovery-assignment.md docs/09.03_대본.md docs/diagrams/pipeline-architecture.svg docs/diagrams/pipeline-architecture.png tests/test_assignment_docs.py
git commit -m "docs: complete sixth pipeline submission"
```

### Task 7: Full Verification and Submission Gate

**Files:**
- Modify only if a verified mismatch is found: `docs/submission-checklist.md`

**Interfaces:**
- Consumes all prior tasks.
- Produces a final evidence-backed pass/fail report; it does not silently waive failed checks.

- [ ] **Step 1: Run the complete unit suite**

Run: `.venv/bin/python -m unittest discover -s tests -v`

Expected: 0 failures and only explicitly environment-gated integration skips.

- [ ] **Step 2: Run service integration tests when Kafka and PostgreSQL are healthy**

Run the existing integration commands from README with their `RUN_*_INTEGRATION=1` flags. Record any environment-gated tests separately; do not report them as passed if they were skipped.

- [ ] **Step 3: Validate DAGs and evidence consistency**

Run:

```bash
AIRFLOW_HOME="$PWD/airflow-runtime" .venv/bin/airflow dags list-import-errors
git diff --check
jq empty docs/evidence/sixth-assignment/*.json
```

Compare every headline count in README, assignment document and script against the source JSON with `rg` and `jq`.

- [ ] **Step 4: Inspect Git scope and secrets**

Run `git status --short`, `git diff --stat origin/main...HEAD` and the repository's secret scan if present. Confirm that `data/archive`, `.env`, Airflow runtime and DB dumps are not staged.

- [ ] **Step 5: Commit the final checklist only if it changed**

```bash
git add docs/submission-checklist.md
git commit -m "docs: verify sixth assignment submission"
```

The implementation is complete only when the current-assignment gate in the spec is satisfied with actual evidence. Kafka v2 partition rebalancing, historical non-CPI catalog expansion and strategy backtesting remain separately named follow-up projects rather than being presented as completed here.
