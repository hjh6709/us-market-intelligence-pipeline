# Serving Layer and Execution Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the stored economic-event research results through a browser dashboard and JSON API, reproduce one input-to-read execution in under two minutes, and expose a truthful execution-readiness contract for the eventual automated-trading system.

**Architecture:** A PostgreSQL repository reads existing final tables, a serving service combines event, bar, macro-context and strategy records, and a pure readiness policy separates historical research signals from order actions. FastAPI exposes the shared service through JSON endpoints and one dependency-free HTML dashboard; a small CLI reuses the analysis and serving functions to demonstrate input → process → store → read without external API calls or orders.

**Tech Stack:** Python 3.12–3.13, FastAPI, Uvicorn, Pydantic, psycopg 3, PostgreSQL 17, standard-library HTML/JavaScript, unittest, JSON evidence, SVG/PNG documentation

**Spec:** `docs/superpowers/specs/2026-09-04-serving-layer-design.md`

## Global Constraints

- The final project goal is automated live trading, but this implementation must never call a broker order endpoint.
- Current runtime stage is exactly `RESEARCH_ONLY`; current order action is exactly `NO_TRADE`.
- Keep `research_signal`, historical `simulation`, and `execution_readiness.order_action` as separate response fields.
- Use only stored PostgreSQL results for the live presentation; do not call Alpaca, FRED or ALFRED and do not replay the large Kafka archive.
- Reuse the existing `economic_events`, `market_bars`, `macro_event_contexts`, `macro_event_impacts`, `event_strategy_results`, `pipeline_runs`, `pipeline_work_items` and `pipeline_run_checks` tables.
- Never invent missing bars, macro values, forecasts, actuals or surprise values.
- Preserve 1m, 3m and 5m coverage semantics and expose `COMPLETE` or `PARTIAL` where stored.
- The end-to-end demo must accept `event_id` and `symbol`, finish in under two minutes on the prepared local dataset, and remain idempotent.
- Do not commit API keys, database credentials, raw Parquet, Airflow metadata, browser storage or screenshots containing secrets.
- Preserve unrelated user changes in the current dirty workspace; create an isolated worktree with `superpowers:using-git-worktrees` before executing this plan.
- Use `superpowers:test-driven-development` for every code task and `superpowers:verification-before-completion` before any completion claim.

---

## File Structure

- `pyproject.toml`: add the serving runtime and HTTP test dependencies.
- `src/serving_models.py`: Pydantic response models and literal status names shared by API and CLI.
- `src/execution_readiness.py`: pure readiness checks and stage/order-action decision logic.
- `src/serving_repository.py`: PostgreSQL-only read queries and row-to-domain mapping.
- `src/serving_service.py`: combine repository results into one event-symbol detail response.
- `src/serving_api.py`: FastAPI application factory, HTTP validation and route mapping.
- `src/templates/dashboard.html`: one-page presentation dashboard that calls the JSON API.
- `src/serving_demo.py`: selected-event analysis, Upsert, read-back and count summary orchestration.
- `scripts/run_serving_demo.py`: thin CLI for the 1–2 minute presentation run.
- `tests/test_execution_readiness.py`: pure policy tests.
- `tests/test_serving_repository.py`: SQL parameterization and mapping tests with fake connections.
- `tests/test_serving_service.py`: response-composition tests with a fake repository.
- `tests/test_serving_api.py`: API status, schema, validation and HTML rendering tests.
- `tests/test_serving_demo.py`: ordered process/store/read and idempotency tests.
- `tests/test_assignment_docs.py`: seventh-assignment documentation contract.
- `docs/evidence/serving-layer/`: redacted actual API/demo results and dashboard capture.
- `docs/serving-layer-assignment.md`: requirement-by-requirement submission document.
- `docs/09.07_대본.md`: four-minute presentation script and safe live-demo sequence.
- `docs/diagrams/pipeline-architecture.svg` and `.png`: current serving path plus clearly marked planned execution path.
- `README.md`: current run commands, serving result, architecture and implementation boundary.

---

### Task 1: Define Serving Models and the Execution-Readiness Policy

**Files:**
- Modify: `pyproject.toml`
- Create: `src/serving_models.py`
- Create: `src/execution_readiness.py`
- Create: `tests/test_execution_readiness.py`

**Interfaces:**
- Produces: `ReadinessInput(market_data_ready: bool, strategy_result_ready: bool, strategy_mean_net_return_pct: Decimal | None, forecast: Decimal | None, actual: Decimal | None, surprise: Decimal | None, paper_execution_enabled: bool, position_recovery_enabled: bool, kill_switch_enabled: bool)`.
- Produces: `evaluate_execution_readiness(value: ReadinessInput) -> ExecutionReadiness`.
- Produces: `ExecutionReadiness(stage, order_action, eligible_for_order, requires_human_approval, checks, reasons)` where `stage == "RESEARCH_ONLY"` and `order_action == "NO_TRADE"` in this release.
- Produces: Pydantic API models `EventSummary`, `ImpactView`, `SimulationView`, `ReadinessCheckView`, `ExecutionReadinessView`, `EventSymbolDetail`, `BarView`, and `StrategySummaryView`.

- [ ] **Step 1: Add the failing readiness tests**

```python
def test_research_signal_never_becomes_an_order_in_this_release(self):
    result = evaluate_execution_readiness(fully_ready_input())
    self.assertEqual(result.stage, "RESEARCH_ONLY")
    self.assertEqual(result.order_action, "NO_TRADE")
    self.assertFalse(result.eligible_for_order)

def test_negative_strategy_and_missing_surprise_are_reported_separately(self):
    result = evaluate_execution_readiness(current_project_input())
    checks = {item.name: item.status for item in result.checks}
    self.assertEqual(checks["strategy_performance"], "FAIL")
    self.assertEqual(checks["event_surprise"], "FAIL")
    self.assertIn("strategy performance gate", " ".join(result.reasons))
```

- [ ] **Step 2: Run the test and verify RED**

Run: `.venv/bin/python -m unittest tests.test_execution_readiness -v`

Expected: import failure because `src.execution_readiness` does not exist.

- [ ] **Step 3: Add dependencies and implement the smallest policy**

Add compatible bounded dependencies to `pyproject.toml`:

```toml
"fastapi>=0.115,<1",
"uvicorn>=0.34,<1",
"httpx>=0.28,<1",
```

Implement checks with these exact names:

```python
CHECK_NAMES = (
    "market_data",
    "strategy_result",
    "strategy_performance",
    "event_surprise",
    "paper_execution",
    "position_recovery",
    "kill_switch",
)
```

`strategy_performance` passes only when the aggregate mean exists and is greater than zero. `event_surprise` passes only when forecast, actual and surprise all exist. The three operational checks mirror their Boolean inputs. The release-level fail-closed override always returns `RESEARCH_ONLY` and `NO_TRADE`, even if a synthetic test input passes every check.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `.venv/bin/python -m unittest tests.test_execution_readiness -v`

Expected: all readiness tests pass.

- [ ] **Step 5: Commit the policy foundation**

```bash
git add pyproject.toml src/serving_models.py src/execution_readiness.py tests/test_execution_readiness.py
git commit -m "feat: define execution readiness contract"
```

### Task 2: Add a Read-Only PostgreSQL Repository and Serving Service

**Files:**
- Create: `src/serving_repository.py`
- Create: `src/serving_service.py`
- Create: `tests/test_serving_repository.py`
- Create: `tests/test_serving_service.py`

**Interfaces:**
- Consumes: models and `evaluate_execution_readiness` from Task 1.
- Produces: `PostgresServingRepository(database_url: str)`.
- Produces repository methods `health() -> bool`, `list_events(event_type: str | None, released_from: date | None, released_to: date | None) -> list[EventRecord]`, `list_symbols(event_id: str) -> list[str]`, `get_event(event_id: str) -> EventRecord | None`, `get_impacts(event_id: str, symbol: str) -> list[ImpactRecord]`, `get_macro_context(event_id: str) -> list[MacroContextRecord]`, `get_strategy_result(event_id: str, symbol: str) -> StrategyRecord | None`, `get_strategy_summary() -> StrategySummaryRecord`, and `get_bars(event_id: str, symbol: str, timeframe: str) -> list[BarRecord]`.
- Produces: `ServingService(repository)` with `get_event_symbol_detail(event_id: str, symbol: str) -> EventSymbolDetail` and methods corresponding to list and bar endpoints.
- Produces: `ServingNotFoundError(resource: str)` for routes to translate into HTTP 404.

- [ ] **Step 1: Write failing repository and service tests**

```python
def test_list_events_uses_bound_parameters_for_filters(self):
    repo = PostgresServingRepository("postgresql://unused", connect=fake_connect)
    repo.list_events(event_type="CPI", released_from=date(2026, 1, 1), released_to=date(2026, 8, 31))
    sql, params = fake_connect.last_execute
    self.assertNotIn("CPI", sql)
    self.assertEqual(params, ("CPI", date(2026, 1, 1), date(2026, 8, 31)))

def test_detail_keeps_signal_simulation_and_order_action_separate(self):
    detail = ServingService(fake_repository()).get_event_symbol_detail("event", "NVDA")
    self.assertEqual(detail.research_signal, "LONG")
    self.assertEqual(detail.simulation.net_return_pct, Decimal("-0.62"))
    self.assertEqual(detail.execution_readiness.order_action, "NO_TRADE")
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_serving_repository tests.test_serving_service -v`

Expected: import failures because repository and service modules do not exist.

- [ ] **Step 3: Implement parameterized reads and response composition**

Use `psycopg.connect(database_url, connect_timeout=5)` and context managers. `get_bars` must first read the event `released_at`, then constrain bars to `[released_at - 60 minutes, released_at + 120 minutes)` and allow only `1m`, `3m`, or `5m` through a constant whitelist. Do not interpolate event IDs, symbols, dates or timeframe values into SQL.

Compute strategy summary directly from `event_strategy_results` for `pre60_momentum_post60` version `v1`:

```sql
SELECT count(*), count(net_return_pct), avg(net_return_pct),
       count(*) FILTER (WHERE net_return_pct > 0)
FROM event_strategy_results
WHERE strategy_name = %s AND strategy_version = %s
```

The service maps signal `1/-1/0` to `LONG/SHORT/FLAT`, builds `SimulationView` only from stored strategy columns, and passes readiness inputs to the pure policy. Missing events, missing symbols and empty requested bar sets raise distinct `ServingNotFoundError` values.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `.venv/bin/python -m unittest tests.test_serving_repository tests.test_serving_service -v`

Expected: all repository and service tests pass.

- [ ] **Step 5: Commit the serving domain**

```bash
git add src/serving_repository.py src/serving_service.py tests/test_serving_repository.py tests/test_serving_service.py
git commit -m "feat: read final event research results"
```

### Task 3: Expose the JSON API and Presentation Dashboard

**Files:**
- Modify: `pyproject.toml`
- Create: `src/serving_api.py`
- Create: `src/templates/dashboard.html`
- Create: `tests/test_serving_api.py`

**Interfaces:**
- Consumes: `ServingService` and response models from Tasks 1–2.
- Produces: `create_app(service: ServingService | None = None) -> FastAPI`.
- Produces: module-level `app = create_app()` for `uvicorn src.serving_api:app`.
- Produces the exact routes specified in the design document.

- [ ] **Step 1: Write failing endpoint and dashboard tests**

```python
def test_detail_endpoint_returns_research_and_execution_sections(self):
    client = TestClient(create_app(fake_service()))
    response = client.get("/api/v1/events/event/symbols/NVDA")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.json()["research_signal"], "LONG")
    self.assertEqual(response.json()["execution_readiness"]["order_action"], "NO_TRADE")

def test_invalid_timeframe_is_422(self):
    client = TestClient(create_app(fake_service()))
    response = client.get("/api/v1/events/event/symbols/NVDA/bars?timeframe=15m")
    self.assertEqual(response.status_code, 422)

def test_dashboard_contains_filters_chart_and_readiness_sections(self):
    response = TestClient(create_app(fake_service())).get("/")
    self.assertIn('id="event-select"', response.text)
    self.assertIn('id="price-chart"', response.text)
    self.assertIn('id="readiness-checks"', response.text)
```

- [ ] **Step 2: Run endpoint tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_serving_api -v`

Expected: import failure because `src.serving_api` does not exist.

- [ ] **Step 3: Implement API routes and the static dashboard**

Read `DATABASE_URL` from the environment with the existing local default only when no service is injected. Translate `ServingNotFoundError` to a redacted 404 response. Validate event type against `CPI`, `EMPLOYMENT`, `PCE`, `FOMC`, symbols as uppercase 1–10 character ticker strings, and timeframe as `Literal["1m", "3m", "5m"]`.

Add the packaged template declaration so the dashboard remains available after installation:

```toml
[tool.setuptools.package-data]
src = ["templates/*.html"]
```

The HTML must use browser `fetch` against the same-origin API, SVG for the line chart, and plain CSS/JavaScript with no CDN. It must visibly label:

```text
연구 신호 — 과거 분석 결과이며 주문이 아닙니다
과거 시뮬레이션 — 실제 체결 결과가 아닙니다
자동매매 준비 상태 — RESEARCH_ONLY / NO_TRADE
```

Disable any order button; the dashboard contains no POST request and no broker credential input.

- [ ] **Step 4: Run API tests and verify GREEN**

Run: `.venv/bin/python -m unittest tests.test_serving_api -v`

Expected: all API and dashboard tests pass.

- [ ] **Step 5: Commit the web serving layer**

```bash
git add pyproject.toml src/serving_api.py src/templates/dashboard.html tests/test_serving_api.py
git commit -m "feat: serve research dashboard and API"
```

### Task 4: Build the Selected-Event End-to-End Demo

**Files:**
- Modify: `src/macro_event_impact.py`
- Modify: `src/event_strategy_backtest.py`
- Create: `src/serving_demo.py`
- Create: `scripts/run_serving_demo.py`
- Modify: `scripts/__init__.py` only if required by the repository's import pattern
- Modify: `tests/test_macro_event_impact.py`
- Modify: `tests/test_event_strategy_backtest.py`
- Create: `tests/test_serving_demo.py`

**Interfaces:**
- Extends: `macro_event_impact.calculate_and_store(database_url, *, event_types=..., symbols=..., event_ids: Sequence[str] | None = None) -> tuple[int, int]`.
- Extends: `event_strategy_backtest.calculate_and_store(database_url, *, transaction_cost_bps=..., event_ids: Sequence[str] | None = None, symbols: Sequence[str] | None = None) -> dict[str, object]`.
- Produces: `run_serving_demo(database_url: str, event_id: str, symbol: str, service: ServingService | None = None) -> ServingDemoResult`.
- CLI: `.venv/bin/python scripts/run_serving_demo.py --event-id 'CPI|2026-07|2026-08-12T12:30:00Z' --symbol NVDA --output docs/evidence/serving-layer/demo-result.json`.

- [ ] **Step 1: Write failing filter and orchestration tests**

```python
def test_analysis_queries_are_filtered_to_selected_event_and_symbol(self):
    calculate_and_store(DB, event_ids=["event"], symbols=["SPY", "NVDA"])
    sql_text, params = captured_query()
    self.assertIn("economic_event_id = ANY", sql_text)
    self.assertIn("symbol = ANY", sql_text)
    self.assertIn("event", flatten(params))

def test_demo_runs_process_store_read_in_order(self):
    result = run_serving_demo(
        "postgresql://unused", "event", "NVDA",
        impact_runner=record("process_impacts"),
        strategy_runner=record("store_strategy"),
        service=fake_service(record("read_result")),
    )
    self.assertEqual(calls, ["process_impacts", "store_strategy", "read_result"])
    self.assertEqual(result.order_action, "NO_TRADE")
```

- [ ] **Step 2: Run selected tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_macro_event_impact tests.test_event_strategy_backtest tests.test_serving_demo -v`

Expected: failures because filter arguments and demo module do not exist.

- [ ] **Step 3: Add optional filters without changing default full-run behavior**

When filters are `None`, preserve the existing 202-event/full-symbol behavior exactly. When set, apply bound `ANY(%s)` predicates. For event-impact calculation, include `SPY` automatically as the benchmark when the chosen symbol is not SPY, but return and count the requested symbol separately in the demo summary.

The demo JSON must contain:

```json
{
  "input": {"event_id": "...", "symbol": "NVDA"},
  "processing": {"events": 1, "symbols": 1, "impact_rows_upserted": 4},
  "storage": {"strategy_rows_upserted": 1, "duplicate_business_keys": 0},
  "read": {"impact_rows": 4, "bar_timeframes": ["1m", "3m", "5m"]},
  "result": {"stage": "RESEARCH_ONLY", "order_action": "NO_TRADE"}
}
```

Write the output atomically and redact database URLs from errors. A repeated run must return the same final business-key counts.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `.venv/bin/python -m unittest tests.test_macro_event_impact tests.test_event_strategy_backtest tests.test_serving_demo -v`

Expected: all existing and new tests pass.

- [ ] **Step 5: Commit the reproducible demo**

```bash
git add src/macro_event_impact.py src/event_strategy_backtest.py src/serving_demo.py scripts/run_serving_demo.py tests/test_macro_event_impact.py tests/test_event_strategy_backtest.py tests/test_serving_demo.py
git commit -m "feat: add end to end serving demo"
```

### Task 5: Run the Actual Local Demo and Capture Redacted Evidence

**Files:**
- Create: `docs/evidence/serving-layer/README.md`
- Create: `docs/evidence/serving-layer/demo-result.json`
- Create: `docs/evidence/serving-layer/api-detail.json`
- Create: `docs/evidence/serving-layer/dashboard.png`

**Interfaces:**
- Consumes the CLI and FastAPI app from Tasks 3–4.
- Produces public, secret-free evidence whose numbers come from one actual local execution.

- [ ] **Step 1: Start PostgreSQL and select a real complete event-symbol pair**

Run:

```bash
docker compose up -d --wait postgres
.venv/bin/python -c "import os, psycopg; c=psycopg.connect(os.getenv('DATABASE_URL','postgresql://market:market@localhost:55432/market')); print(c.execute(\"SELECT e.economic_event_id, i.symbol FROM economic_events e JOIN macro_event_impacts i USING (economic_event_id) WHERE i.window_name='POST_60M' AND i.coverage_status='COMPLETE' ORDER BY e.released_at DESC LIMIT 1\").fetchone())"
```

Expected: one existing event ID and symbol. Use that exact pair in subsequent commands; do not invent a preferred event.

- [ ] **Step 2: Execute the demo twice and verify idempotency**

Run the CLI twice with the selected pair and compare `macro_event_impacts` and `event_strategy_results` business-key counts before and after. Expected: both runs succeed, the JSON reports four impact windows and one strategy row, and duplicate business keys remain zero.

- [ ] **Step 3: Start the API and save redacted responses**

Run: `.venv/bin/uvicorn src.serving_api:app --host 127.0.0.1 --port 8000`

Verify `GET /health`, the selected detail endpoint, bar endpoints for all three timeframes, and `GET /api/v1/strategy/summary`. Save formatted JSON without headers, credentials or local absolute paths.

- [ ] **Step 4: Capture and visually inspect the dashboard**

Use the in-app browser against `http://127.0.0.1:8000/`, select the actual event and symbol, and capture `dashboard.png`. Inspect the full image for clipped text, readable chart labels, all seven readiness checks, `RESEARCH_ONLY`, `NO_TRADE`, and the three research/simulation/order disclaimers.

- [ ] **Step 5: Document evidence provenance and commit**

The evidence README records the execution timestamp, exact command, event ID, symbol, input/process/store/read counts, idempotency query result and the fact that no external API or broker order endpoint was called.

```bash
git add docs/evidence/serving-layer
git commit -m "docs: record serving layer execution evidence"
```

### Task 6: Update the Seventh-Assignment Documentation and Architecture

**Files:**
- Modify: `README.md`
- Create: `docs/serving-layer-assignment.md`
- Create: `docs/09.07_대본.md`
- Modify: `docs/diagrams/pipeline-architecture.svg`
- Regenerate: `docs/diagrams/pipeline-architecture.png`
- Modify: `docs/diagrams/README.md`
- Modify: `tests/test_assignment_docs.py`
- Modify: `docs/09.03_대본.md` only to remove the stray standalone `ㅋ`, preserving all other user edits

**Interfaces:**
- Consumes actual evidence from Task 5.
- Produces a submission document covering all seventh-assignment requirements and a four-minute script tied to exact screen locations.

- [ ] **Step 1: Add failing documentation-contract tests**

```python
def test_seventh_assignment_documents_actual_serving_and_boundaries(self):
    assignment = Path("docs/serving-layer-assignment.md").read_text(encoding="utf-8")
    script = Path("docs/09.07_대본.md").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(encoding="utf-8")
    for phrase in ("입력 → 처리 → 저장 → 읽기", "RESEARCH_ONLY", "NO_TRADE", "모의주문", "실전 자동매매"):
        self.assertIn(phrase, assignment)
    self.assertIn("docs/serving-layer-assignment.md", readme)
    self.assertIn("Serving API · Dashboard", diagram)
    self.assertIn("향후 주문 계층", diagram)
    self.assertNotIn("\nㅋ\n", script)
```

- [ ] **Step 2: Run documentation tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_assignment_docs -v`

Expected: failure because seventh-assignment files and labels do not exist.

- [ ] **Step 3: Write the submission document and README from actual evidence**

The assignment document follows this order:

1. 문제와 실제 데이터
2. 최신 구조와 데이터 모델
3. 한 번의 실행에서 입력·처리·저장·읽기 건수
4. 이전 부하·장애·복구에서 확인한 것과 미보장 항목
5. API와 대시보드가 저장 결과를 읽는 장면
6. 연구 신호·시뮬레이션·주문 행동의 차이
7. 실전 자동매매까지 남은 단계
8. 1~2분 발표 시연 명령과 실패 시 되돌리는 방법

The root README leads with the current serving result, includes exact startup and demo commands, and marks Slack, broker ordering, fills, positions and live trading as planned. It must not claim a dashboard capture or row count different from Task 5 evidence.

- [ ] **Step 4: Update and render the architecture diagram**

Add a solid implemented lane:

```text
PostgreSQL → FastAPI ServingService → Browser Dashboard
```

Add a clearly styled planned lane:

```text
Strategy approval → Risk engine → Slack approval → Broker adapter
→ Order/fill/position recovery → Limited live → Automated live
```

Render SVG to PNG using the command documented in `docs/diagrams/README.md`, then inspect the PNG for clipped labels and for an unmistakable visual distinction between implemented and planned paths.

- [ ] **Step 5: Write the four-minute script and run documentation tests**

The script shows only: conclusion, current architecture, one count table, dashboard, readiness panel, previous failure/recovery evidence and next steps. The live sequence is health → one selected event → one demo command → refreshed dashboard and must fit within two minutes.

Run: `.venv/bin/python -m unittest tests.test_assignment_docs -v`

Expected: all documentation contract tests pass.

- [ ] **Step 6: Commit the final documentation**

```bash
git add README.md docs/serving-layer-assignment.md docs/09.07_대본.md docs/09.03_대본.md docs/diagrams/pipeline-architecture.svg docs/diagrams/pipeline-architecture.png docs/diagrams/README.md tests/test_assignment_docs.py
git commit -m "docs: complete serving layer assignment"
```

### Task 7: Full Verification, Security Review and Submission Readiness

**Files:**
- Modify only files required to fix verified failures from Tasks 1–6.

**Interfaces:**
- Produces a clean, evidence-backed branch ready for code review and PR integration.

- [ ] **Step 1: Synchronize dependencies and run the complete test suite**

Run:

```bash
uv sync --extra airflow
.venv/bin/python -m unittest discover -s tests -v
```

Expected: all tests pass; existing environment-dependent skips remain explicitly reported rather than converted to passes.

- [ ] **Step 2: Run static repository checks**

Run:

```bash
git diff --check
rg -n "APCA_API_SECRET|FRED_API_KEY=.+|postgresql://[^:]+:[^@]+@|BEGIN (RSA|OPENSSH) PRIVATE KEY" README.md docs src scripts tests
rg -n "미완료|현재 구현.*실주문|실전 자동매매.*구현" README.md docs/serving-layer-assignment.md docs/09.07_대본.md
```

Expected: no whitespace errors, no committed secret values, no placeholders, and no claim that broker ordering is currently implemented.

- [ ] **Step 3: Re-run the safe presentation path**

Start PostgreSQL and the API, run the exact documented demo command, fetch the selected detail JSON, and open the dashboard. Expected: the output matches committed evidence; stage is `RESEARCH_ONLY`; action is `NO_TRADE`; no external provider or order request appears in logs.

- [ ] **Step 4: Review the diff against the approved spec**

Check every section of `docs/superpowers/specs/2026-09-04-serving-layer-design.md` against changed files. Confirm that the implemented API routes, dashboard sections, demo evidence, README, diagram and presentation script are all present, and that out-of-scope broker features remain labeled as planned.

- [ ] **Step 5: Request code review and fix only verified issues**

Use `superpowers:requesting-code-review`. Because proactive subagents are disabled for this task, perform inline review unless the user explicitly requests delegated review. Re-run the focused tests for every fix and then repeat the full suite.

- [ ] **Step 6: Commit verification fixes if any**

```bash
git add pyproject.toml src/serving_models.py src/execution_readiness.py src/serving_repository.py src/serving_service.py src/serving_api.py src/templates/dashboard.html src/serving_demo.py src/macro_event_impact.py src/event_strategy_backtest.py scripts/run_serving_demo.py tests/test_execution_readiness.py tests/test_serving_repository.py tests/test_serving_service.py tests/test_serving_api.py tests/test_serving_demo.py tests/test_macro_event_impact.py tests/test_event_strategy_backtest.py tests/test_assignment_docs.py README.md docs/serving-layer-assignment.md docs/09.07_대본.md docs/09.03_대본.md docs/diagrams/pipeline-architecture.svg docs/diagrams/pipeline-architecture.png docs/diagrams/README.md docs/evidence/serving-layer
git commit -m "fix: address serving layer verification findings"
```

- [ ] **Step 7: Present integration options**

Use `superpowers:finishing-a-development-branch` only after the full verification output is fresh. Report exact passed/skipped counts, actual demo duration, evidence paths and any remaining limitation before offering PR creation or merge.
