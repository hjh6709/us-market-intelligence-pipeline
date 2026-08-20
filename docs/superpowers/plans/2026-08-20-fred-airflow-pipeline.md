# FRED/ALFRED Airflow Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** FRED/ALFRED 9개 series의 metadata와 observation/vintage를 Airflow logical date 기준으로 수집하고 PostgreSQL에 멱등 저장하며, 공개 저장소에 안전한 실행 증거를 만든다.

**Architecture:** Airflow와 무관한 Python core가 HTTP, 정규화, validation과 PostgreSQL transaction을 담당한다. Airflow 3.3.0 DAG는 9개 series를 mapped task로 실행하고 retry·window·quality gate만 관리하며, Docker Compose `batch` profile에서 앱 runtime과 분리한다.

**Tech Stack:** Python 3.14, standard-library HTTP, psycopg 3, PostgreSQL 17.6, Apache Airflow 3.3.0 Docker image, Docker Compose, `unittest`

**Spec:** `docs/superpowers/specs/2026-08-20-fred-airflow-pipeline-design.md`

## Global Constraints

- Series는 `CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE`, `UNRATE`, `DFF`, `DGS2`, `DGS10`, `VIXCLS`로 고정한다.
- Observation unique key는 `(series_id, observation_date, realtime_start)`다.
- `observation_date`를 `released_at`으로 변환하지 않는다.
- FRED `.`은 `NULL`로 저장하고 다른 비숫자 value는 거부한다.
- Airflow는 `apache-airflow==3.3.0`으로 고정하고 Compose `batch` profile에서만 실행한다.
- Airflow task retry는 3회, exponential backoff, 최대 지연 15분, 실행 timeout 2분이다.
- `.env`, API key, key가 포함된 URL, raw HTTP header/payload, Airflow runtime/log/metadata DB, DB dump와 발표 캡처는 Git에 추가하지 않는다.
- Evidence JSON에는 count, date range와 missing count만 기록한다.
- 모든 production behavior는 실패하는 테스트를 먼저 확인한 뒤 구현한다.

---

### Task 1: 공개 저장소 보안 경계와 macro value objects

**Files:**
- Modify: `.gitignore`
- Modify: `.env.example`
- Create: `src/macro_models.py`
- Create: `tests/fixtures/fred/series.json`
- Create: `tests/fixtures/fred/observations.json`
- Create: `tests/test_macro_models.py`

**Interfaces:**
- Produces: `FRED_SERIES: tuple[str, ...]`
- Produces: `MacroSeries.from_fred(payload: dict, ingested_at: datetime) -> MacroSeries`
- Produces: `MacroObservation.from_fred(series_id: str, payload: dict, ingested_at: datetime) -> MacroObservation`
- Produces: immutable dataclasses whose fields match the migration in Task 3.

- [ ] **Step 1: Write failing model tests and sanitized fixtures**

Use a minimal public fixture with no request URL, key, header or request id. Tests must assert all typed fields, Decimal conversion, `.` to `None`, invalid numeric rejection and `realtime_start > realtime_end` rejection.

```python
def test_missing_fred_value_is_preserved_as_none(self):
    row = MacroObservation.from_fred(
        "DGS10",
        {
            "realtime_start": "2026-08-19",
            "realtime_end": "9999-12-31",
            "date": "2026-08-19",
            "value": ".",
        },
        datetime(2026, 8, 20, tzinfo=UTC),
    )
    self.assertIsNone(row.value)
```

- [ ] **Step 2: Run the model tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_macro_models -v`

Expected: import failure because `src.macro_models` does not exist.

- [ ] **Step 3: Implement immutable validated models and registry**

Use `@dataclass(frozen=True, slots=True)`, `date.fromisoformat`, `datetime.fromisoformat` and `Decimal`. Require timezone-aware `last_updated` and `ingested_at`. Preserve `9999-12-31` as a valid `date`.

```python
FRED_SERIES = (
    "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "UNRATE",
    "DFF", "DGS2", "DGS10", "VIXCLS",
)
```

- [ ] **Step 4: Tighten Git exclusions before any runtime is created**

Add these patterns without ignoring `.env.example`:

```gitignore
.env*
!.env.example
airflow-runtime/
logs/
*.db
*.sqlite*
*.dump
*.backup
docs/evidence/**/captures/
```

Add only an empty `FRED_API_KEY=` to `.env.example`.

- [ ] **Step 5: Run tests and security-path checks**

Run:

```bash
.venv/bin/python -m unittest tests.test_macro_models -v
git check-ignore .env airflow-runtime/airflow.db logs/task.log local.dump docs/evidence/fred-airflow/captures/ui.png
git check-ignore .env.example
```

Expected: model tests pass; sensitive examples are ignored; `.env.example` is not ignored.

- [ ] **Step 6: Commit**

```bash
git add .gitignore .env.example src/macro_models.py tests/fixtures/fred tests/test_macro_models.py
git commit -m "feat: validate FRED macro records"
```

---

### Task 2: FRED HTTP client and error classification

**Files:**
- Create: `src/fred.py`
- Create: `tests/test_fred.py`

**Interfaces:**
- Consumes: FRED payload contracts from Task 1.
- Produces: `FredClient(api_key: str, timeout_seconds: float = 15.0, opener=urlopen)`
- Produces: `fetch_series(series_id: str, *, as_of: date) -> dict`
- Produces: `fetch_observations(series_id: str, window: FredWindow) -> list[dict]`
- Produces: `FredWindow(realtime_start, realtime_end, observation_start, observation_end)`
- Produces: `FredRateLimitError`, `FredTimeoutError`, `FredTransportError`, `FredContractError`.

- [ ] **Step 1: Write failing request-contract tests**

Inject a recording opener. Assert HTTPS host, endpoint path, `file_type=json`, series and all four date bounds. The fake response returns bytes through a context manager. Do not print or assert the API key value.

```python
def test_observation_request_contains_point_in_time_window(self):
    client = FredClient("test-key", opener=self.opener)
    rows = client.fetch_observations("DGS10", self.window)
    self.assertEqual(len(rows), 2)
    self.assertEqual(self.query_without_api_key["output_type"], ["1"])
```

- [ ] **Step 2: Write failing transport and contract tests**

Test an opener that raises `HTTPError(..., 429, ...)`, one that raises `TimeoutError`, a 500 response, invalid JSON, and JSON without `observations`/`seriess`. Assert only typed exceptions and never exception text containing the key.

- [ ] **Step 3: Run client tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_fred -v`

Expected: import failure because `src.fred` does not exist.

- [ ] **Step 4: Implement minimal standard-library client**

Build URLs with `urlencode`, open with `urllib.request.urlopen`, parse JSON, and return only the endpoint collection. Implement a private `_request_json(path, params)` that redacts the key from all raised messages. Do not add an internal retry loop; Airflow owns retries.

- [ ] **Step 5: Run focused and full unit tests**

Run:

```bash
.venv/bin/python -m unittest tests.test_fred -v
.venv/bin/python -m unittest discover -s tests -v
```

Expected: all unit tests pass; external integration tests remain explicitly skipped.

- [ ] **Step 6: Commit**

```bash
git add src/fred.py tests/test_fred.py
git commit -m "feat: fetch point-in-time FRED data"
```

---

### Task 3: PostgreSQL macro schema and transactional repository

**Files:**
- Create: `db/migrations/002_macro_observations.sql`
- Create: `src/macro_repository.py`
- Create: `tests/test_macro_repository.py`
- Create: `tests/integration/test_macro_repository.py`

**Interfaces:**
- Consumes: `MacroSeries`, `MacroObservation` from Task 1.
- Produces: `upsert_macro_batch(series, observations, *, database_url) -> MacroUpsertResult`
- Produces: `read_macro_quality(*, database_url, expected_series) -> MacroQualityResult`
- `MacroUpsertResult` contains `series_id`, `observation_count`, `missing_count`.

- [ ] **Step 1: Write failing serialization tests**

Assert model-to-parameter tuple ordering and verify a mixed-series observation list is rejected before connecting.

- [ ] **Step 2: Run serialization tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_macro_repository -v`

Expected: import failure because `src.macro_repository` does not exist.

- [ ] **Step 3: Write migration and minimal repository**

Create the exact two tables and checks from the design. Add indexes:

```sql
CREATE INDEX macro_observations_as_of_idx
ON macro_observations (series_id, observation_date DESC, realtime_start DESC);

CREATE INDEX macro_observations_vintage_idx
ON macro_observations (series_id, realtime_start, observation_date);
```

Use one psycopg transaction for series metadata plus all observations. Use `ON CONFLICT ... DO UPDATE` and `SET TIME ZONE 'UTC'`.

- [ ] **Step 4: Run unit tests and verify GREEN**

Run: `.venv/bin/python -m unittest tests.test_macro_repository -v`

Expected: all repository unit tests pass without a live DB.

- [ ] **Step 5: Write PostgreSQL integration tests**

Cover these exact assertions:

```text
first fixture upsert: series=1, observations=2
same fixture second upsert: series=1, observations still=2
same business key revised value: observations=2 and value updated
invalid realtime range in a direct SQL batch: entire transaction rolled back
missing value: SQL NULL and missing count=1
```

- [ ] **Step 6: Run integration tests and verify behavior**

Run:

```bash
docker compose up -d --wait postgres
RUN_MACRO_POSTGRES_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_macro_repository -v
```

Expected: all macro repository integration tests pass against port `55432`.

- [ ] **Step 7: Commit**

```bash
git add db/migrations/002_macro_observations.sql src/macro_repository.py tests/test_macro_repository.py tests/integration/test_macro_repository.py
git commit -m "feat: persist FRED vintages in PostgreSQL"
```

---

### Task 4: One-series pipeline and logical/backfill window

**Files:**
- Create: `src/fred_pipeline.py`
- Create: `tests/test_fred_pipeline.py`

**Interfaces:**
- Consumes: `FredClient`, model factories and `upsert_macro_batch`.
- Produces: `resolve_fred_window(logical_date: date, conf: Mapping[str, str]) -> FredWindow`
- Produces: `ingest_fred_series(series_id: str, window: FredWindow, *, client, database_url: str, clock) -> FredIngestionSummary`
- Produces: JSON-safe summary fields `series_id`, `raw_count`, `normalized_count`, `missing_count`, `upserted_count`, `observation_start`, `observation_end`.

- [ ] **Step 1: Write failing daily and manual window tests**

Daily logical date `2026-08-20` must resolve real-time dates `2026-08-14` through `2026-08-20`. Manual conf must require all four date fields and reject inverted ranges or future `observation_end` relative to `realtime_end`.

- [ ] **Step 2: Write failing orchestration tests**

Use small fakes, not network or DB mocks tied to implementation details. Assert metadata and observations are fetched once, normalized, sent as one repository batch, and summarized without raw payload/key.

- [ ] **Step 3: Run pipeline tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_fred_pipeline -v`

Expected: import failure because `src.fred_pipeline` does not exist.

- [ ] **Step 4: Implement minimal orchestration and JSON-safe summary**

Keep dependency injection at the boundary. Do not read Airflow context, `.env` or global secrets inside the pure functions. The DAG supplies key, database URL, window and UTC clock.

- [ ] **Step 5: Run focused and full unit tests**

Run:

```bash
.venv/bin/python -m unittest tests.test_fred_pipeline -v
.venv/bin/python -m unittest discover -s tests -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/fred_pipeline.py tests/test_fred_pipeline.py
git commit -m "feat: orchestrate FRED series ingestion"
```

---

### Task 5: Airflow DAG and isolated batch profile

**Files:**
- Create: `dags/fred_macro_dag.py`
- Create: `docker/airflow/Dockerfile`
- Modify: `compose.yml`
- Create: `tests/test_fred_macro_dag_contract.py`

**Interfaces:**
- Consumes: `FRED_SERIES`, `resolve_fred_window`, `ingest_fred_series`, `read_macro_quality`.
- Produces: DAG id `fred_macro_daily`, schedule `0 14 * * *`, mapped task id `ingest_series`, final task id `quality_gate`.
- Produces: Compose service `airflow` under profile `batch`, bound to `127.0.0.1:8080`.

- [ ] **Step 1: Write failing static DAG contract tests**

Parse the DAG source with `ast` without requiring Airflow in the app venv. Assert the pinned DAG id, schedule, `catchup=False`, 9-series expansion, retry/backoff/timeout constants, and absence of literal API keys or connection URLs.

- [ ] **Step 2: Run DAG contract tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_fred_macro_dag_contract -v`

Expected: failure because `dags/fred_macro_dag.py` does not exist.

- [ ] **Step 3: Implement the thin Airflow TaskFlow DAG**

Use Airflow 3 public imports from `airflow.sdk`. Read `FRED_API_KEY` and `DATABASE_URL` only inside task execution. Resolve optional manual dates from `dag_run.conf`. Return mapped summaries only, then verify all 9 expected series and PostgreSQL quality counts.

```python
@dag(
    dag_id="fred_macro_daily",
    schedule="0 14 * * *",
    start_date=datetime(2026, 8, 20, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=2),
    },
)
```

- [ ] **Step 4: Add the isolated Airflow image and Compose profile**

Base `docker/airflow/Dockerfile` on `apache/airflow:3.3.0`, install only psycopg binary needed by the core, and never copy `.env` or runtime files. Mount `dags/` and `src/` read-only. Use a named `airflow_runtime` volume and `command: standalone`. Bind UI only to `127.0.0.1`.

- [ ] **Step 5: Validate static contract and Compose**

Run:

```bash
.venv/bin/python -m unittest tests.test_fred_macro_dag_contract -v
docker compose config -q
docker compose --profile batch build airflow
docker compose --profile batch run --rm airflow airflow dags list
```

Expected: the DAG appears once with no import errors and the image build context contains no `.env`.

- [ ] **Step 6: Test one fixture-backed DAG task path**

Add a task-test mode that points the client to a local fixture transport without adding raw responses to XCom. Run `airflow tasks test` for window resolution and quality contract. Do not make a real API call in this step.

- [ ] **Step 7: Commit**

```bash
git add dags/fred_macro_dag.py docker/airflow/Dockerfile compose.yml tests/test_fred_macro_dag_contract.py
git commit -m "feat: schedule FRED ingestion with Airflow"
```

---

### Task 6: Optional live smoke, deterministic evidence and documentation

**Files:**
- Create: `scripts/fred_live_smoke.py`
- Create: `scripts/evidence/fred_airflow_evidence.py`
- Create: `scripts/evidence/macro_observation_evidence.sql`
- Create: `docs/evidence/fred-airflow/README.md`
- Create: `docs/evidence/fred-airflow/result.json`
- Create: `docs/test-results/2026-08-20-fred-airflow.md`
- Modify: `README.md`
- Modify: `PROJECT_PLAN.md`
- Modify: `docs/course-alignment.md`
- Modify: `.github/workflows/ci.yml`
- Create: `tests/test_fred_live_smoke.py`

**Interfaces:**
- Consumes: Tasks 1–5 public APIs.
- Produces: `python -m scripts.fred_live_smoke --series DGS10 --observation-start YYYY-MM-DD --observation-end YYYY-MM-DD`.
- Produces: sanitized `docs/evidence/fred-airflow/result.json` and duplicate-check SQL.

- [ ] **Step 1: Write failing live-smoke output tests**

Inject a fake client and assert output contains series id, counts and date range but not raw payload, API key, request URL or database URL. Missing `FRED_API_KEY` must fail with a short setup message.

- [ ] **Step 2: Run smoke tests and verify RED**

Run: `.venv/bin/python -m unittest tests.test_fred_live_smoke -v`

Expected: import failure because the script does not exist.

- [ ] **Step 3: Implement the optional smoke and evidence runner**

The live smoke fetches one series only and never writes raw data. The deterministic evidence runner uses sanitized fixtures, runs all 9 configured series through normalization/upsert, repeats the same logical window, queries row counts, and writes only the agreed JSON fields.

- [ ] **Step 4: Execute deterministic PostgreSQL evidence twice**

Run:

```bash
docker compose up -d --wait postgres
.venv/bin/python -m scripts.evidence.fred_airflow_evidence
.venv/bin/python -m scripts.evidence.fred_airflow_evidence
docker compose exec -T postgres psql -U market -d market -f /dev/stdin < scripts/evidence/macro_observation_evidence.sql
```

Expected: 9 `macro_series`, a fixed observation count, identical second-run business count, zero duplicate business keys and at least one SQL `NULL` value.

- [ ] **Step 5: Run optional real API smoke only when key exists**

Run: `.venv/bin/python -m scripts.fred_live_smoke --series DGS10 --observation-start 2026-08-13 --observation-end 2026-08-20`

Expected: sanitized count/date summary. If no key is configured, record `not run: FRED_API_KEY unavailable`; never invent a successful result.

- [ ] **Step 6: Update CI and user documentation**

CI runs unit tests and `RUN_MACRO_POSTGRES_INTEGRATION=1` tests against the existing PostgreSQL service. README explains what FRED contributes, why vintage is stored, `batch` profile commands, daily versus manual backfill and current scope boundary.

- [ ] **Step 7: Run complete verification**

Run:

```bash
.venv/bin/python -m unittest discover -s tests -v
RUN_MACRO_POSTGRES_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_macro_repository -v
.venv/bin/python -m compileall -q src scripts tests dags
uv lock --check
docker compose config -q
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 8: Run the public-repository security gate**

Run tracked/staged file scans for these categories, then manually read the staged diff:

```text
FRED_API_KEY with a non-empty value
fred API URL containing api_key query value
BEGIN ... PRIVATE KEY
tracked .env / .db / .sqlite / .dump / .backup / runtime log / capture
database URL using credentials other than documented local market:market
```

Any real key discovery stops the commit and requires key revocation before proceeding.

- [ ] **Step 9: Commit**

```bash
git add .github/workflows/ci.yml README.md PROJECT_PLAN.md docs/course-alignment.md docs/evidence/fred-airflow docs/test-results/2026-08-20-fred-airflow.md scripts tests/test_fred_live_smoke.py
git commit -m "docs: record FRED Airflow pipeline evidence"
```
