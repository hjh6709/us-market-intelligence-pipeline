# Pipeline Load and Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and execute a reproducible 2022-01-01 through 2026-08-12 CPI-window load, failure, and recovery experiment on GCP using archived Alpaca SIP trades and point-in-time FRED/ALFRED macro context.

**Architecture:** Collect external data once into partitioned Parquet plus checksummed manifests, then replay only the local archive through Kafka, Spark, and PostgreSQL. A parameterized experiment runner records baseline, load, fault, and recovery runs while small JSON summaries and sanitized screenshots provide assignment evidence.

**Tech Stack:** Python 3.13, PyArrow, Alpaca Historical Trades API, FRED/ALFRED API, Airflow 3.3, Kafka, Spark 4.2, PostgreSQL, Docker Compose, GCP Compute Engine

**Spec:** `docs/superpowers/specs/2026-08-31-pipeline-load-recovery-design.md`

## Global Constraints

- External Alpaca and FRED APIs are collected once with bounded retry; load is applied only to archived local data.
- The load dataset covers actual CPI releases from 2022-01-01 through 2026-08-12 and `SPY`, `QQQ`, `SMH`, `NVDA`.
- The load run must contain at least 1,181,180 raw trades.
- API keys, passwords, DSNs, raw payloads, Parquet archives, Airflow metadata, and complete logs must not be committed.
- Existing dirty user files are not staged, rewritten, or removed unless this plan explicitly names them.
- Every success claim must be backed by current-run JSON, SQL output, or a screenshot derived from those artifacts.

---

### Task 1: Point-in-time macro context contract

**Files:**
- Create: `db/migrations/003_pipeline_experiments.sql`
- Create: `src/macro_context_ingestion.py`
- Modify: `src/cpi_ingestion.py`
- Test: `tests/test_macro_context_ingestion.py`
- Test: `tests/test_macro_migration.py`

**Interfaces:**
- Consumes: `FredClient.fetch_observations(...)`, `CpiRelease`, existing `macro_series`, `macro_observations`, and `economic_events` tables.
- Produces: `MacroSeriesSpec`, `EventMacroContext`, `fetch_event_macro_context(...)`, `upsert_event_macro_context(...)`, and the `macro_event_contexts`, `pipeline_experiment_runs`, `pipeline_experiment_failures` tables.

- [ ] **Step 1: Write failing tests for the ten-series catalog and point-in-time selection**

```python
def test_catalog_contains_required_project_series():
    assert set(MACRO_SERIES) == {
        "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "UNRATE",
        "PAYEMS", "DFF", "DGS2", "DGS10", "VIXCLS",
    }

def test_context_never_selects_observation_after_release():
    selected = select_latest_available(observations, as_of=date(2024, 1, 11))
    assert selected.observation_date <= date(2024, 1, 11)
    assert selected.realtime_start <= date(2024, 1, 11) <= selected.realtime_end
```

- [ ] **Step 2: Run the focused tests and verify they fail because the new module and migration do not exist**

Run: `.venv/bin/python -m unittest tests.test_macro_context_ingestion tests.test_macro_migration -v`

- [ ] **Step 3: Add the macro catalog and deterministic event-context selection**

Implement:

```python
@dataclass(frozen=True)
class MacroSeriesSpec:
    series_id: str
    title: str
    frequency: str
    units: str
    seasonal_adjustment: str | None

@dataclass(frozen=True)
class EventMacroContext:
    economic_event_id: str
    series_id: str
    observation_date: date
    realtime_start: date
    value: Decimal | None

def select_latest_available(
    observations: Sequence[MacroObservation], *, as_of: date
) -> MacroObservation:
    candidates = [
        item for item in observations
        if item.observation_date <= as_of and item.is_valid_on(as_of)
    ]
    if not candidates:
        raise ValueError("no point-in-time observation was available")
    return max(candidates, key=lambda item: item.observation_date)
```

The migration creates primary key `(economic_event_id, series_id)` for `macro_event_contexts`, primary key `experiment_run_id` for `pipeline_experiment_runs`, and primary key `(experiment_run_id, failure_type)` for `pipeline_experiment_failures`.

- [ ] **Step 4: Run migration and macro tests**

Run: `.venv/bin/python -m unittest tests.test_macro_context_ingestion tests.test_macro_migration -v`
Expected: PASS.

- [ ] **Step 5: Commit only the macro contract files**

```bash
git add db/migrations/003_pipeline_experiments.sql src/macro_context_ingestion.py src/cpi_ingestion.py tests/test_macro_context_ingestion.py tests/test_macro_migration.py
git commit -m "feat: add point-in-time macro experiment context"
```

### Task 2: CPI event manifest for 2022 through 2026

**Files:**
- Modify: `config/cpi_releases.json`
- Create: `scripts/validate_cpi_release_manifest.py`
- Modify: `tests/test_cpi_ingestion.py`

**Interfaces:**
- Consumes: BLS official CPI archive dates and `load_cpi_releases(...)`.
- Produces: a unique, chronologically ordered event list whose release dates are within `2022-01-01..2026-08-12`.

- [ ] **Step 1: Add failing manifest coverage tests**

```python
def test_release_manifest_covers_requested_load_period(self):
    releases = load_cpi_releases()
    self.assertEqual(releases[0].release_date.isoformat(), "2022-01-12")
    self.assertEqual(releases[-1].release_date.isoformat(), "2026-08-12")
    self.assertGreaterEqual(len(releases), 50)
    self.assertEqual(releases, sorted(releases, key=lambda item: item.released_at))
```

- [ ] **Step 2: Run the test and confirm the existing 12-event manifest fails coverage**

Run: `.venv/bin/python -m unittest tests.test_cpi_ingestion.CpiIngestionTest.test_release_manifest_covers_requested_load_period -v`

- [ ] **Step 3: Populate only confirmed BLS CPI releases and validate URLs, target months, ET timestamps, order, and uniqueness**

The validator prints only event counts and date bounds; it never stores downloaded BLS pages.

- [ ] **Step 4: Run the manifest validator and CPI tests**

Run: `.venv/bin/python scripts/validate_cpi_release_manifest.py config/cpi_releases.json`
Expected output contains `first=2022-01-12`, `last=2026-08-12`, `duplicates=0`.

Run: `.venv/bin/python -m unittest tests.test_cpi_ingestion -v`
Expected: PASS.

- [ ] **Step 5: Commit the confirmed release manifest**

```bash
git add config/cpi_releases.json scripts/validate_cpi_release_manifest.py tests/test_cpi_ingestion.py
git commit -m "data: extend confirmed CPI release manifest"
```

### Task 3: Checksummed Parquet market archive

**Files:**
- Modify: `pyproject.toml`
- Modify: `.gitignore`
- Create: `src/market_trade_archive.py`
- Test: `tests/test_market_trade_archive.py`

**Interfaces:**
- Consumes: `AlpacaHistoricalClient.fetch_page(...)`, `normalize_historical_trade(...)`, one `CpiRelease`, one symbol, and an archive root.
- Produces: `ArchivePartition`, `ArchiveManifest`, `collect_archive_partition(...)`, `read_archive_records(...)`, and JSON manifests next to ignored Parquet files.

- [ ] **Step 1: Write failing tests for page-wise writing, hashes, completed-manifest reuse, and truncated pagination rejection**

```python
def test_completed_hash_matching_partition_skips_api(tmp_path):
    first = collect_archive_partition(client, spec, archive_root=tmp_path)
    second = collect_archive_partition(failing_client, spec, archive_root=tmp_path)
    assert second == first

def test_manifest_count_matches_parquet_rows(tmp_path):
    manifest = collect_archive_partition(client, spec, archive_root=tmp_path)
    assert sum(1 for _ in read_archive_records(manifest)) == manifest.row_count
```

- [ ] **Step 2: Run tests and verify the archive module is missing**

Run: `.venv/bin/python -m unittest tests.test_market_trade_archive -v`

- [ ] **Step 3: Add `pyarrow>=21,<22`, ignore `data/archive/`, stream each API page to a temporary Parquet file, fsync, hash, then atomically rename**

Partition path:

```text
data/archive/event_type=CPI/release_date=YYYY-MM-DD/symbol=SYMBOL/trades.parquet
data/archive/event_type=CPI/release_date=YYYY-MM-DD/symbol=SYMBOL/manifest.json
```

Never retain all dataset trades in one Python list. Delete only the temporary file created by the failed collection attempt; preserve completed partitions.

- [ ] **Step 4: Run archive tests and dependency lock/install**

Run: `uv sync`

Run: `.venv/bin/python -m unittest tests.test_market_trade_archive -v`
Expected: PASS.

- [ ] **Step 5: Commit archive implementation**

```bash
git add pyproject.toml uv.lock .gitignore src/market_trade_archive.py tests/test_market_trade_archive.py
git commit -m "feat: archive historical trades as parquet partitions"
```

### Task 4: Archived Kafka replay and measured experiment result

**Files:**
- Create: `src/archive_kafka_replay.py`
- Create: `src/pipeline_experiment.py`
- Create: `scripts/evidence/pipeline_experiment_summary.sql`
- Test: `tests/test_archive_kafka_replay.py`
- Test: `tests/test_pipeline_experiment.py`

**Interfaces:**
- Consumes: completed `ArchiveManifest` objects, `KafkaPublisher`, `kafka_trace_consumer.run`, `spark_sip_trade_batch.run`, and PostgreSQL.
- Produces: `replay_archive(...)`, `ExperimentResult`, `run_experiment(...)`, and a sanitized JSON file under `data/local/experiment-results/`.

- [ ] **Step 1: Write failing tests for streaming replay counts, offset aggregation, duration, redacted errors, and PostgreSQL idempotency metrics**

```python
def test_replay_count_matches_all_manifests():
    result = replay_archive(manifests, publisher=fake_publisher, trace_id="run-1")
    assert result.published_trades == sum(item.row_count for item in manifests)

def test_result_never_serializes_database_url_or_api_key():
    payload = result.to_json()
    assert "postgresql://" not in payload
    assert "APCA_API" not in payload
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `.venv/bin/python -m unittest tests.test_archive_kafka_replay tests.test_pipeline_experiment -v`

- [ ] **Step 3: Implement one-pass Parquet replay and measured orchestration**

`ExperimentResult` includes:

```python
experiment_run_id: str
dataset_id: str
environment: str
status: str
raw_input_trades: int
kafka_published: int
kafka_consumed: int
spark_input: int
spark_invalid: int
spark_duplicates: int
spark_output_bars: int
postgres_stored_bars: int
postgres_business_key_duplicates: int
duration_seconds: float
events_per_second: float
error_type: str | None
```

The JSON writer uses a temporary file and atomic rename. Failed experiments retain counts completed before failure and set `status="failed"`.

- [ ] **Step 4: Run unit and existing Kafka/Spark contract tests**

Run: `.venv/bin/python -m unittest tests.test_archive_kafka_replay tests.test_pipeline_experiment tests.test_kafka_publisher tests.test_spark_sip_trade_batch -v`
Expected: PASS.

- [ ] **Step 5: Commit measured replay files**

```bash
git add src/archive_kafka_replay.py src/pipeline_experiment.py scripts/evidence/pipeline_experiment_summary.sql tests/test_archive_kafka_replay.py tests/test_pipeline_experiment.py
git commit -m "feat: measure archived Kafka replay experiments"
```

### Task 5: Airflow fault and recovery workflow

**Files:**
- Create: `dags/pipeline_load_recovery.py`
- Create: `src/load_recovery_airflow.py`
- Test: `tests/test_load_recovery_airflow.py`
- Test: `tests/test_load_recovery_dag.py`

**Interfaces:**
- Consumes: dataset manifest path, `experiment_mode`, `fault_mode`, Kafka/Spark/PostgreSQL settings, and TaskFlow context run ID.
- Produces: DAG `pipeline_load_recovery_experiment`, validated fault plan, recovery run link, and final `ExperimentResult`.

- [ ] **Step 1: Write failing tests for allowed modes and pre-side-effect validation**

```python
def test_invalid_time_range_fails_before_runner_is_called():
    with self.assertRaisesRegex(ValueError, "start must be before end"):
        validate_experiment_config(invalid_params, run_id="manual__invalid")
    runner.assert_not_called()

def test_fault_mode_is_bounded():
    self.assertEqual(
        ALLOWED_FAULT_MODES,
        {"none", "api_503", "stop_before_spark", "database_unavailable", "duplicate"},
    )
```

- [ ] **Step 2: Run tests and confirm missing DAG/helper failures**

Run: `.venv/bin/python -m unittest tests.test_load_recovery_airflow tests.test_load_recovery_dag -v`

- [ ] **Step 3: Implement TaskFlow tasks without passing raw events through XCom**

Task order:

```text
validate_experiment_config
  → load_archive_manifests
  → replay_archive_to_kafka
  → verify_kafka_delivery
  → controlled_failure_gate
  → spark_batch_and_postgres_upsert
  → verify_integrity
  → write_experiment_result
```

`stop_before_spark` raises only after Kafka offset ranges are persisted. `database_unavailable` points only the experimental Spark task at `localhost:1`; it does not modify stored credentials. Recovery uses the persisted manifest and offset ranges with `fault_mode=none`.

- [ ] **Step 4: Run Airflow import, DAG, and existing Airflow tests**

Run: `.venv/bin/python -m unittest tests.test_load_recovery_airflow tests.test_load_recovery_dag tests.test_market_replay_dag -v`

Run: `.venv/bin/airflow dags list-import-errors`
Expected: no import error for `pipeline_load_recovery_experiment`.

- [ ] **Step 5: Commit Airflow experiment workflow**

```bash
git add dags/pipeline_load_recovery.py src/load_recovery_airflow.py tests/test_load_recovery_airflow.py tests/test_load_recovery_dag.py
git commit -m "feat: automate load failure and recovery experiments"
```

### Task 6: GCP experiment environment and actual runs

**Files:**
- Create: `infra/gcp/create_experiment_vm.sh`
- Create: `infra/gcp/delete_experiment_vm.sh`
- Create: `infra/gcp/run_experiment.sh`
- Create: `docs/evidence/load-recovery/README.md`

**Interfaces:**
- Consumes: project `project-6ebdf72b-a53c-4925-8d2`, zone `us-central1-a`, VM name `market-pipeline-load-lab`, local ignored `.env`, and committed source.
- Produces: one `e2-standard-4` VM with a 100GB standard disk, actual baseline/load/fault/recovery result JSON files, sanitized SQL output, runtime metadata, and deletion evidence.

- [ ] **Step 1: Add shell syntax tests before any cloud mutation**

Run: `bash -n infra/gcp/create_experiment_vm.sh infra/gcp/delete_experiment_vm.sh infra/gcp/run_experiment.sh`
Expected: exit 0.

- [ ] **Step 2: Verify account/project, set the explicit project, enable Compute Engine, and create the VM**

```bash
gcloud config set project project-6ebdf72b-a53c-4925-8d2
gcloud services enable compute.googleapis.com
bash infra/gcp/create_experiment_vm.sh
```

The script rejects any project ID other than `project-6ebdf72b-a53c-4925-8d2` and records only machine type, zone, disk size and creation time.

- [ ] **Step 3: Install dependencies, copy `.env` with mode 600, collect the external archives once, and verify archive hashes**

Run: `bash infra/gcp/run_experiment.sh collect`

Expected summary: first/last release dates, event count, 4 symbols, partition count, total rows, failed partitions 0, hash mismatches 0. The raw archive remains ignored on the VM.

- [ ] **Step 4: Execute baseline and load runs on the same VM**

Run: `bash infra/gcp/run_experiment.sh baseline`

Run: `bash infra/gcp/run_experiment.sh load`

Expected: baseline has exactly 118,118 raw trades and load has at least 1,181,180; each run has published=consumed=Spark input and business-key duplicates 0.

- [ ] **Step 5: Execute and recover every fault scenario**

Run:

```bash
bash infra/gcp/run_experiment.sh api-503
bash infra/gcp/run_experiment.sh invalid-input
bash infra/gcp/run_experiment.sh spark-interruption
bash infra/gcp/run_experiment.sh database-failure
bash infra/gcp/run_experiment.sh duplicate
```

Expected: each injected run fails at its declared stage; recovery JSON links the failed run and reports missing bars 0 and business-key duplicates 0.

- [ ] **Step 6: Copy only sanitized result artifacts to `docs/evidence/load-recovery/` and delete the VM after capture completion**

Run: `bash infra/gcp/delete_experiment_vm.sh`

Expected: `gcloud compute instances describe market-pipeline-load-lab` returns not found after evidence files are local.

- [ ] **Step 7: Commit scripts and sanitized evidence only**

```bash
git add infra/gcp docs/evidence/load-recovery
git commit -m "test: record GCP load and recovery experiments"
```

### Task 7: Assignment document, screenshots, and four-minute script

**Files:**
- Create: `docs/load-recovery-assignment.md`
- Create: `docs/08.31_대본.md`
- Create: `docs/evidence/load-recovery/render_evidence.py`
- Create: `docs/evidence/load-recovery/baseline-vs-load.png`
- Create: `docs/evidence/load-recovery/failure-recovery.png`
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/submission-checklist.md`
- Test: `tests/test_load_recovery_assignment_docs.py`

**Interfaces:**
- Consumes: actual result JSON, sanitized logs, SQL output and GCP runtime metadata from Task 6.
- Produces: one linked assignment narrative, two readable PNG evidence panels, a human-readable four-minute script, and README navigation.

- [ ] **Step 1: Write failing documentation tests for all four assignment requirements and evidence links**

```python
def test_assignment_contains_required_actual_sections(self):
    document = Path("docs/load-recovery-assignment.md").read_text()
    for heading in (
        "정상 기준 실행", "더 큰 데이터 실행", "장애 재현",
        "복구 후 누락·중복 검증", "실행 방법", "현재 구현과 다음 단계",
    ):
        self.assertIn(heading, document)
```

- [ ] **Step 2: Run the documentation test and verify missing artifacts fail**

Run: `.venv/bin/python -m unittest tests.test_load_recovery_assignment_docs -v`

- [ ] **Step 3: Render evidence images from actual JSON without inventing or manually copying metrics**

`render_evidence.py` reads only committed result JSON. Each PNG includes run ID, environment, input, output, duration, throughput, failure point, recovery result and the label `actual measured result`. Secret-bearing fields are rejected before rendering.

- [ ] **Step 4: Write the assignment and presentation script in plain Korean**

The script shows only:

1. pipeline and dataset definition;
2. baseline versus load comparison;
3. one representative failure and recovery;
4. final no-loss/no-duplicate conclusion.

Detailed API 503, validation, duplicate and DB evidence remains in the assignment document for questions.

- [ ] **Step 5: Run docs tests, secret scan, full unit tests, and diff checks**

Run: `.venv/bin/python -m unittest discover -s tests -v`

Run: `rg -n "APCA-API-SECRET-KEY=|FRED_API_KEY=.+|postgresql://[^[:space:]]+:[^[:space:]]+@" docs infra/gcp`
Expected: no secret values.

Run: `git diff --check`
Expected: no whitespace errors.

- [ ] **Step 6: Commit only files produced or intentionally updated by this assignment**

```bash
git add README.md docs/README.md docs/submission-checklist.md docs/load-recovery-assignment.md docs/08.31_대본.md docs/evidence/load-recovery tests/test_load_recovery_assignment_docs.py
git commit -m "docs: present load failure and recovery evidence"
```

### Task 8: Final evidence audit

**Files:**
- Modify only if an audit finds a factual mismatch: `docs/load-recovery-assignment.md`, `docs/08.31_대본.md`, `docs/evidence/load-recovery/README.md`

**Interfaces:**
- Consumes: assignment rubric, committed result JSON, SQL output, screenshots, and implementation.
- Produces: a rubric-to-evidence table with no unsupported claims.

- [ ] **Step 1: Compare every displayed number with its source JSON or SQL output**

Run: `.venv/bin/python docs/evidence/load-recovery/render_evidence.py --verify-only`
Expected: `verified_metrics` equals the number of metrics rendered and `mismatches=0`.

- [ ] **Step 2: Verify the four rubric requirements are linked from the assignment document**

Run: `.venv/bin/python -m unittest tests.test_load_recovery_assignment_docs -v`
Expected: PASS.

- [ ] **Step 3: Inspect both PNG files at original size and fix only readability defects**

Verify that table labels, units, run IDs and failure/recovery status are visible without exposing secrets.

- [ ] **Step 4: Commit factual or readability corrections if needed**

```bash
git add docs/load-recovery-assignment.md docs/08.31_대본.md docs/evidence/load-recovery
git commit -m "docs: finalize load experiment evidence"
```
