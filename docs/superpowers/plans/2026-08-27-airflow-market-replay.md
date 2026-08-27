# Airflow Market Replay Implementation Plan

> **For Codex:** Execute this plan sequentially in the current workspace. Preserve unrelated dirty files. Use test-driven development and verify each milestone before moving on.

**Goal:** Run the existing Alpaca SIP trade replay, Kafka delivery check, Spark minute-bar processing, and PostgreSQL verification as one parameterized Airflow DAG, then prove it with NVDA and SPY runs.

**Architecture:** Keep the existing shared Kafka topic and identify each Airflow run with a deterministic trace ID. Extract reusable `run()` functions from the current CLIs, add a small orchestration module for parameter validation and result verification, and keep the DAG itself limited to task dependency wiring. Pass only small summary dictionaries through XCom; market records continue to move through Kafka and PostgreSQL.

**Tech Stack:** Python 3.14, Apache Airflow 3 Task SDK, confluent-kafka, Spark 4.2 local mode, PostgreSQL 17, unittest, Docker Compose for Kafka/PostgreSQL.

---

## Task 1: Add parameter and trace-ID contracts

**Files:**
- Create: `src/airflow_market_replay.py`
- Create: `tests/test_airflow_market_replay.py`

1. Write failing tests for accepted ticker/feed/UTC timestamps, rejected malformed values, `start < end`, and stable trace IDs for one DAG run.
2. Run `python -m unittest tests.test_airflow_market_replay -v` and confirm the tests fail because the module is absent.
3. Implement a `MarketReplayConfig` value object and `validate_run_config()` with a strict ticker pattern, allowed feeds, timezone-aware ISO timestamps, and deterministic sanitized trace ID.
4. Run the focused test and confirm it passes.
5. Commit only the new contract files.

## Task 2: Make existing CLI stages callable by Airflow

**Files:**
- Modify: `src/historical_market_replay.py`
- Modify: `src/kafka_trace_consumer.py`
- Modify: `tests/test_historical_market_replay.py`
- Modify: `tests/test_kafka_trace_consumer.py`

1. Write failing tests that call each stage as a function and assert that it returns the same small summary currently printed by its CLI.
2. Run the two focused test modules and confirm the new tests fail.
3. Extract `run(args)` from each `main()` without changing request, publishing, or counting behavior; retain CLI JSON output and exit codes.
4. Inject client/publisher/consumer factories where needed so unit tests never call Alpaca or Kafka.
5. Run both focused test modules and confirm they pass.
6. Commit only these callable-stage changes.

## Task 3: Add Spark and PostgreSQL orchestration checks

**Files:**
- Modify: `src/airflow_market_replay.py`
- Modify: `tests/test_airflow_market_replay.py`

1. Write failing tests for construction of replay, consumer, and Spark arguments from one validated config and upstream summary.
2. Write failing tests for PostgreSQL result verification: matching bar count succeeds, count mismatch and wrong symbol fail.
3. Implement thin task helper functions that call the existing stage `run()` functions and return summary dictionaries.
4. Implement a parameterized PostgreSQL count query for `symbol`, `start <= bar_start < end`, `timeframe='1m'`, `source='alpaca_replay'`, and the selected `feed`; do not log the DSN.
5. Run `python -m unittest tests.test_airflow_market_replay -v` and confirm it passes.
6. Commit only orchestration helper changes.

## Task 4: Build the Airflow 3 DAG

**Files:**
- Create: `dags/market_replay_pipeline.py`
- Create: `tests/test_market_replay_dag.py`
- Create: `requirements-airflow.txt`
- Modify: `.gitignore`

1. Write a failing DAG import/shape test expecting DAG ID `market_sip_replay_pipeline`, manual schedule, four runtime Params, and five ordered tasks.
2. Add the reproducible Airflow 3 installation requirement and ignore only runtime metadata/log artifacts.
3. Implement the DAG with the public `airflow.sdk` API, JSON-Schema-backed `Param` definitions, `max_active_runs=1`, one retry, and tasks:
   - `validate_run_config`
   - `replay_trades_to_kafka`
   - `verify_kafka_delivery`
   - `build_minute_bars_with_spark`
   - `verify_stored_result`
4. Run the DAG shape test and Airflow import-error check.
5. Commit only DAG, dependency, ignore, and DAG-test files.

## Task 5: Run local integration prerequisites

**Files:**
- No source changes expected.

1. Start Kafka and PostgreSQL with `docker compose up -d postgres kafka kafka-init`.
2. Apply the existing PostgreSQL migrations.
3. Confirm the Kafka topic exists and PostgreSQL is healthy.
4. Install Airflow with the official constraints matching Python 3.14, initialize its local runtime under ignored `airflow-runtime/`, and confirm the DAG has no import errors.
5. Run existing unit and relevant integration tests before real API execution.

## Task 6: Execute the DAG for NVDA and SPY

**Files:**
- Create: `docs/evidence/airflow-market-replay/README.md`
- Create: `docs/evidence/airflow-market-replay/nvda-run-summary.json`
- Create: `docs/evidence/airflow-market-replay/spy-run-summary.json`

1. Trigger the DAG with `NVDA`, `2026-08-12T12:25:00Z`, `2026-08-12T12:35:00Z`, and `sip`.
2. Verify every task succeeds and export a sanitized small summary containing input, trace ID, stage counts, and stored bar count.
3. Trigger the same DAG with only ticker changed to `SPY`.
4. Verify every task succeeds and export the same sanitized summary.
5. Query PostgreSQL to confirm the saved one-minute bars for both tickers.
6. If the ten-minute range exceeds local limits, shorten the same range for both runs through DAG Params only and record the exact changed range.
7. Commit only sanitized evidence; never add `.env`, Airflow metadata DB, complete logs, or raw trades.

## Task 7: Make the assignment easy to submit and present

**Files:**
- Create: `docs/airflow-assignment.md`
- Modify: `README.md`
- Modify: `docs/README.md`

1. Add a focused assignment document containing the DAG graph, input table, exact local commands, NVDA/SPY result table, output location/schema, current implementation, and next steps.
2. Keep the main README focused on the whole project; add one link to the fifth-session assignment in the later documentation/results section.
3. Link the assignment document from the docs index.
4. Clearly distinguish actual run evidence from planned retry/backfill/scheduling enhancements.
5. Run documentation consistency tests and `git diff --check`.

## Task 8: Final verification

**Files:**
- Review all files changed by this plan.

1. Run the full unit test suite.
2. Run relevant Kafka/Spark/PostgreSQL integration tests.
3. Run Airflow DAG import checks.
4. Confirm `git status` contains no secrets, raw data, Airflow runtime files, or accidental changes to the user’s pre-existing work.
5. Compare the implementation and evidence against every item in the fifth-session assignment.
6. Report exact commands and observed results; do not wait for CI.
