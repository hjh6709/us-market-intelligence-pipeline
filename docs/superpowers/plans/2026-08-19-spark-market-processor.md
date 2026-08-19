# Spark Market Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consume canonical Alpaca trade envelopes from Kafka with Spark Structured Streaming and produce validated, deduplicated, final one-minute OHLCV/VWAP bars.

**Architecture:** Keep schema definitions, reusable DataFrame transformations, and the streaming runner separate. Batch DataFrame fixtures prove parsing, validation, and deterministic aggregation; a real Kafka integration test proves watermark, deduplication, checkpoint, and append-output behavior.

**Tech Stack:** Python 3.14, Java 21, PySpark 4.2.0, Spark Kafka connector 4.2.0 for Scala 2.13, Apache Kafka 4.3.1, `unittest`.

**Spec:** `docs/superpowers/specs/2026-08-19-spark-market-processor-design.md`

## Global Constraints

- Branch is `spark-market-processor`; no `codex/` prefix is used.
- PostgreSQL, technical indicators, anomaly alerts, Airflow, and production DLQ are outside this PR.
- Kafka input topic is `raw.market.v1`; Spark query is the Kafka Consumer artifact.
- Default watermark is exactly `2 minutes` and remains CLI-configurable.
- Deduplication key is deterministic `event_id` using `dropDuplicatesWithinWatermark`.
- Bar key is symbol, source, feed, and one-minute event-time window.
- Condition policy is `all_valid_trades_v1`; no unverified condition code is removed.
- Runner never prints credentials or complete invalid raw payloads.
- Every stateful query uses its own checkpoint directory.

---

### Task 1: PySpark Runtime Boundary

**Files:** Modify `pyproject.toml`, `uv.lock`; create `src/spark_session.py`, `tests/test_spark_session.py`.

**Interfaces:** `create_local_spark(app_name: str, master: str = "local[2]") -> SparkSession`.

- [ ] Add `pyspark==4.2.0` with `uv add`.
- [ ] Write a failing test importing `create_local_spark`, running a one-row DataFrame action, and asserting UTC session timezone.
- [ ] Run `.venv/bin/python -m unittest tests/test_spark_session.py -v` and verify missing-module failure.
- [ ] Implement a local SparkSession with `spark.sql.session.timeZone=UTC`, UI disabled, and two shuffle partitions.
- [ ] Run the Spark test and existing suite; verify green.
- [ ] Commit `feat: add local Spark runtime`.

---

### Task 2: Explicit Schema Parsing and Validation

**Files:** Create `src/spark_schemas.py`, `src/preprocess.py`, `tests/test_preprocess.py`.

**Interfaces:** `parse_market_events(kafka_df)`, `validate_market_trades(parsed_df, allowed_symbols)`, `split_valid_invalid(validated_df)`.

- [ ] Write failing fixture tests for canonical NVDA parsing and reason codes covering malformed JSON, event type, schema version, missing IDs, allowlist, price, size, timestamp, and timestamp mismatch.
- [ ] Run `.venv/bin/python -m unittest tests/test_preprocess.py -v`; verify import failure.
- [ ] Define envelope/payload `StructType`; price uses `DecimalType(18, 6)`, size uses `LongType`, and timestamp strings remain available for comparison.
- [ ] Parse Kafka `value` with `from_json`, retain Kafka metadata, normalize raw fields, and build the bounded `reason_codes` array with Spark expressions.
- [ ] Treat an empty reason array as valid; invalid output retains reason and metadata without logging raw JSON.
- [ ] Run preprocessing tests and full suite; verify green.
- [ ] Commit `feat: validate Spark market trades`.

---

### Task 3: Deterministic One-Minute Bars

**Files:** Modify `src/preprocess.py`, `tests/test_preprocess.py`.

**Interfaces:** `aggregate_minute_bars(trades_df)` and `prepare_streaming_trades(valid_df, watermark_delay)`.

- [ ] Write failing batch tests with out-of-order trades asserting open/close by `(event_timestamp,event_id)`, high, low, volume, count, decimal VWAP, `1m`, final flag, and policy; add zero-volume VWAP null case.
- [ ] Aggregate by symbol/source/feed and `window(event_timestamp, "1 minute")`; use `min_by` and `max_by` with a timestamp/event-id struct for deterministic open/close.
- [ ] Calculate VWAP as decimal notional divided by positive volume, flatten the window to `bar_start`, and select canonical output columns.
- [ ] For streaming input, apply the configured watermark and `dropDuplicatesWithinWatermark(["event_id"])`; reject batch use of this stateful helper.
- [ ] Run aggregation tests and full suite; verify green.
- [ ] Commit `feat: aggregate one-minute market bars`.

---

### Task 4: Streaming Runner and Kafka Integration

**Files:** Create `src/spark_market_processor.py`, `tests/test_spark_market_processor.py`, `tests/integration/test_spark_market_processor.py`; modify `.gitignore`, `.env.example`.

**Interfaces:** CLI consumes Kafka servers/topic, symbol allowlist, watermark, checkpoint root, offset mode, and optional timeout; it produces a final-bar append query and invalid-reason update query.

- [ ] Write failing tests for CLI defaults and checkpoint paths `bars/` and `invalid-metrics/`.
- [ ] Configure connector `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`; use `latest` normally and `earliest` only for explicit replay/test mode.
- [ ] Start bar query in append/console mode and invalid metrics in update/console mode with separate checkpoints; stop cleanly and propagate query failures.
- [ ] Ignore `.spark-checkpoints/` and add Spark environment defaults.
- [ ] Write a real-broker test guarded by `RUN_SPARK_KAFKA_INTEGRATION=1`. It creates a unique topic, starts Spark with a temporary checkpoint, publishes normal/out-of-order/duplicate/late/advance fixtures, and asserts the final NVDA bar from a memory sink.
- [ ] Restart with the same checkpoint and verify processed offsets do not emit the prior bar again.
- [ ] Run unit and real Kafka integration tests; capture query progress, watermark, and dropped-row metrics.
- [ ] Commit `feat: stream Kafka trades through Spark`.

---

### Task 5: Evidence, Documentation, and Draft PR

**Files:** Modify `README.md`, `docs/course-alignment.md`; create `docs/test-results/2026-08-19-spark-market-processor-smoke.md`.

**Interfaces:** Produce reproducible commands and a Draft PR from `spark-market-processor` to `main`.

- [ ] Document that Spark Structured Streaming is the Kafka Consumer and PostgreSQL remains the next PR.
- [ ] Record runtime versions, fixture inputs, final bar, duplicate/late behavior, checkpoint restart, and single-broker limitation.
- [ ] Run all unit tests, compileall, both real Kafka integration tests, `docker compose config`, and `git diff --check`.
- [ ] Commit `docs: record Spark market processor verification`.
- [ ] Push `spark-market-processor` with upstream tracking.
- [ ] Use the GitHub connector, not the invalid local `gh` token, to create a Draft PR against `main` with changes, rationale, limitations, and validation commands.
