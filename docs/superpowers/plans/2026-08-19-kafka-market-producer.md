# Kafka Market Producer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish Alpaca IEX raw trade payloads inside the canonical project envelope to Kafka topic `raw.market.v1`, keyed by symbol, and prove the result against a local single-broker Kafka instance.

**Architecture:** Keep Alpaca payload handling, envelope creation, and Kafka delivery as separate units. The collector reads only `T`, `S`, `i`, and `t` before publishing; it preserves the provider payload unchanged. Kafka delivery uses `confluent-kafka` with idempotence and `acks=all`; a real local Kafka integration test verifies the serialized record and partition key.

**Tech Stack:** Python 3.14, `websockets` 15, `confluent-kafka` 2.15, Apache Kafka 4.3.1 KRaft, Docker Compose, `unittest`.

**Spec:** `PROJECT_PLAN.md`, `docs/architecture.md`, `docs/data-model.md`, `docs/data-source-catalog.md`

## Global Constraints

- Topic is exactly `raw.market.v1`; record key is the raw `S` symbol.
- Envelope `event_type` is `market.trade.raw`, `schema_version` is `1`, `source` is `alpaca`, and `feed` is `iex` for the live collector.
- Deterministic `event_id` hashes source, feed, event type, symbol, provider trade ID, and source event timestamp.
- Provider payload is serialized without deleting or renaming fields.
- Collector reads only `T`, `S`, `i`, and `t` for routing and identity; full schema and trade-condition validation remain Spark responsibilities.
- Kafka producer uses `enable.idempotence=true` and `acks=all`; single-broker mode does not claim replication high availability.
- API credentials remain in ignored `.env`; no key or secret appears in source, logs, tests, reports, or Git.
- No Spark, PostgreSQL, Airflow, retry daemon, schema registry, or monitoring stack is added in this feature.

---

### Task 1: Canonical Raw Market Envelope

**Files:**
- Create: `src/market_event.py`
- Create: `tests/test_market_event.py`

**Interfaces:**
- Consumes: `build_market_envelope(payload: Mapping[str, Any], feed: str, ingested_at: datetime, trace_id: str | None) -> dict[str, Any]`
- Produces: canonical JSON-ready dictionary used by the Kafka publisher and integration test.

- [ ] **Step 1: Write the failing tests**

```python
def test_builds_canonical_envelope_without_changing_payload():
    raw = {"T": "t", "S": "NVDA", "i": 23, "x": "V", "p": 221.69,
           "s": 5, "c": ["@", "I"], "z": "C",
           "t": "2026-08-19T13:30:00.102733966Z"}
    envelope = build_market_envelope(
        raw,
        feed="iex",
        ingested_at=datetime(2026, 8, 19, 13, 30, 1, tzinfo=timezone.utc),
        trace_id="run-1",
    )
    assert envelope["payload"] == raw
    assert envelope["event_type"] == "market.trade.raw"
    assert envelope["source_event_id"] == "23"
    assert envelope["event_timestamp"] == raw["t"]
    assert envelope["trace_id"] == "run-1"

def test_event_id_changes_when_reused_provider_id_has_new_timestamp():
    first = dict(RAW_TRADE, i=1, t="2026-08-19T13:27:08Z")
    second = dict(RAW_TRADE, i=1, t="2026-08-19T13:27:13Z")
    assert build_market_envelope(first, "test", NOW)["event_id"] != \
           build_market_envelope(second, "test", NOW)["event_id"]
```

- [ ] **Step 2: Run tests and verify RED**

Run: `.venv/bin/python -m unittest tests/test_market_event.py -v`

Expected: import failure because `src.market_event` does not exist.

- [ ] **Step 3: Implement the minimal envelope builder**

```python
def build_market_envelope(payload, feed, ingested_at, trace_id=None):
    for name in ("T", "S", "i", "t"):
        if name not in payload:
            raise ValueError(f"Missing routing field: {name}")
    identity = ["alpaca", feed, "market.trade.raw", payload["S"],
                str(payload["i"]), payload["t"]]
    digest = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    return {
        "event_id": f"sha256:{digest}",
        "event_type": "market.trade.raw",
        "schema_version": 1,
        "source": "alpaca",
        "feed": feed,
        "source_event_id": str(payload["i"]),
        "event_timestamp": payload["t"],
        "ingested_at": ingested_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "trace_id": trace_id,
        "payload": dict(payload),
    }
```

- [ ] **Step 4: Run all unit tests and verify GREEN**

Run: `.venv/bin/python -m unittest discover -s tests -v`

Expected: all existing and new tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/market_event.py tests/test_market_event.py
git commit -m "feat: add canonical raw market envelope"
```

---

### Task 2: Reliable Kafka Publisher

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/kafka_publisher.py`
- Create: `tests/test_kafka_publisher.py`

**Interfaces:**
- Consumes: `KafkaPublisher(bootstrap_servers: str, topic: str = "raw.market.v1", producer: Producer | None = None)` and `publish(envelope: Mapping[str, Any]) -> None`.
- Produces: Kafka record with UTF-8 symbol key and compact JSON value; `close(timeout_seconds: float = 10.0) -> None` verifies outstanding delivery.

- [ ] **Step 1: Add the client dependency**

Run: `uv add 'confluent-kafka>=2.15,<3'`

Expected: `pyproject.toml` and `uv.lock` contain the bounded client version.

- [ ] **Step 2: Write failing publisher tests with a recording test double**

The double implements the real boundary methods `produce`, `poll`, and `flush`; assertions remain on publisher-visible key/value behavior.

```python
def test_publishes_symbol_key_and_canonical_json_value():
    recorder = RecordingProducer()
    publisher = KafkaPublisher("localhost:9092", producer=recorder)
    publisher.publish(ENVELOPE)
    record = recorder.records[0]
    assert record["topic"] == "raw.market.v1"
    assert record["key"] == b"NVDA"
    assert json.loads(record["value"]) == ENVELOPE

def test_close_fails_when_delivery_callback_reports_error():
    recorder = RecordingProducer(delivery_error=RuntimeError("broker unavailable"))
    publisher = KafkaPublisher("localhost:9092", producer=recorder)
    publisher.publish(ENVELOPE)
    with pytest.raises(KafkaDeliveryError):
        publisher.close()
```

Use `unittest.assertRaises` rather than adding pytest.

- [ ] **Step 3: Run tests and verify RED**

Run: `.venv/bin/python -m unittest tests/test_kafka_publisher.py -v`

Expected: import failure because `src.kafka_publisher` does not exist.

- [ ] **Step 4: Implement minimal delivery handling**

```python
DEFAULT_PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "none",
    "linger.ms": 0,
}

class KafkaPublisher:
    def publish(self, envelope):
        symbol = envelope["payload"]["S"]
        value = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode()
        self._producer.produce(
            self.topic, key=symbol.encode(), value=value, on_delivery=self._on_delivery
        )
        self._producer.poll(0)

    def close(self, timeout_seconds=10.0):
        remaining = self._producer.flush(timeout_seconds)
        if remaining or self._delivery_errors:
            raise KafkaDeliveryError(...)
```

On `BufferError`, call `poll(1)` and retry at most three times; raise a clear error after the third full-queue result. Never silently drop a record.

- [ ] **Step 5: Run the full suite and verify GREEN**

Run: `.venv/bin/python -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/kafka_publisher.py tests/test_kafka_publisher.py
git commit -m "feat: add reliable Kafka market publisher"
```

---

### Task 3: Alpaca WebSocket to Kafka Collector

**Files:**
- Create: `src/market_producer.py`
- Create: `tests/test_market_producer.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: Alpaca WebSocket JSON message lists and `KafkaPublisher.publish(envelope)`.
- Produces: `process_messages(messages, publisher, feed, trace_id, clock) -> int` returning the number of published trades; CLI reads `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, and Alpaca credentials.

- [ ] **Step 1: Write failing message-processing tests**

```python
def test_publishes_only_trade_messages():
    publisher = RecordingPublisher()
    count = process_messages(
        [{"T": "success", "msg": "authenticated"}, RAW_TRADE],
        publisher=publisher,
        feed="iex",
        trace_id="collector-1",
        clock=lambda: NOW,
    )
    assert count == 1
    assert publisher.envelopes[0]["payload"] == RAW_TRADE

def test_rejects_incomplete_trade_before_kafka():
    incomplete = {"T": "t", "S": "NVDA", "i": 1}
    with unittest.TestCase().assertRaisesRegex(ValueError, "Missing routing field: t"):
        process_messages([incomplete], publisher, "iex", "collector-1", lambda: NOW)
```

- [ ] **Step 2: Run tests and verify RED**

Run: `.venv/bin/python -m unittest tests/test_market_producer.py -v`

Expected: import failure because `src.market_producer` does not exist.

- [ ] **Step 3: Implement the collector**

The CLI reuses `load_credentials`, `build_auth_message`, `build_subscribe_message`, and `require_success` from `live_market_smoke.py`. It connects once to `wss://stream.data.alpaca.markets/v2/iex`, authenticates, subscribes, passes each received list through `process_messages`, calls `publisher.poll(0)` through `publish`, and closes the publisher in `finally`. CLI arguments include `--symbols`, `--max-trades`, and `--timeout` so a bounded live verification exits cleanly.

- [ ] **Step 4: Extend environment template**

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=raw.market.v1
```

- [ ] **Step 5: Run the full suite and verify GREEN**

Run: `.venv/bin/python -m unittest discover -s tests -v`

Expected: all tests pass and no credentials appear in output.

- [ ] **Step 6: Commit**

```bash
git add .env.example src/market_producer.py tests/test_market_producer.py
git commit -m "feat: stream Alpaca trades into Kafka"
```

---

### Task 4: Local Kafka and End-to-End Verification

**Files:**
- Create: `compose.yml`
- Create: `tests/integration/test_kafka_market_producer.py`
- Modify: `README.md`
- Create: `docs/test-results/2026-08-19-kafka-producer-smoke.md`

**Interfaces:**
- Consumes: Docker service `kafka` at `localhost:9092` and topic `raw.market.v1`.
- Produces: repeatable integration test that publishes one canonical envelope and consumes the same key/value from a real broker.

- [ ] **Step 1: Add the pinned single-broker KRaft compose service**

Use `apache/kafka:4.3.1`, combined broker/controller mode, external listener `localhost:9092`, internal listener `kafka:19092`, replication factor `1`, and an init service that creates `raw.market.v1` with `3` partitions, replication factor `1`, and `retention.ms=86400000`.

- [ ] **Step 2: Validate the Compose model**

Run: `docker compose config`

Expected: exit `0`; rendered services are `kafka` and `kafka-init` with no unresolved variables.

- [ ] **Step 3: Start Kafka and verify topic configuration**

Run: `docker compose up -d kafka kafka-init`

Run: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic raw.market.v1`

Expected: `PartitionCount: 3`, `ReplicationFactor: 1`, and `retention.ms=86400000`.

- [ ] **Step 4: Write and run the real-broker integration test**

The test creates a unique consumer group, subscribes before producing, publishes one canonical NVDA fixture through `KafkaPublisher`, polls up to 10 seconds, and asserts:

```python
self.assertEqual(message.key(), b"NVDA")
self.assertEqual(json.loads(message.value()), envelope)
self.assertIsNone(message.error())
```

Run: `RUN_KAFKA_INTEGRATION=1 .venv/bin/python -m unittest tests/integration/test_kafka_market_producer.py -v`

Expected: one test passes against the real broker.

- [ ] **Step 5: Run bounded live Alpaca→Kafka verification**

Run from the main checkout after merge so the existing ignored `.env` is used:

```bash
.venv/bin/python -m src.market_producer \
  --symbols SPY QQQ NVDA --max-trades 10 --timeout 60
```

Consume exactly 10 records with the Kafka console consumer and confirm each value has the canonical envelope and original payload.

- [ ] **Step 6: Document evidence and operating commands**

README must distinguish the completed Producer slice from future Spark/PostgreSQL work. The smoke report records versions, topic description, produced/consumed counts, field contract, delivery outcome, and single-broker limitation without storing credentials.

- [ ] **Step 7: Run final verification**

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m compileall -q src tests
docker compose config
git diff --check
```

Expected: all unit and integration tests pass, compilation succeeds, Compose validates, and diff check is empty.

- [ ] **Step 8: Commit**

```bash
git add compose.yml tests/integration/test_kafka_market_producer.py README.md docs/test-results/2026-08-19-kafka-producer-smoke.md
git commit -m "test: verify market producer with local Kafka"
```
