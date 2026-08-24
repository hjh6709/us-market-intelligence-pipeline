# Kafka·Spark Supporting Assignment

이 문서는 CPI 발표 구간의 실제 IEX 체결을 Kafka·Spark로 재현한 수업 과제를 정리한다.

## 목적

2026-08-12 CPI 발표 시각 `08:30 ET(12:30 UTC)`과 연결되는 NVDA 실제 IEX 체결을 Kafka에 발행하고 Spark Structured Streaming으로 검증·중복 제거·event-time 1분 집계한 뒤 PostgreSQL에 멱등 저장한다.

```text
Alpaca IEX trade / CPI release-window replay
→ Kafka raw.market.v1
→ Spark Structured Streaming
→ PostgreSQL market_bars
```

raw IEX trade는 발표 당일 실시간 수집 경로를 재현하고, 이미 집계된 Historical SIP bar는 별도 배치 경로에서 CPI 반응 분석과 사후 범위 검증에 사용한다. 서로 다른 feed를 한 데이터처럼 섞거나 SIP bar를 raw trade topic에 넣지 않는다.

## Kafka 메시지

- Topic: `raw.market.v1`
- key: `symbol`
- partitions: 3
- retention: 24시간
- event type: `market.trade.raw`
- source/feed: `alpaca/iex`
- 공통 필드: `event_id`, `schema_version`, `source_event_id`, `event_timestamp`, `ingested_at`, `trace_id`, `payload`

### 공통 envelope 명세

| 필드 | 타입 | 의미 |
| --- | --- | --- |
| `event_id` | string | source·feed·종목·원본 거래 ID·거래 시각으로 만든 결정적 중복 제거 키 |
| `event_type` | string | 이벤트 종류. 현재 값은 `market.trade.raw` |
| `schema_version` | integer | 메시지 구조 버전. 현재 값은 `1` |
| `source` | string | 데이터 제공처. 현재 값은 `alpaca` |
| `feed` | string | 시장 데이터 범위. 현재 값은 `iex` |
| `source_event_id` | string | Alpaca가 제공한 원본 거래 ID |
| `event_timestamp` | UTC datetime string | 실제 거래가 발생한 시각 |
| `ingested_at` | UTC datetime string | Producer가 메시지를 수집한 시각 |
| `trace_id` | string 또는 null | 한 번의 live 연결·replay 실행을 추적하는 ID |
| `payload` | object | 변경하지 않고 보존한 Alpaca 원본 거래 필드 |

### Alpaca `payload` 명세

| 필드 | 타입 | 의미 |
| --- | --- | --- |
| `T` | string | Alpaca 메시지 종류. 거래는 `t` |
| `S` | string | 종목 코드 |
| `i` | integer | Alpaca 거래 ID |
| `x` | string | 거래소 코드 |
| `p` | decimal(18,6) | 체결 가격 |
| `s` | integer | 체결 수량 |
| `c` | array\<string\> | 거래 조건 코드 목록 |
| `t` | UTC datetime string | 원본 체결 시각 |
| `z` | string | 테이프 코드 |

아래 값은 schema 설명을 위한 **합성 JSON 예시**이며 실제 거래 가격이나 API key가 아니다.

```json
{
  "event_id": "sha256:example-only",
  "event_type": "market.trade.raw",
  "schema_version": 1,
  "source": "alpaca",
  "feed": "iex",
  "source_event_id": "12345",
  "event_timestamp": "2026-08-19T19:50:00.123456Z",
  "ingested_at": "2026-08-21T01:00:00.000000Z",
  "trace_id": "assignment-example",
  "payload": {
    "T": "t",
    "S": "SMH",
    "i": 12345,
    "x": "V",
    "p": 100.25,
    "s": 10,
    "c": ["@"],
    "t": "2026-08-19T19:50:00.123456Z",
    "z": "C"
  }
}
```

Spark의 정본 schema와 validation 규칙은 [데이터 모델](data-model.md)에 있다.

## 실행

```bash
docker compose up -d --wait kafka kafka-init postgres

.venv/bin/python -m src.spark_market_processor \
  --starting-offsets latest --symbols NVDA --watermark "2 minutes" \
  --checkpoint-root .spark-checkpoints/cpi-20260812-nvda --timeout 180

.venv/bin/python -m src.historical_market_replay \
  --symbol NVDA --start 2026-08-12T11:30:00Z \
  --end 2026-08-12T13:34:00Z --feed iex \
  --trace-id cpi-20260812-nvda-001

.venv/bin/python -m src.kafka_trace_consumer \
  --trace-id cpi-20260812-nvda-001 \
  --expected-count 1576 --timeout 60
```

100배속 replay:

```bash
.venv/bin/python -m src.historical_market_replay \
  --symbol SMH --start 2026-08-19T19:45:00Z \
  --end 2026-08-19T20:00:00Z --feed iex \
  --trace-id load-20260824-smh-100x \
  --speed-multiplier 100
```

## 실제 검증 결과

| 실행 | 결과 |
| --- | --- |
| WebSocket → Kafka | 실제 IEX 거래 10건 수신·발행·재소비 |
| CPI release-window replay | Producer 1,576건 = Consumer 1,576건 |
| Spark 처리 | 입력 1,576건, validation 오류 0건 |
| PostgreSQL | 확정 거래 509건 → 1분봉 18건, 중복 key 0건 |
| 100배속 replay | 1,523건, 169.567 events/s, Consumer·Spark 각 1,523건 |

Spark는 JSON schema parsing, 필수값·종목·가격·수량 검증, UTC event-time 변환, `event_id` 중복 제거, 2분 watermark와 1분 window 집계를 수행한다. 처리 전 1,576건 중 validation 오류는 0건이며, append mode에서 watermark를 통과해 확정된 거래 509건이 최종 1분봉 18건으로 저장됐다. 나머지 1,067건은 실행 종료 시점에 아직 watermark를 통과하지 않은 window의 추정치이며 삭제 또는 오류로 계산하지 않는다.

요청 범위는 CPI 발표 전 60분부터 정규장 개장 후 4분까지다. 무료 IEX의 첫 실제 체결은 `12:10 UTC`에 나타났으므로 발표 전 60분 전체 coverage가 있다고 주장하지 않는다. 개장 후 4분은 개장 반응을 포함하면서 event-time watermark를 진행시키기 위해 포함했다.

## 최종 저장 명세

- 저장소: PostgreSQL
- schema/table: PostgreSQL 기본 schema의 `market_bars`
- 저장 단위: symbol별 event-time 1분 OHLCV
- business key: `(symbol, bar_start, timeframe, source, feed)`
- 저장 방식: Spark `foreachBatch`의 PostgreSQL upsert

| 최종 컬럼 | 타입 | 의미 |
| --- | --- | --- |
| `symbol` | text | 종목 코드 |
| `bar_start` | timestamptz | 1분 window 시작 UTC 시각 |
| `timeframe` | text | 봉 주기. 현재 `1m` |
| `open`·`high`·`low`·`close` | numeric | 1분 OHLC 가격 |
| `volume` | bigint | 1분간 체결 수량 합계 |
| `trade_count` | bigint | 1분간 거래 건수 |
| `vwap` | numeric | 거래량 가중 평균 가격 |
| `source`·`feed` | text | 데이터 제공처와 시장 범위 |
| `is_final` | boolean | watermark를 통과한 확정 bar 여부 |
| `condition_policy` | text | 집계에 적용한 거래 조건 정책 버전 |
| `spark_batch_id` | bigint | 저장한 Spark micro-batch ID |
| `updated_at` | timestamptz | 마지막 upsert 시각 |

## 현재 구현과 다음 단계

현재 구현된 범위는 CPI 발표 구간 실제 거래의 Kafka 전송, Consumer 건수 검증, Spark 전처리·집계 및 PostgreSQL 저장까지다. CPI 발표 일정·ALFRED vintage·SIP bar 배치는 별도 구현되어 같은 발표 시각으로 연결된다. Airflow 자동 실행, 실시간 이상 징후 계산, API·대시보드와 주문 실행은 이번 과제 결과가 아니라 후속 범위다.

상세 증거:

- [Kafka·Spark 실행 보고서](test-results/2026-08-21-kafka-spark-assignment.md)
- [CPI 발표 구간 Kafka·Spark 실행 보고서](test-results/2026-08-24-cpi-kafka-spark.md)
- [100배속 replay 보고서](test-results/2026-08-24-replay-load-100x.md)
- [실제 수집 증거](evidence/actual-ingestion/README.md)

현재 로컬 Kafka는 single broker이므로 복제 기반 고가용성을 제공하지 않는다. 실제 처리량 역시 Spark가 반드시 필요한 규모라는 뜻이 아니라, 과정에서 학습한 event-time·watermark·checkpoint를 직접 검증하기 위한 구현이다.
