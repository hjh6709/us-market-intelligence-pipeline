# Spark Market Processor Design

## 1. 목적과 범위

`raw.market.v1`의 Alpaca trade envelope를 Spark Structured Streaming으로 읽어 명시적 schema로 검증·정규화하고, event time 기준 최종 1분 OHLCV/VWAP bar를 만든다.

이번 PR에 포함한다.

- Kafka source와 Spark local Structured Streaming 실행 경계
- canonical envelope와 Alpaca trade payload parsing
- allowlist, type, price, size, timestamp 검증과 valid/invalid 분리
- `event_id` watermark deduplication
- symbol/source/feed별 event-time 1분 OHLCV, VWAP, trade count
- checkpoint와 append output mode
- 정상·중복·watermark 안의 지연·watermark 초과 지연 fixture
- 실제 Kafka broker를 사용한 streaming smoke test

이번 PR에 포함하지 않는다.

- PostgreSQL/JDBC와 idempotent upsert
- 기술지표, 이상 탐지와 alert
- Airflow, SIP reconciliation과 macro impact 분석
- 운영용 DLQ sink, Prometheus/Grafana와 Spark cluster

PostgreSQL이 없으므로 실행 명령은 final bar를 console sink로 보여준다. 변환 함수가 반환하는 bar schema는 다음 PR의 PostgreSQL sink 입력 계약이 된다.

## 2. 선택한 접근 방식

### 선택: 재사용 가능한 DataFrame 변환 + 실제 Kafka streaming smoke

schema parsing, validation, deduplication, aggregation을 작은 DataFrame 함수로 나눈다. 결정적 fixture는 local Spark batch DataFrame으로 빠르게 검증하고, watermark/state/checkpoint/Kafka source는 실제 local broker의 streaming integration test로 검증한다.

모든 테스트를 Kafka stream으로만 실행하면 느리고 schema·집계 오류와 broker 문제를 분리하기 어렵다. 반대로 Python 함수로 Spark 동작을 흉내 내면 Structured Streaming 구현 증거가 되지 않는다. 선택한 방식은 두 단점을 피한다.

## 3. 런타임과 버전

- PySpark `4.2.0`
- Java `21` — Spark 4.2 공식 요구사항인 Java 17 이상 충족
- Python `3.14` — Spark 4.2 공식 요구사항인 Python 3.10 이상 충족
- Kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`
- Kafka broker `4.3.1`, topic `raw.market.v1`, 3 partitions
- local mode `local[2]`

의존성 설치 후 SparkSession 생성과 작은 DataFrame action을 먼저 실행해 실제 호환성을 확인한다. 실패하면 기능 코드를 시작하기 전에 공식 지원 조합 안에서 Python runtime 또는 Spark patch version을 조정하고 결과를 기록한다.

## 4. 컴포넌트

### `src/spark_schemas.py`

canonical envelope와 Alpaca payload의 `StructType`을 한곳에 정의한다. 입력 JSON에서 예상하지 않은 provider field가 추가돼도 parsing은 실패하지 않지만, 필요한 field의 누락이나 잘못된 type은 validation reason으로 남긴다.

### `src/preprocess.py`

다음 순수 DataFrame 경계를 제공한다.

```text
parse_market_events(kafka_df)
→ validate_market_trades(parsed_df, allowed_symbols)
→ select_valid_trades(validated_df)
→ withWatermark(event_timestamp, watermark_delay)
→ dropDuplicatesWithinWatermark(event_id)
→ aggregate_minute_bars(valid_trade_df)
```

`parse_market_events`는 Kafka `value`를 UTF-8 JSON으로 parsing하고 Kafka metadata(topic, partition, offset, timestamp)를 보존한다. provider field는 다음 normalized 이름으로 매핑한다.

```text
payload.S → symbol
payload.p → price
payload.s → size
payload.x → exchange
payload.c → conditions
payload.t / envelope.event_timestamp → event_timestamp
```

envelope timestamp와 payload timestamp가 다르면 `TIMESTAMP_MISMATCH`다.

### `src/spark_market_processor.py`

CLI와 SparkSession 구성을 담당한다. `raw.market.v1`을 한 번 정의한 source DataFrame에서 두 query를 시작한다. bar query는 final bar를 append/console mode로 출력한다. invalid metric query는 `foreachBatch`에서 해당 micro-batch의 reason별 count만 출력해 장시간 누적되는 불필요한 groupBy state를 만들지 않는다. 두 query는 각각 `bars/`, `invalid-metrics/` checkpoint path를 사용한다. local MVP에서는 같은 Kafka source를 두 번 읽는 비용보다 invalid 관측 가능성을 우선하며, 부하 테스트에서 이 비용을 측정한다. 주요 입력은 bootstrap servers, topic, symbol allowlist, watermark delay, checkpoint root, trigger interval이다.

runner는 변환 로직을 중복 구현하지 않는다. 종료 시 query exception과 마지막 progress를 출력하되 credential이나 raw payload 전체를 log에 남기지 않는다.

## 5. 검증과 invalid 계약

한 record에 여러 문제가 있으면 `reason_codes: array<string>`에 모두 기록한다. reason code는 다음 값으로 제한한다.

- `MALFORMED_JSON`
- `INVALID_EVENT_TYPE`
- `UNSUPPORTED_SCHEMA_VERSION`
- `MISSING_EVENT_ID`
- `MISSING_SOURCE_EVENT_ID`
- `SYMBOL_NOT_ALLOWED`
- `INVALID_PRICE`
- `INVALID_SIZE`
- `INVALID_TIMESTAMP`
- `TIMESTAMP_MISMATCH`

`price > 0`, `size >= 0`, UTC-aware timestamp, allowlist symbol을 요구한다. live future-skew와 replay 최솟값은 실행 모드에 따라 달라지므로 이 PR에서 임의의 wall-clock cutoff로 과거 fixture를 폐기하지 않는다. 해당 정책은 replay runner를 만들 때 configuration으로 추가한다.

invalid row는 테스트에서 분리 결과와 reason을 검증한다. 운영용 `dead-letter.v1` 발행은 다음 sink PR에서 추가한다. 이번 runner의 두 번째 query는 invalid payload 전체를 출력하지 않고 micro-batch별 reason count만 보여준다.

## 6. Trade condition 정책

첫 정책 이름은 `all_valid_trades_v1`이다. schema가 유효한 IEX trade는 conditions를 보존한 채 집계에 포함한다. 근거가 확인되지 않은 condition code를 임의로 제외하지 않는다.

Alpaca 공식 문서는 bar의 open/close, high/low, volume 반영 여부가 tape·condition·bar type에 따라 다르다고 설명한다. 따라서 이번 자체 bar를 Alpaca 제공 bar와 완전히 동일하다고 주장하지 않는다. 후속 parity fixture에서 조건별 차이를 측정한 뒤 별도 version의 inclusion policy로 변경하며, 결과에는 policy version을 남긴다.

## 7. 1분 bar 계산

group key는 `(symbol, source, feed, 1-minute event-time window)`다.

- `bar_start`: window start
- `open`: `(event_timestamp, event_id)`가 가장 작은 trade의 price
- `high`: 최대 price
- `low`: 최소 price
- `close`: `(event_timestamp, event_id)`가 가장 큰 trade의 price
- `volume`: size 합계
- `trade_count`: deduplication 이후 trade 수
- `vwap`: `sum(price * size) / sum(size)`, volume이 0이면 null
- `timeframe`: `1m`
- `is_final`: append output이므로 true
- `condition_policy`: `all_valid_trades_v1`

같은 nanosecond timestamp의 순서는 `event_id`로 결정해 replay 결과를 결정적으로 만든다. 금액 계산은 `DoubleType` 누적 오차 대신 decimal expression을 사용하고 최종 가격은 명시된 scale로 반환한다.

## 8. Watermark, 중복과 checkpoint

기본 watermark delay는 fixture로 검증할 `2 minutes`이며 CLI에서 변경 가능하다. watermark 안에 도착한 late trade는 window state에 포함된다. watermark를 이미 통과한 trade는 final bar를 바꾸지 않으며 Spark progress의 `numRowsDroppedByWatermark`를 증거로 남긴다.

중복 제거 key는 deterministic `event_id`다. `dropDuplicatesWithinWatermark`를 사용해 state 범위를 watermark로 제한한다. 정상 재시작은 동일 checkpoint를 사용해 Kafka offset과 state를 복구한다. 새 checkpoint는 full replay이며 두 동작을 같은 것으로 설명하지 않는다.

## 9. 오류 처리와 보안

- Kafka 시작 실패, query failure와 checkpoint 오류는 non-zero exit로 종료한다.
- invalid data는 query 자체를 죽이지 않고 reason count로 분리한다.
- Spark/Kafka log에 API key, secret과 raw payload 전체를 기록하지 않는다.
- checkpoint는 Git에 포함하지 않으며 로컬 volume/path에 둔다.
- 단일 Kafka broker이므로 replication 기반 고가용성을 주장하지 않는다.

## 10. 테스트와 완료 기준

단위/로컬 Spark 테스트:

- canonical JSON이 normalized trade로 변환된다.
- malformed/type mismatch/allowlist/가격/수량/timestamp 오류가 정확한 reason을 가진다.
- out-of-order 입력에서도 open/close가 event time 순서로 결정된다.
- OHLCV/VWAP/trade count가 fixture 기대값과 일치한다.
- size 합계가 0이면 VWAP가 null이다.

실제 Kafka streaming integration test:

- `raw.market.v1` fixture를 Spark Kafka source가 소비한다.
- 동일 `event_id`가 한 번만 집계된다.
- 정상, duplicate, late-within-watermark, watermark-advancing event를 넣었을 때 final 1분 bar가 기대값과 같다.
- 동일 checkpoint 재시작 시 이미 처리한 offset을 다시 final output으로 만들지 않는다.
- query progress와 checkpoint path가 보고서에 기록된다.

완료 시 README에는 Spark query가 Kafka Consumer 역할을 한다는 점, 실행 명령, 아직 PostgreSQL에 저장하지 않는 현재 경계를 명시한다.

## 11. 공식 근거

- [PySpark 4.2.0 설치 요구사항](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Spark 4.2.0 Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Structured Streaming DataFrame APIs](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html)
- [Alpaca Market Data FAQ — bar aggregation](https://docs.alpaca.markets/docs/market-data-faq#how-are-bars-aggregated)
