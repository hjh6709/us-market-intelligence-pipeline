# MVP Design Decisions

상태: proposed, measurement gates defined

기준일: 2026-08-13

이 문서는 데이터 특성, 사용자, Kafka/Spark/Airflow 사용 이유, 저장·조회 방식을 명시한다. 아직 관측하지 않은 처리량은 추정값으로 확정하지 않고 측정 절차와 결정 조건을 정의한다.

## 1. 해결할 문제와 사용자

### 핵심 문제

22개 미국 주식·ETF의 IEX trade event를 지속적으로 수집하고 1분 OHLCV와 가격·거래량 이상 징후로 변환하여, 데이터가 중복·지연되거나 처리 프로세스가 재시작돼도 일관된 결과를 조회할 수 있게 한다. 실시간 경고는 `PRELIMINARY_IEX`로 명시하고 15분 이상 지난 historical SIP bar로 검증해 확정 또는 기각한다.

### 1차 사용자 — 프로젝트 운영자 / 데이터 엔지니어

필요한 답:

- 수집과 처리가 정상인가?
- 마지막으로 처리한 event와 bar는 언제인가?
- Kafka lag, Spark micro-batch, Airflow DAG 상태는 어떤가?
- 중복·지연·잘못된 event가 얼마나 발생했는가?
- 장애 후 데이터가 유실되거나 중복 저장되지 않았는가?

접근 방식: pipeline status, structured logs, Spark query progress, PostgreSQL 검증 query.

### 2차 사용자 — 데이터 분석가

필요한 답:

- 특정 종목과 기간의 1분 OHLCV는 무엇인가?
- 이상 징후 발생 전후 return과 volume feature는 무엇인가?
- 같은 시각의 시장·섹터 ETF는 어떻게 움직였는가?
- 당시 사용 가능한 거시경제 관측값은 무엇인가?

접근 방식: PostgreSQL SQL. FastAPI는 선택 구현이며 SQL 조회를 대체하는 P0 요구사항이 아니다.

### 선택 사용자 — Dashboard/API 사용자

필요한 답:

- 지금 어떤 종목에 최신 alert가 있는가?
- alert를 발생시킨 관측값과 threshold는 무엇인가?
- alert가 IEX 예비 경고인지 SIP 검증이 끝난 경고인지?
- IEX와 SIP의 가격 차이·거래량 coverage는 어느 정도인가?
- 데이터가 최신이며 신뢰 가능한가?

접근 방식: 7회차 선택 구현인 FastAPI/Streamlit. 주문과 포지션 변경 기능은 제공하지 않는다.

## 2. 데이터 특성

| Dataset | 형태 | 도착 방식 | 시간 특성 | P0 여부 | 저장 |
| --- | --- | --- | --- | --- | --- |
| Alpaca IEX market trade | 작은 JSON event | WebSocket 실시간 | event time, out-of-order 가능 | 필수 | Kafka 24h, PostgreSQL IEX 1m bar |
| Alpaca historical SIP bar | 1분 OHLCV JSON | Airflow batch, 15분 이상 지연 | 닫힌 window 검증 | 필수 | PostgreSQL SIP 1m bar/reconciliation |
| Replay market data | Parquet/JSON fixture | 조절 가능한 stream | 원래 inter-arrival 또는 배속 | 필수 | repository fixture, Kafka 경유 |
| FRED macro | JSON observation | Airflow daily batch | 관측일·발표시각·수집시각 분리 | 필수 | PostgreSQL |
| Alpaca news | JSON/document metadata | REST/WebSocket | publish/update time | 선택 | PostgreSQL metadata/event |

정확한 수집 시간, warm-up 기간, 저장 위치와 삭제 정책은 [데이터 수집·수명주기](data-lifecycle.md)에 정의한다. P0 실시간 탐지 범위는 정규장이고 live/recorded 최소 10거래일을 목표로 하며, feed별 과거 20거래일 1분 bar를 baseline warm-up으로 사용한다.

### 현재 확정된 규모

- 분석 universe: 22 symbols
- 실시간 market feed: Alpaca Basic IEX
- 검증 market feed: Alpaca historical SIP, `end <= now - 15m`
- 입력: trade 중심, quote는 P0 제외
- 집계 단위: event-time 1 minute
- local Kafka: single broker
- local Spark Structured Streaming

### 아직 모르는 값

다음 값은 live connection과 replay dataset이 없으므로 아직 확정할 수 없다.

```text
average events/sec
peak events/sec
average and p95 payload bytes
symbol별 event 비중
premarket/regular/after-hours별 속도
daily raw bytes
```

이를 문서상의 임의 숫자로 채우지 않는다.

### 측정 방법

live smoke run과 recorded fixture 생성 시 다음을 같은 `run_id`로 기록한다.

```text
events_total
measurement_duration_seconds
average_events_per_second
max_events_in_1_second_bucket
p95_events_in_10_second_bucket
average_payload_bytes
p95_payload_bytes
events_by_symbol
events_by_partition
```

저장량은 다음 식으로 계산한다.

```text
estimated_raw_bytes_per_day
= average_events_per_second × average_payload_bytes × active_seconds
```

Kafka record/header와 segment overhead가 있으므로 실제 broker directory 크기도 함께 측정한다. 계산값만으로 retention disk를 결정하지 않는다.

## 3. Kafka 결정

### 파일 수집 대신 Kafka를 쓰는 이유

| 요구 | 파일만 사용할 때 | Kafka를 사용할 때 |
| --- | --- | --- |
| 연속 WebSocket event | writer/reader coordination을 직접 구현 | producer가 즉시 publish, Spark가 독립 consume |
| Spark 재시작 | 읽은 위치와 부분 파일 관리 필요 | checkpoint와 Kafka offset으로 복구 |
| 짧은 기간 replay | 파일 재주입 프로그램 필요 | retention 안에서 offset 기반 재소비 가능 |
| backpressure 관찰 | 파일 backlog를 별도 계산 | consumer lag와 처리율로 관찰 |
| 순서 | 파일 작성 순서에 결합 | 같은 partition 안에서 순서 유지 |
| 소비자 추가 | 파일 공유·polling 규칙 필요 | 독립 consumer group 추가 가능 |

Kafka가 모든 저장 문제를 해결하지는 않는다. 장기 분석용 replay fixture는 Parquet/JSON 파일로 유지하고, 애플리케이션 조회 결과는 PostgreSQL에 저장한다. Kafka는 실시간 전달·단기 buffer·재처리 경계다.

### Topic과 partition

| Topic | P0 partition | Key | Retention | 근거 |
| --- | ---: | --- | --- | --- |
| `raw.market.v1` | 3 | symbol | 24h | local parallelism 실험, symbol별 순서, 1일 내 replay |
| `dead-letter.v1` | 1 | original key | 7d | 낮은 예상량, 수동 분석 기간 확보 |

`raw.market.v1=3 partitions`는 처리량으로 산출한 최종값이 아니라 **local MVP 초기 설정**이다. 22개 symbol을 하나의 partition에 몰지 않으면서 local Spark에서 partition 병렬 처리와 skew를 관찰할 수 있는 최소 실험값으로 선택한다.

Partition key는 `symbol`이다. 동일 symbol의 trade가 같은 partition으로 들어가므로 symbol 내부 순서를 보존한다. 서로 다른 symbol 사이의 전체 순서는 보장하거나 필요로 하지 않는다.

### Partition 재검토 조건

6회차 load test에서 아래를 확인한다.

- partition별 record 비중과 lag
- Spark input partitions와 task utilization
- `processedRowsPerSecond < inputRowsPerSecond` 상태의 지속 여부
- micro-batch backlog가 입력 종료 후 해소되는 시간
- 가장 큰 partition의 event 비중

다음 중 하나가 반복되면 partition 증가 실험을 수행한다.

- 지속적인 lag가 발생하고 Spark/DB가 여유 자원을 가지고 있음
- 한 partition의 event 비중이 평균의 2배 이상이며 병목과 함께 관측됨
- task concurrency 부족이 Spark UI에서 병목 원인으로 확인됨

Partition 증가는 자동으로 하지 않는다. PostgreSQL sink나 단일 Spark driver가 병목이라면 Kafka partition을 늘려도 해결되지 않는다.

### Retention 재검토 조건

24시간 retention은 발표·장애 실험을 위한 초기값이다. 아래 식을 실제 측정값으로 계산하고 local disk budget과 비교한다.

```text
required_disk
= measured_bytes_per_hour × retention_hours × safety_factor
```

retention 안에서 장애 복구와 당일 replay가 불가능하면 늘리고, disk pressure가 발생하면 Parquet fixture 보존을 전제로 줄인다.

### Producer 신뢰성 경계

MVP producer는 `confluent-kafka-python`을 사용하고 초기 후보로 `enable.idempotence=true`, `acks=all`을 적용한다. 멱등 Producer와 호환되는 retry/in-flight 설정은 client 기본값을 무작정 덮어쓰지 않고 version matrix smoke test로 확인한다. 각 record는 delivery callback에서 topic/partition/offset 또는 error class를 기록하고, 종료 시 bounded flush 결과를 확인한다.

이 설정만으로 end-to-end exactly-once를 주장하지 않는다. WebSocket reconnect가 같은 trade를 다시 전달할 수 있고 Spark `foreachBatch`의 PostgreSQL write는 at-least-once이므로 다음 세 경계를 함께 사용한다.

```text
Kafka producer idempotence
+ deterministic event_id / Spark deduplication
+ PostgreSQL business unique key / upsert
```

기본 single broker에서 `acks=all`은 현재 ISR의 확인 정책일 뿐 broker 복제나 failover를 만들지 않는다. 기본 failure drill은 Kafka restart recovery이며, broker 장애 중 지속 쓰기는 선택 3-broker `resilience` profile에서만 검증·주장한다.

## 4. Spark 결정

### Pandas 대신 Spark를 쓰는 이유

현재 22개 IEX symbol 처리량만으로는 Pandas/Python consumer도 충분할 가능성이 높다. Spark 선택의 직접적인 이유는 다음 두 가지다.

1. 과정의 필수 산출물로 Structured Streaming 전처리·집계를 직접 구현해야 한다.
2. Kafka source, event-time window, watermark, checkpoint와 micro-batch failure recovery를 하나의 처리 경계에서 검증할 수 있다.

P0 Spark 처리:

```text
explicit schema parsing
provider raw field normalization
validation / invalid split
event-id deduplication
event-time watermark
symbol + 1-minute window aggregation
OHLCV / VWAP / trade count
foreachBatch PostgreSQL upsert
```

P0에서 하지 않는 처리:

- 수백 GB historical backtest
- 복잡한 stream-stream join
- 분산 ML 학습
- 별도 Spark cluster 운영
- Spark를 통한 LLM/news 처리

거시·뉴스와 market bar의 시점 결합은 PostgreSQL의 `as_of` query 또는 선택 Signal Engine에서 수행한다. 억지로 Spark streaming join에 넣지 않는다.

## 5. Airflow 결정

### cron 대신 Airflow를 쓰는 이유

단일 FRED HTTP 요청 하나라면 cron으로도 충분하다. 이 프로젝트의 macro 수집과 지연 market reconciliation은 다음 의존성과 운영 증거가 필요하므로 Airflow local mode를 사용한다.

```text
configuration check
→ extract observations
→ validate/normalize
→ idempotent upsert
→ data quality checks
→ pipeline status update
```

별도 market reconciliation DAG는 다음 흐름을 실행한다.

```text
select finalized IEX windows ending <= now - 20m
→ fetch matching historical SIP bars
→ validate/upsert feed=sip
→ compare IEX and SIP without mixing baselines
→ store reconciliation evidence
→ transition PRELIMINARY_IEX alert to confirmed or rejected
```

Airflow가 제공할 증거:

- task dependency와 실행 순서
- logical date 기준 증분·백필
- timeout, retry, exponential backoff
- 개별 task 성공/실패와 재실행 기록
- UI에서 확인 가능한 DAG run 상태

Airflow는 실시간 trade, Spark query 시작/종료 반복, 초 단위 alert를 담당하지 않는다. 실시간 IEX 경고를 사후 검증할 뿐이다. FRED schedule은 daily로 시작하며 실제 series update 시각을 확인한 후 cron expression을 고정한다. reconciliation schedule은 Alpaca의 무료 SIP 지연 조건을 어기지 않도록 safety margin과 API quota를 측정한 뒤 고정한다.

Dynamic Task Mapping은 9개 FRED series의 독립 실패 격리와 재실행이 실제로 유리할 때만 적용한다. Dataset scheduling도 raw/processed 데이터 준비 완료를 별도 DAG 사이에서 전달해야 할 때만 사용한다. 과정에서 학습했다는 이유만으로 task와 DAG를 분할하지 않는다.

## 6. 저장소 결정

| 저장소 | 선택 | 역할 | 선택 이유 |
| --- | --- | --- | --- |
| PostgreSQL | P0 | feed별 bar/feature, reconciliation, macro, alert/history, pipeline status | 관계형 query, JOIN, unique key/upsert, SQL 분석 |
| Kafka | P0 | raw event 단기 buffer | 실시간 전달, lag 관찰, retention 내 재처리 |
| Parquet/JSON | P0 fixture | deterministic replay | 반복 가능한 demo/load/failure test |
| MongoDB | 미선택 | 없음 | P0 schema가 안정적이고 관계형·시간 조건 query가 중심 |
| S3/object storage | 후속 | 장기 raw archive | local 측정 후 보존 요구가 생길 때 검토 |

PostgreSQL에 raw tick을 장기 저장하지 않는다. Spark가 생성한 1분 bar와 그 이후의 분석 결과만 저장하여 write volume과 query 범위를 제한한다.

## 7. 조회 패턴과 인덱스

인덱스는 예상 가능한 P0 query에만 만든다. 모든 column을 미리 index하지 않는다.

| ID | 사용자/컴포넌트 | Query pattern | Key / index candidate |
| --- | --- | --- | --- |
| Q1 | 분석가 | symbol·기간별 1분 bar를 시간순 조회 | `market_bars(symbol, timeframe, bar_start DESC)` |
| Q2 | Dashboard/운영자 | 최신 `bar_start`의 모든 symbol 조회 | 선택 시 `market_bars(bar_start DESC, symbol)` |
| Q3 | 분석가 | symbol·기간별 feature 조회 | `technical_features(symbol, timeframe, as_of DESC)` |
| Q4 | 운영자 | 상태별 최신/기간별 anomaly alert 조회 | `anomaly_alerts(status, event_timestamp DESC)` 및 `(symbol, event_timestamp DESC)` |
| Q5 | Signal/분석가 | `as_of` 이전 최신 macro observation | `macro_observations(series_id, observation_date DESC, realtime_start DESC)` |
| Q6 | 운영자 | component별 pipeline health | `pipeline_status` primary key `(component, instance)` |
| Q7 | 분석가/운영자 | symbol·기간별 IEX/SIP 차이와 검증 결과 | `market_bar_reconciliations(symbol, bar_start DESC)` |

Business uniqueness:

```text
market_bars:
(symbol, bar_start, timeframe, source, feed)

technical_features:
(symbol, as_of, timeframe, feature_version, source, feed)

macro_observations:
(series_id, observation_date, realtime_start)

anomaly_alerts:
(symbol, event_timestamp, alert_type, threshold_version, source, feed)

market_bar_reconciliations:
(symbol, bar_start, timeframe, rule_version)

alert_reconciliations:
(alert_id, rule_version)
```

표의 index는 migration 후보이며 측정 없이 모두 생성하는 목록이 아니다. Q2의 별도 index는 실제 dashboard/API를 구현할 때만 생성한다. 구현 후 `EXPLAIN (ANALYZE, BUFFERS)`로 Q1·Q3·Q4를 확인하고 사용되지 않는 중복 index는 추가하지 않는다.

## 8. 성능 기준을 정하는 순서

아직 하드웨어와 live rate를 측정하지 않았으므로 임의의 EPS나 latency SLO를 약속하지 않는다.

1. live/replay 1x baseline 측정
2. 병목 없이 유지되는 최고 배율 탐색
3. 첫 병목의 위치를 Kafka, Spark, JDBC/PostgreSQL로 분류
4. 처리율, lag, p95 latency와 resource usage 기록
5. 그 결과로 partition, micro-batch trigger, JDBC batch size를 한 번에 하나씩 조정
6. 같은 fixture로 재측정

최소 성공 조건:

- 정해진 실험 시간 동안 lag가 무한히 증가하지 않는다.
- 입력이 종료되면 backlog가 해소된다.
- final business row 수와 집계값이 fixture expected result와 일치한다.
- checkpoint restart와 full replay 후에도 business duplicate가 없다.

처리 성능 숫자는 6회차 report에서 측정 환경과 함께 기록한다.

## 9. 아직 남겨둔 결정

| 결정 | 현재 상태 | 확정에 필요한 증거 |
| --- | --- | --- |
| 실제 peak EPS | 미측정 | regular/opening session live capture |
| watermark delay | 초기 후보 2분 | late-event distribution과 fixture test |
| Spark trigger interval | 미결정 | baseline micro-batch duration |
| JDBC batch size | 미결정 | DB write latency/load test |
| Kafka partition 최종값 | 초기값 3 | partition skew와 end-to-end load test |
| Q2 latest-bar index | 조건부 | FastAPI/Streamlit 구현 및 query plan |
| 장기 Parquet/S3 archive | 후속 | daily raw bytes와 replay 보존 요구 |
| SIP reconciliation schedule | 미결정 | API quota, DAG duration, confirmation latency |
| SIP confirmation tolerance | 미결정 | 실제 IEX/SIP close 차이와 volume coverage 분포 |

미결정 항목은 구현 중 임의로 숨겨 확정하지 않고 측정 결과와 함께 이 표를 갱신한다.
