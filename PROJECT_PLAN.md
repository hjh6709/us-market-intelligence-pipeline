# 4-Week Project Plan

기준일: **2026-08-13**

최종 발표: **2026-09-12**

과정: **4주, 총 8회**

이 문서는 [최종 프로젝트 비전](docs/final-vision.md)의 **Stage A — Data Pipeline MVP** 실행 계획이다. 수업의 필수 기술과 산출물인 Kafka, Spark Structured Streaming, Airflow를 local 환경에서 직접 구현하고 설명하는 것을 가장 먼저 완료한다.

## 1. 프로젝트 목표

> Alpaca IEX의 미국 주식 실시간 trade 데이터를 Kafka로 수집하고 Spark Structured Streaming으로 1분 OHLCV와 예비 이상 징후를 계산해 PostgreSQL에 저장하며, Airflow로 15분 이상 지난 SIP 데이터의 정합성 검증과 FRED 거시경제 수집을 수행하는 재현 가능한 금융 데이터 파이프라인을 구축한다.

핵심 데모:

```text
Alpaca IEX or deterministic replay
→ Kafka
→ Spark Structured Streaming
→ IEX 1-minute bars / PRELIMINARY_IEX alerts
→ PostgreSQL

Alpaca historical SIP (end <= now - 15m)
→ Airflow reconciliation
→ CONFIRMED_SIP / REJECTED_AFTER_RECONCILIATION

FRED
→ Airflow
→ PostgreSQL
```

뉴스·LLM, FastAPI, Streamlit, Signal Engine은 핵심 파이프라인과 부하·장애 검증이 끝난 뒤 추가하는 선택 구현이다.

## 2. Guideline 반영 결정

과정에서 학습·실습한 기술과 이 프로젝트의 구현·증거·제외 사유는 [과정 연계 문서](docs/course-alignment.md)에 별도로 연결한다. 기술을 사용했다는 사실보다 어떤 문제를 해결했고 어떤 실행 증거를 남겼는지를 완료 기준으로 삼는다.

### Spark를 MVP에 포함한다

22개 IEX 종목의 현재 처리량만 보면 Python consumer도 충분할 가능성이 높다. 그러나 이 과정의 목표와 최종 산출물에 Spark 전처리·집계 코드가 명시되어 있으므로 **Spark local mode를 핵심 처리 엔진으로 채택한다.**

Spark의 역할은 작고 명확하게 제한한다.

```text
Kafka source
→ JSON schema parsing and validation
→ event-time watermark / deduplication
→ symbol + 1-minute window aggregation
→ OHLCV / VWAP / trade count
→ micro-batch idempotent PostgreSQL upsert
```

별도 Spark cluster나 복잡한 stateful ML은 만들지 않는다. 처리량 확대보다 Structured Streaming의 event-time window, checkpoint, failure recovery를 직접 구현·검증하는 것이 목적이다.

### 우선순위

```text
1. Kafka Producer + replay dataset
2. Spark Structured Streaming preprocess/aggregation
3. PostgreSQL schema + idempotent load
4. Airflow SIP reconciliation + FRED DAG
5. Load test + failure recovery
6. Technical/anomaly features
--------------------------------
7. FastAPI / Streamlit
8. News / LLM / composite signal
9. Agent / MCP / RAG
```

선 아래 기능 때문에 필수 산출물이 지연되면 해당 기능을 제거한다.

## 3. MVP Definition of Done

| ID | 필수 결과 | 검증 가능한 수용 기준 |
| --- | --- | --- |
| P0-1 | Kafka 수집 | Alpaca 또는 replay producer가 22개 허용 종목의 provider raw trade payload와 공통 envelope를 `raw.market.v1`에 발행한다. |
| P0-2 | Spark 전처리 | Spark가 schema, symbol, timestamp, price, volume을 검증하고 중복/지연 데이터를 정의된 정책으로 처리한다. |
| P0-3 | Spark 집계 | event-time 1분 window로 OHLCV, VWAP, trade count를 계산한다. |
| P0-4 | 데이터 저장 | `foreachBatch` sink가 PostgreSQL business key upsert를 수행하며 같은 input replay에도 row 수와 값이 일관된다. |
| P0-5 | Feature/anomaly | 확정 IEX bar와 IEX 전용 baseline에서 feature를 계산하고 `PRELIMINARY_IEX` alert와 source/feed를 저장한다. |
| P0-6 | Airflow | historical SIP reconciliation과 FRED DAG가 예약/백필 수집을 수행하며 같은 logical date/window 재실행 시 중복되지 않는다. |
| P0-7 | Load test | replay 배속별 throughput, Spark processing rate, batch duration, Kafka lag, DB latency, CPU/memory를 기록한다. |
| P0-8 | Failure recovery | Kafka/Spark/PostgreSQL 중단, duplicate, out-of-order, invalid event 시나리오의 복구 결과가 남는다. |
| P0-9 | Reproducibility | clean checkout에서 Docker Compose와 문서화된 명령으로 local pipeline과 offline replay를 실행한다. |
| P0-10 | 제출물 | 구조도, 데이터 모델, producer/consumer, Spark 코드, DAG, DB schema/sample, README, 발표/데모 자료가 준비된다. |

P0가 완료되기 전에는 프로젝트가 완료된 것으로 보지 않는다.

## 4. 목표 코드 산출물

실제 구현 시 저장소 스타일에 맞추되 다음 책임을 분리한다.

```text
src/
├── producers/
│   ├── market_producer.py       # Alpaca → Kafka
│   └── replay_producer.py       # fixture → Kafka, load test
├── streaming/
│   └── preprocess.py            # Kafka → Spark → 1m bars → PostgreSQL
├── features/
│   └── anomaly.py               # finalized bar → feature/alert
├── providers/
│   ├── alpaca.py                # IEX stream + delayed SIP bars
│   └── fred.py
├── reconciliation/
│   └── market.py               # IEX/SIP compare + alert transition
├── repositories/
│   └── postgres.py              # idempotent upsert
└── api/                         # optional

airflow/dags/
├── market_reconciliation_dag.py
└── fred_macro_dag.py

tests/
├── fixtures/
├── unit/
└── integration/
```

`consumer.py`라는 이름만 별도로 만들기보다 Spark query 자체가 Kafka consumer임을 README와 발표에서 설명한다. 필요하면 health/inspection용 단순 consumer를 추가하되, 동일 데이터를 이중 처리하지 않는다.

## 5. 8회차 실행 계획

### 1회차 — 주제와 문제 정의

완료 항목:

- [x] 프로젝트 한 줄 목표
- [x] Multi-source dataset 선정
- [x] 데이터 엔지니어링 중심 범위와 장기 비전 분리
- [x] public repository용 README 초안

발표 증거:

- 왜 주식·거시·뉴스 데이터를 선택했는가
- 가격 예측보다 파이프라인 재현성과 이상 징후 설명에 집중하는 이유

### 2회차 — 데이터 및 구조 설계

필수 산출물:

- Dataset A: Alpaca real-time market trades
- Dataset B: Alpaca historical SIP 1-minute bars, 15분 이상 지연 검증
- Dataset C: FRED macro observations
- Dataset D: Alpaca news, 선택 구현
- Dataset E: deterministic replay Parquet/JSON fixtures
- 각 API의 제공 데이터, 실제 선택 field, 제외 범위와 raw→normalized mapping
- `IEX → Kafka → Spark → PostgreSQL`, `SIP/FRED → Airflow → PostgreSQL` 구조도
- Kafka/Spark/Airflow/PostgreSQL 선정 이유
- 공통 event envelope와 DB logical schema
- 1차/2차 사용자와 P0 query pattern
- 초기 Kafka partition/retention과 재검토 조건
- PostgreSQL business key와 최소 index 후보
- 데이터별 수집 시간·warm-up·저장 위치·보존 기간·삭제 기준

Exit gate:

- Spark가 담당할 처리와 담당하지 않을 처리가 한 문장씩 정의되어 있다.
- IEX coverage, 22-symbol limit, SIP 15분 지연, UTC, replay 원칙이 문서에 표시되어 있다.
- IEX/SIP baseline 분리와 alert 상태 전이가 데이터 모델에 정의되어 있다.
- local runtime/Java/Spark/Kafka 호환 버전 검증 계획이 있다.
- 실제 EPS는 미측정으로 표시되고 측정 방법과 partition 재결정 조건이 문서화되어 있다.
- 정규장 10거래일 수집 목표, 과거 20거래일 feed별 warm-up, raw 24시간/분석 결과 90일 보존 정책을 설명할 수 있다.

### 3회차 — Kafka 수집 설계 및 구현

작업:

1. `MarketDataProvider`, raw envelope와 provider payload contract
2. Alpaca producer의 auth, subscribe, heartbeat, reconnect/backoff
3. `confluent-kafka-python` delivery callback과 graceful flush/close
4. `enable.idempotence=true`, `acks=all`, 호환되는 in-flight/retry 설정의 smoke test
5. 원본 payload 보존, deterministic `event_id`, key=`symbol`, topic=`raw.market.v1`
6. replay producer와 속도 배율 설정
7. 정규장 session filter와 market calendar 적용
8. bounded publish retry, 실패 분류와 silent drop 없는 logging

발표 증거:

- producer 코드 흐름
- 실제/fixture JSON sample
- topic/partition/key/retention 선택 이유
- subscribe acknowledgement와 replay 실행 결과

Exit gate:

- live credential이 있으면 22종목 subscription이 확인된다.
- credential이 없어도 replay fixture가 Kafka에 동일 schema로 발행된다.
- producer 재시작 후 같은 fixture의 `event_id`가 변하지 않는다.
- publish 성공·실패가 delivery callback으로 관찰되며 종료 시 미전송 record를 확인한다.
- single broker의 `acks=all`은 복제 고가용성을 만들지 않는다는 제한을 설명할 수 있다.

### 4회차 — Spark 전처리·집계 및 저장

작업:

1. Spark local Structured Streaming의 Kafka source
2. Alpaca raw JSON schema parsing → normalized `MarketTrade` → valid/invalid 분기
3. `event_timestamp` watermark와 event-id dedup
4. Alpaca trade condition 정책을 적용한 symbol + 1-minute event-time window OHLCV/VWAP/count
5. append output mode로 watermark 이후 final bar 출력
6. checkpoint directory 분리
7. `foreachBatch` PostgreSQL business-key upsert
8. finalized bar 기반 기본 return/volume feature

발표 증거:

- Spark DataFrame transformation 흐름
- watermark와 append output mode 선택 이유
- checkpoint와 DB upsert의 역할 차이
- `market_bars`/`technical_features` schema와 sample rows

Exit gate:

- 정상, duplicate, late-within-watermark, too-late fixture 결과가 기대값과 같다.
- 같은 replay를 다시 실행해도 DB business row가 증가하지 않는다.
- Spark checkpoint를 유지한 재시작과 새 checkpoint 기반 full replay를 각각 설명할 수 있다.

### 5회차 — Airflow DAG

작업:

1. 15분 schedule에서 `window_end <= now-20m`인 닫힌 window를 고르는 SIP reconciliation provider/task
2. SIP bar validate/upsert → IEX/SIP 비교 → alert 상태 전이
3. FRED 9개 series를 일 1회 수집하고 최근 7일을 overlap하는 extract → validate → upsert → quality check task
4. logical date/window 기반 증분·백필 범위와 idempotency
5. retry, exponential backoff, timeout, pipeline status 기록

권장 최소 series:

`CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE`, `UNRATE`, `DFF`, `DGS2`, `DGS10`, `VIXCLS`.

Exit gate:

- 같은 logical date로 DAG를 두 번 실행해도 중복이 없다.
- 같은 market window를 다시 검증해도 reconciliation과 alert history가 중복되지 않는다.
- SIP 실패/누락 시 alert는 `PRELIMINARY_IEX` 상태를 유지한다.
- fixture contract test와 실제 API smoke test가 분리되어 있다.
- 429, timeout, missing value를 재현한 task test가 통과한다.

### 6회차 — Load test 및 장애 대응

부하 방식:

```text
recorded/replay dataset
→ 1x → 10x → 50x → 100x
→ 요구 성능을 벗어날 때까지 단계 증가
```

같은 replay dataset과 목표 배속에서 Producer 설정도 별도로 비교한다. `enable.idempotence=true`와 `acks=all`은 신뢰성 경계로 고정하고, 한 번에 한 축을 바꾼 baseline 포함 최소 3개 후보를 각 3회 실행한다.

| 후보 | 변경 축 | 비교 목적 |
| --- | --- | --- |
| Reliability baseline | 보수적인 `batch.size`, `linger.ms`, `compression.type=none` | 지연·처리량 기준선 |
| Batching candidate | `batch.size` 또는 `linger.ms` 한 축 | 호출/압축 비용과 delivery latency trade-off |
| Compression candidate | `compression.type=lz4` | bytes/event·CPU·처리량 trade-off |

설정별 producer events/sec, delivery latency p95, 실패율, bytes/event와 CPU를 같은 실행 ID로 기록한다. 신뢰성 조건이 다른 `acks=1`과 `acks=all`을 단순 성능 최적화 후보처럼 비교하지 않는다.

측정 항목:

```text
producer events/sec
Kafka consumer lag
Spark inputRowsPerSecond / processedRowsPerSecond
micro-batch duration and backlog
event-time-to-DB latency p50/p95
PostgreSQL batch write latency
CPU / memory / disk
duplicate / invalid / too-late counts
```

장애 시나리오:

- Alpaca/WebSocket disconnect
- Spark process restart with checkpoint
- PostgreSQL unavailable during `foreachBatch`
- Kafka restart
- duplicate/out-of-order/too-late events
- corrupted JSON/schema version mismatch

검증 환경:

- 필수: structured log, Spark query progress, Kafka lag와 system metric을 실행 ID별 CSV/JSON report로 export
- 선택: 로컬 자원 여유가 있을 때 `monitoring` profile의 Prometheus/Grafana와 exporter로 같은 metric을 시각화
- 기본 single broker: Kafka 프로세스 재시작 후 Spark 소비 재개 검증
- 조건부 multi-broker 실험: P0 load/failure gate를 통과하고 자원 여유가 확인될 때만 3-broker KRaft와 replication factor 2 이상을 후보로 검토하며 profile 이름·설정은 구현 시 확정

Exit gate:

- 각 배속의 결과가 표나 차트로 남는다.
- Producer 설정별 3회 측정값과 선택 근거가 표로 남는다.
- 필수 metric report가 실행 ID와 함께 남고, Prometheus/Grafana를 사용했다면 같은 실행의 dashboard를 함께 남긴다.
- 처음 병목이 발생한 지점과 근거 metric을 설명한다.
- 장애 후 데이터 유실·중복 여부와 복구 시간을 기록한다.
- 무제한 retry가 없고 DLQ/실패 상태가 관찰된다.
- single-broker 재시작 복구와, 조건부로 수행했을 때만 multi-broker failover를 구분해 설명한다.

### 7회차 — API/inference 선택 구현 및 통합

P0 안정화가 먼저다. 남은 시간에 아래 순서로 하나의 얇은 수직 기능만 추가한다.

1. FastAPI read endpoints: latest bars, alerts, pipeline status
2. Streamlit overview
3. Alpaca News dedup/filter + LLM structured event
4. Technical/Macro/Event composite signal

Exit gate:

- 추가 기능이 없어도 P0 데모는 완결된다.
- 구현했다면 endpoint schema, sample response, error/freshness 상태가 문서화된다.
- LLM은 신호나 주문을 직접 생성하지 않는다.

### 8회차 — 최종 발표

데모 순서:

1. 데이터 출처와 IEX 한계를 설명한다.
2. replay producer로 Kafka에 trade를 발행한다.
3. Spark UI/metrics와 1분 window 결과를 보여준다.
4. PostgreSQL IEX bar, feature, `PRELIMINARY_IEX` alert를 확인한다.
5. 15분 이상 지난 SIP fixture/API로 Airflow reconciliation을 실행하고 confirmed/rejected 상태 전이를 확인한다.
6. Airflow FRED DAG와 macro data를 확인한다.
7. load-test 결과와 장애 복구 trace를 설명한다.
8. 선택 구현이 있으면 API/dashboard를 보여준다.
9. Agent·MCP·RAG는 검증된 후속 단계로만 제시한다.

## 6. 처리 경계와 Trigger

### Spark Market Processor

```text
Trade
→ validation/deduplication
→ event-time 1m OHLCV
→ PostgreSQL market_bars
```

### Feature/Anomaly Engine

```text
Finalized 1m bar
→ same-feed return / volume baseline / ATR-derived feature
→ technical_features
→ PRELIMINARY_IEX anomaly_alerts
```

### Airflow Market Reconciliation

```text
IEX finalized window ending <= now - 20m
+ matching historical SIP bar and SIP baseline
→ reconciliation evidence
→ CONFIRMED_SIP or REJECTED_AFTER_RECONCILIATION
```

### Optional Signal Engine

**1분봉이 watermark 정책에 따라 확정될 때** 해당 bar 시각의 snapshot으로 계산한다.

```text
bar finalized @ T
+ latest technical snapshot <= T
+ latest released/ingested macro snapshot <= T
+ latest valid event snapshot <= T
→ market signal as_of=T
```

`T` 이후에 알려진 정보를 사용하지 않는다.

## 7. Kafka topic 결정

P0 topic:

| Topic | 역할 | Key | Consumer |
| --- | --- | --- | --- |
| `raw.market.v1` | provider raw trade payload + common envelope | symbol | Spark Structured Streaming |
| `dead-letter.v1` | 처리할 수 없는 event와 오류 metadata | original key | inspection/reporting |

선택 구현:

| Topic | 생성 조건 |
| --- | --- |
| `raw.news.v1` | live/replay news collector와 news processor를 실제 구현할 때 |

`market.bars.1m.v1`, `market.events.v1`, `market.features.v1`, `market.signals.v1`은 실제 두 번째 consumer가 생기기 전에는 만들지 않는다. P0의 Spark 결과는 PostgreSQL에 직접 upsert한다.

## 8. Late event와 재처리

- Spark는 `event_timestamp`에 configured watermark를 적용한다.
- watermark 안의 late event는 Spark state에서 해당 window 집계를 갱신한다.
- watermark보다 오래된 event는 설정된 정책에 따라 drop/DLQ metric으로 기록한다.
- `foreachBatch`는 at-least-once write가 가능하므로 DB business key upsert가 필수다.
- 정상 재시작은 Spark checkpoint에서 Kafka offset과 state를 복구한다.
- full historical rebuild는 Kafka retention 안의 event 또는 deterministic replay dataset을 사용한다.
- PostgreSQL bar만으로 raw trades를 완전히 복원할 수 있다고 주장하지 않는다.

P0 output mode는 append로 정해 final bar만 DB에 저장한다. 정확한 watermark delay는 fixture로 동작을 검증한 뒤 고정한다. 초기 후보는 2분이지만 측정 전 확정값으로 취급하지 않는다.

## 9. 테스트 전략

### Unit / DataFrame transformation

- provider payload → normalized trade
- deterministic event id
- valid/invalid schema 분기
- OHLCV/VWAP aggregation
- feature/anomaly threshold와 warm-up
- IEX/SIP baseline isolation과 alert state transition
- UTC/DST/calendar transform
- FRED normalization

### Integration

- replay → Kafka → Spark → PostgreSQL
- Spark checkpoint restart
- FRED fixture → Airflow task → PostgreSQL
- IEX/SIP bar fixture → reconciliation task → alert status/history
- optional news fixture → processor/LLM stub → PostgreSQL

### Contract

- Alpaca market/news response fixture
- FRED response fixture
- Kafka event schema version compatibility
- PostgreSQL unique key/upsert

실제 외부 API test는 별도 marker로 기본 회귀 suite와 분리한다.

## 10. 위험과 완화

| 위험 | 영향 | 완화 |
| --- | --- | --- |
| Spark local이 현재 데이터량에 과함 | 개발·메모리 복잡도 | 역할을 1분 집계로 제한하고 local profile 및 resource 측정 |
| Java/Spark/Kafka connector 호환 | 실행 실패 | 2회차에 version matrix smoke test, 버전 고정 |
| JDBC `foreachBatch` 중복 | DB row 증가 | business unique key + upsert + replay integration test |
| watermark 오해 | late data 유실/지연 | output mode별 fixture와 dropped/updated count 기록 |
| IEX가 전체 시장 거래량이 아님 | 분석 과장 | `feed=iex` 표시, NBBO/전체시장 주장 금지 |
| historical SIP 검증 실패·지연 | 예비 경고 확정 지연 | alert를 preliminary로 유지, bounded retry, pending/confirmation latency 관측 |
| IEX/SIP baseline 혼합 | 통계 왜곡과 잘못된 판정 | feed를 business key에 포함하고 feed별 feature/baseline contract test |
| API 가입·quota 변경 | live demo 중단 | 동일 schema replay를 기본 데모로 준비 |
| Airflow+Spark+Kafka local 자원 | stack 불안정 | Compose profile, 순차 실행 가능, CPU/memory 측정 |
| single broker를 HA처럼 설명 | 잘못된 장애 대응 주장 | 기본 환경은 restart recovery만 검증하고 multi-broker는 P0 이후 조건부 실험으로 분리 |
| monitoring stack의 메모리 부하 | core pipeline 불안정 | `monitoring` profile로 분리하고 측정 시간에만 실행 |
| OCI 공개 포트·secret 노출 | 계정·데이터 보안 위험 | NSG 최소 개방, 내부 서비스 비공개 binding, secret 주입, volume backup |
| UI/LLM 범위 확장 | 필수 코드 미완성 | P0 gate 이후에만 선택 구현 시작 |
| 장외 발표 | 실시간 event 없음 | timestamped replay dataset 사용 |

## 11. 최종 제출 체크리스트

- [ ] 파이프라인 구조도와 데이터 흐름
- [ ] 데이터 모델과 sample rows
- [ ] Kafka market/replay producer
- [ ] Kafka Consumer 역할의 Spark Structured Streaming `preprocess.py`와 checkpoint·offset·lag 증거
- [ ] PostgreSQL schema, migration, load/upsert logic
- [ ] Airflow historical SIP reconciliation DAG와 FRED DAG
- [ ] IEX/SIP reconciliation 결과와 alert 상태 전이 이력
- [ ] load-test dataset, runner, metric report
- [ ] 실행 ID별 structured log·CSV/JSON metric report
- [ ] 선택: Prometheus/Grafana dashboard와 조건부 multi-broker 실험 결과
- [ ] 장애 시나리오와 복구 결과
- [ ] unit/integration/contract tests
- [ ] Docker Compose local 실행 환경
- [ ] README: 개요, 실행법, 구조도, 제약
- [ ] 과정 학습 내용 → 구현 → 발표 증거 연결표
- [ ] 발표자료와 replay 기반 데모
- [ ] 선택: FastAPI/Streamlit 또는 news/LLM

## 12. 성공 판단

다음 질문에 코드와 측정 결과로 답하면 프로젝트는 성공이다.

- Kafka event가 Spark의 event-time window를 거쳐 올바른 1분 bar가 되는가?
- checkpoint 재시작과 full replay의 차이를 설명할 수 있는가?
- 중복·지연·DB 장애에도 PostgreSQL 결과가 일관적인가?
- Airflow 재실행과 백필이 멱등적인가?
- 실시간 IEX 예비 경고와 지연 SIP 검증을 구분하고 feed별 baseline을 지켰는가?
- 처리량과 지연의 병목을 metric으로 찾았는가?
- 사용한 각 플랫폼이 어떤 문제를 해결하는지 설명할 수 있는가?

기술 개수보다 이 여섯 가지를 직접 구현하고 검증하는 것이 4주 과정의 완료 기준이다.
