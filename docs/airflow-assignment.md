# 5차시 과제 — 기존 Kafka·Spark 작업 Airflow 자동화

## 한눈에 보는 결과

기존 `Alpaca → Kafka → Spark → PostgreSQL` 코드를 하나의 Airflow DAG로 연결했다. 한 번의 DAG 실행에 종목 목록을 전달하면 Airflow가 종목별 작업을 자동으로 만들도록 구성했다.

| 요구사항 | 구현 결과 |
| --- | --- |
| 기존 수집·처리 코드를 DAG로 실행 | `market_sip_replay_pipeline`에서 종목별 수집·Kafka·Spark·DB 검증 연결 |
| 입력값으로 재실행 | `tickers`, `start`, `end`, `feed`를 Airflow Params로 입력 |
| 여러 종목 처리 | Dynamic Task Mapping으로 입력 목록의 종목마다 독립 작업 인스턴스 생성 |
| 값을 바꾼 두 번째 실행 | 첫 실행 `SPY·QQQ·SMH·NVDA`, 두 번째 실행 `SPY·QQQ` |
| 실행 결과 | 첫 실행의 네 종목 작업 16개와 DAG 최종 상태 `success` |
| 저장 결과 | PostgreSQL `market_bars`, `source=alpaca_replay`, `feed=sip` |

## 자동화한 데이터 흐름

```text
Airflow Params
(tickers[], start, end, feed)
        ↓
validate_run_config
목록 검증 + 종목별 config·trace_id 생성
        ↓
Dynamic Task Mapping: 입력 목록의 각 ticker에 아래 경로 생성

  replay_trades_to_kafka[ticker]
  Alpaca Historical Trades 수집 → raw.market-sip.v1 발행
          ↓
  verify_kafka_delivery[ticker]
  같은 trace_id의 Producer·Consumer 건수 비교
          ↓
  build_minute_bars_with_spark[ticker]
  JSON 검증 → 중복 제거 → 거래 조건 적용 → 1분봉 집계 → DB Upsert
          ↓
  verify_stored_result[ticker]
  Spark 결과 행 수와 PostgreSQL 저장 행 수 비교
```

Kafka 원시 메시지를 Airflow XCom에 넣지는 않는다. XCom에는 ticker, trace ID와 단계별 건수처럼 작은 실행 요약만 전달한다. 실제 체결은 Kafka로, 최종 1분봉은 PostgreSQL로 이동한다.

`airflow dags test`는 로컬 테스트 명령이므로 화면에서는 mapped task가 주로 순서대로 실행된다. 그러나 사용자가 종목마다 DAG를 다시 실행하는 구조는 아니다. 한 DAG run 안에 종목별 `map_index`가 생기며, Scheduler 환경에서는 Executor와 task slot 범위 안에서 독립적으로 스케줄될 수 있다.

## DAG 입력값

| Param | 예시 | 의미 |
| --- | --- | --- |
| `tickers` | `["SPY", "QQQ", "SMH", "NVDA"]` | 처리할 종목 목록. 빈 목록·중복·잘못된 기호는 거부 |
| `start` | `2026-08-12T12:25:00Z` | API 조회 시작 UTC 시각, 포함 |
| `end` | `2026-08-12T12:35:00Z` | API 조회 종료 UTC 시각, 미포함 |
| `feed` | `sip` | `sip` 또는 `iex` |

이번 실행 구간은 미국 동부시간 오전 8시 25분부터 8시 35분 직전까지다. 오전 8시 30분 CPI 발표를 가운데 둔 10분 구간으로, 로컬 환경에서 같은 DAG를 빠르게 반복 검증하기 위해 선택했다. 4차시의 NVDA 121분·58,036건 결과를 대체하는 것이 아니라 Airflow 재실행 기능을 확인하는 별도 실행이다.

## 실제 입력 변경 실행 결과

### 실행 A — 프로젝트 분석 대상 네 종목

- 입력: `tickers=[SPY, QQQ, SMH, NVDA]`
- run ID: `manual__2026-08-27T07:30:57.734232+00:00`
- DAG 상태: `success`

| ticker | Alpaca 수집 | Kafka 발행·수신 | Spark 입력 | 형식 오류·중복 | 가격·VWAP 반영 | 생성·저장 1분봉 | mapped task 상태 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SPY | 3,307 | 3,307 / 3,307 | 3,307 | 0 / 0 | 1,788 | 10 / 10 | success |
| QQQ | 6,143 | 6,143 / 6,143 | 6,143 | 0 / 0 | 2,561 | 10 / 10 | success |
| SMH | 931 | 931 / 931 | 931 | 0 / 0 | 315 | 10 / 10 | success |
| NVDA | 4,688 | 4,688 / 4,688 | 4,688 | 0 / 0 | 824 | 10 / 10 | success |
| **합계** | **15,069** | **15,069 / 15,069** | **15,069** | **0 / 0** | **5,488** | **40 / 40** | **success** |

Airflow는 validation 작업 하나와, 종목별로 mapping된 네 단계씩 총 16개의 작업 인스턴스를 만들었다. `map_index 0·1·2·3`은 입력 순서대로 `SPY·QQQ·SMH·NVDA`에 대응하며 모두 성공했다.

### 실행 B — 입력 목록 변경 확인

- 변경 입력: `tickers=[SPY, QQQ]`
- run ID: `manual__2026-08-27T07:32:54.583401+00:00`
- DAG 상태: `success`
- 결과: validation 1개와 종목별 mapped task 8개 모두 `success`

두 번째 실행은 코드를 수정하지 않고 `tickers` 목록만 네 종목에서 두 종목으로 바꾼 실행이다. 저장은 Upsert이므로 같은 종목·같은 분의 행이 추가로 중복되지 않았다.

Producer는 Kafka가 실제로 저장한 파티션과 시작·종료 offset을 종목별 실행 요약에 남긴다. Consumer와 Spark는 토픽 전체가 아니라 해당 mapped task가 발행한 offset 범위만 읽는다. 따라서 공통 토픽을 사용해도 다른 종목이나 이전 시도의 메시지가 현재 처리에 섞이지 않는다.

실제 실행 요약과 정제된 로그는 [Airflow 실행 증거](evidence/airflow-market-replay/README.md)에 있다.
PostgreSQL의 집계와 실제 1분봉 샘플은 [DB 조회 결과](evidence/airflow-market-replay/postgres-result.txt)에서 확인할 수 있다.

## 저장 위치와 형식

최종 결과는 PostgreSQL `market_bars` 테이블에 저장한다.

| 컬럼 | 의미 |
| --- | --- |
| `symbol`, `bar_start`, `timeframe` | 종목과 1분 구간 식별 |
| `open`, `high`, `low`, `close` | 1분 가격 |
| `volume`, `trade_count`, `vwap` | 거래량, 체결 수, 거래량가중평균가격 |
| `source`, `feed` | `alpaca_replay`, `sip` |
| `condition_policy`, `is_final` | 거래 조건 규칙과 완료 봉 여부 |

`(symbol, bar_start, timeframe, source, feed)`가 고유 기준이므로 같은 입력을 다시 실행해도 행이 중복 추가되지 않고 Upsert된다. Kafka는 `raw.market-sip.v1` 공통 토픽을 사용하며 실행별 `trace_id`와 메시지의 종목 필드로 데이터를 구분한다.

## 실행 방법

### 1. 환경 준비

```bash
cp .env.example .env
uv sync

# Python 3.14 기준 Airflow 설치
uv pip install --python .venv/bin/python \
  -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.14.txt
uv pip install --python .venv/bin/python -e .

docker compose up -d postgres kafka kafka-init
```

`.env`에는 실제 `APCA_API_KEY_ID`와 `APCA_API_SECRET_KEY`가 필요하다. 키는 로그나 Git에 올리지 않는다.

### 2. Airflow 로컬 메타데이터와 DAG 확인

```bash
export AIRFLOW_HOME="$PWD/airflow-runtime"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

.venv/bin/airflow db migrate
.venv/bin/airflow dags list-import-errors
```

정상 결과는 `No data found`이며, 이는 DAG import 오류가 없다는 의미다.

### 3. 네 종목을 한 번의 DAG run으로 실행

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"tickers":["SPY","QQQ","SMH","NVDA"],"start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

### 4. 종목 목록을 바꿔 재실행

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"tickers":["SPY","QQQ"],"start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

## 현재 구현과 다음 단계

현재 구현된 것은 여러 종목 목록을 받는 하나의 DAG, 종목별 Dynamic Task Mapping, 일시적 실패에 대한 1회 retry, 종목·실행별 trace와 Kafka offset 범위 분리, Producer·Consumer·Spark 단계별 건수 검사와 PostgreSQL 저장 검사까지다. 발행·수신·Spark 입력·유효 고유 건수가 다르거나 validation 오류가 하나라도 있으면 해당 mapped task와 DAG를 실패시킨다.

아직 구현하지 않은 것은 정기 schedule, 거래일 달력과 연결한 자동 구간 선택, 누락 분 탐지 후 자동 backfill, 실패 알림이다. 다음 단계에서는 Airflow가 미수집 구간을 찾고 동일 DAG를 backfill하도록 확장하되, 이번 결과와 계획을 구분한다.
