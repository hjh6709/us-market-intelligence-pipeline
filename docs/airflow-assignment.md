# 5차시 과제 — 기존 Kafka·Spark 작업 Airflow 자동화

## 한눈에 보는 결과

기존 `Alpaca → Kafka → Spark → PostgreSQL` 코드를 하나의 Airflow DAG로 연결했다. DAG 코드를 고치지 않고 실행 입력값의 `ticker`만 바꿔 NVDA와 SPY를 각각 실행했다.

| 요구사항 | 구현 결과 |
| --- | --- |
| 기존 수집·처리 코드를 DAG로 실행 | 다섯 작업이 순서대로 실행되는 `market_sip_replay_pipeline` 구현 |
| 입력값으로 재실행 | `ticker`, `start`, `end`, `feed`를 Airflow Params로 입력 |
| 값을 바꾼 두 번째 실행 | 같은 10분 구간에서 `NVDA → SPY`로 ticker만 변경 |
| 실행 결과 | NVDA·SPY 모두 다섯 작업과 DAG 최종 상태 `success` |
| 저장 결과 | PostgreSQL `market_bars`, `source=alpaca_replay`, `feed=sip` |

## 자동화한 데이터 흐름

```text
Airflow Params
(ticker, start, end, feed)
        ↓
validate_run_config
입력값 검증 + 실행별 trace_id 생성
        ↓
replay_trades_to_kafka
Alpaca Historical Trades 수집 → raw.market-sip.v1 발행
        ↓
verify_kafka_delivery
같은 trace_id의 Producer·Consumer 건수 비교
        ↓
build_minute_bars_with_spark
JSON 검증 → 중복 제거 → 거래 조건 적용 → 1분봉 집계 → DB Upsert
        ↓
verify_stored_result
Spark 결과 행 수와 PostgreSQL 저장 행 수 비교
```

Kafka 원시 메시지를 Airflow XCom에 넣지는 않는다. XCom에는 ticker, trace ID와 단계별 건수처럼 작은 실행 요약만 전달한다. 실제 체결은 Kafka로, 최종 1분봉은 PostgreSQL로 이동한다.

## DAG 입력값

| Param | 예시 | 의미 |
| --- | --- | --- |
| `ticker` | `NVDA`, `SPY` | 처리할 종목. 영문 대문자·숫자·점·하이픈만 허용 |
| `start` | `2026-08-12T12:25:00Z` | API 조회 시작 UTC 시각, 포함 |
| `end` | `2026-08-12T12:35:00Z` | API 조회 종료 UTC 시각, 미포함 |
| `feed` | `sip` | `sip` 또는 `iex` |

이번 실행 구간은 미국 동부시간 오전 8시 25분부터 8시 35분 직전까지다. 오전 8시 30분 CPI 발표를 가운데 둔 10분 구간으로, 로컬 환경에서 같은 DAG를 빠르게 반복 검증하기 위해 선택했다. 4차시의 NVDA 121분·58,036건 결과를 대체하는 것이 아니라 Airflow 재실행 기능을 확인하는 별도 실행이다.

## 실제 두 번의 실행 결과

| 단계 | NVDA | SPY |
| --- | ---: | ---: |
| Alpaca 실제 SIP 체결 수집 | 4,688 | 3,307 |
| Kafka 발행 | 4,688 | 3,307 |
| 같은 trace의 Kafka 수신 | 4,688 | 3,307 |
| Spark 입력 | 4,688 | 3,307 |
| 형식 오류 | 0 | 0 |
| 중복 | 0 | 0 |
| 가격·VWAP 계산 반영 | 824 | 1,788 |
| Spark 생성 1분봉 | 10 | 10 |
| PostgreSQL 저장 확인 | 10 | 10 |
| DAG 상태 | success | success |

Producer는 Kafka가 실제로 저장한 파티션과 시작·종료 offset을 실행 요약에 남긴다. Consumer와 Spark는 토픽 전체가 아니라 이 범위만 읽는다. 따라서 NVDA 실행은 4,688건을 발행하고 4,688건만 스캔했으며, 뒤이어 실행한 SPY도 앞선 NVDA 데이터를 다시 읽지 않고 SPY의 3,307건만 스캔했다. 재시도하더라도 해당 시도의 offset 범위만 처리하므로 이전 시도의 부분 발행이 현재 실행에 섞이지 않는다.

실제 실행 요약과 정제된 로그는 [Airflow 실행 증거](evidence/airflow-market-replay/README.md)에 있다.

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

### 3. NVDA 실행

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"ticker":"NVDA","start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

### 4. ticker만 SPY로 변경해 재실행

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"ticker":"SPY","start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

## 현재 구현과 다음 단계

현재 구현된 것은 수동으로 입력값을 넣어 실행하는 하나의 DAG, 일시적 실패에 대한 1회 retry, 실행별 trace와 Kafka offset 범위 분리, Producer·Consumer·Spark 단계별 건수 검사와 PostgreSQL 저장 검사까지다. 발행·수신·Spark 입력·유효 고유 건수가 다르거나 validation 오류가 하나라도 있으면 DAG를 실패시킨다.

아직 구현하지 않은 것은 정기 schedule, 거래일 달력과 연결한 자동 구간 선택, 누락 분 탐지 후 자동 backfill, 실패 알림이다. 다음 단계에서는 Airflow가 미수집 구간을 찾고 동일 DAG를 backfill하도록 확장하되, 이번 결과와 계획을 구분한다.
