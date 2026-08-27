# 4차시 과제 — 지금 만든 작업을 Airflow로 자동화하기

## 이번 과제에서 한 일

지난 과제에서 만든 `Alpaca → Kafka → Spark → PostgreSQL` 파이프라인을 Airflow DAG 하나로 연결했다. 실행할 종목과 시간 범위를 입력하면 Airflow가 수집부터 저장 확인까지 순서대로 실행한다.

이번에는 첫 실행에 `SPY·QQQ·SMH·NVDA` 네 종목을 넣었다. 두 번째 실행에서는 코드를 고치지 않고 종목 목록을 `SPY·QQQ`로 바꿔 같은 파이프라인을 다시 실행했다.

## 과제 요구사항 확인

| 필수 내용 | 구현·제출 결과 |
| --- | --- |
| 기존 수집·처리 코드를 Airflow DAG로 실행 | Alpaca 수집, Kafka 전달 확인, Spark 1분봉 처리, PostgreSQL 저장 확인을 하나의 DAG로 연결 |
| 코드를 고치지 않고 다시 실행할 입력값 | `tickers`, `start`, `end`, `feed` 제공 |
| 값을 바꿔 한 번 더 실행 | 실행 A는 네 종목, 실행 B는 `SPY·QQQ`로 변경 |
| 실행 화면 또는 로그 | [두 실행의 Airflow 작업 상태](evidence/airflow-market-replay/multi-symbol-task-states.txt) |
| 만들어진 결과 | [PostgreSQL 네 종목 조회 결과](evidence/airflow-market-replay/postgres-result.txt) |
| GitHub에 올리지 않은 데이터 | API key, Airflow 로컬 DB·전체 로그, 대용량 원시 체결 payload |

## 어떤 데이터를 처리했는가

- 출처: Alpaca Historical Trades API
- feed: `sip`
- 시간: 2026-08-12 오전 8시 25분 이상, 8시 35분 미만(미국 동부시간)
- 기준 이벤트: 오전 8시 30분 CPI 발표
- 첫 실행 종목: `SPY`, `QQQ`, `SMH`, `NVDA`

이번 10분 구간은 Airflow가 같은 파이프라인을 여러 입력으로 반복 실행할 수 있는지 빠르게 확인하기 위해 선택했다. 지난 Kafka·Spark 과제에서 사용한 NVDA 121분·58,036건을 대체하는 데이터가 아니라, Airflow 자동화 검증을 위한 별도 구간이다.

## Airflow가 자동화한 흐름

```text
입력값
종목 목록 + 시작 시각 + 종료 시각 + feed
        ↓
Airflow가 입력값 검사
        ↓
입력한 각 종목에 아래 작업 생성

Alpaca 실제 과거 체결 수집
        ↓
Kafka에 전송
        ↓
보낸 건수와 받은 건수 확인
        ↓
Spark 검증·중복 확인·1분봉 생성
        ↓
PostgreSQL 저장
        ↓
Spark 결과 수와 DB 저장 수 확인
```

사용자가 종목마다 DAG를 다시 실행하는 구조는 아니다. 한 번의 DAG 실행에 종목 목록을 넣으면 Airflow가 종목별 작업을 자동으로 나눈다. 따라서 한 종목에서 문제가 발생했을 때 어느 종목의 어느 단계에서 실패했는지 확인할 수 있다.

## 바꿀 수 있는 입력값

| 입력값 | 첫 실행 예시 | 의미 |
| --- | --- | --- |
| `tickers` | `["SPY", "QQQ", "SMH", "NVDA"]` | 처리할 종목 목록 |
| `start` | `2026-08-12T12:25:00Z` | 조회 시작 시각(포함) |
| `end` | `2026-08-12T12:35:00Z` | 조회 종료 시각(미포함) |
| `feed` | `sip` | Alpaca 시장 데이터 범위 |

빈 종목 목록, 중복 종목, 잘못된 종목 형식과 시작·종료 시각 오류는 수집 전에 실패하도록 검사한다.

## 실제 실행 결과

### 실행 A — 네 종목

- 입력: `tickers=[SPY, QQQ, SMH, NVDA]`
- run ID: `manual__2026-08-27T07:30:57.734232+00:00`
- DAG 최종 상태: `success`

| 종목 | Alpaca 수집 | Kafka 발행 | Kafka 수신 | Spark 입력 | PostgreSQL 1분봉 | 종목 작업 상태 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| SPY | 3,307 | 3,307 | 3,307 | 3,307 | 10 | success |
| QQQ | 6,143 | 6,143 | 6,143 | 6,143 | 10 | success |
| SMH | 931 | 931 | 931 | 931 | 10 | success |
| NVDA | 4,688 | 4,688 | 4,688 | 4,688 | 10 | success |
| **합계** | **15,069** | **15,069** | **15,069** | **15,069** | **40** | **success** |

네 종목 모두 수집·발행·수신·Spark 입력 건수가 일치했다. 형식 오류와 중복은 0건이었고, 종목마다 10개씩 총 40개의 1분봉을 PostgreSQL에서 확인했다.

### 실행 B — 종목 목록 변경

- 변경 입력: `tickers=[SPY, QQQ]`
- run ID: `manual__2026-08-27T07:32:54.583401+00:00`
- DAG 최종 상태: `success`

코드를 수정하지 않고 종목 목록만 네 종목에서 두 종목으로 바꿨다. Airflow가 두 종목의 작업을 다시 만들었고 모든 단계가 성공했다. 같은 종목과 1분 구간은 Upsert하기 때문에 재실행 후에도 PostgreSQL에 중복 행이 생기지 않았다.

## 최종 저장 결과

결과는 PostgreSQL `market_bars` 테이블에 저장한다. 한 행은 한 종목의 1분봉이다.

| 저장 내용 | 설명 |
| --- | --- |
| 종목·시각 | `symbol`, `bar_start`, `timeframe` |
| 가격 | 시가, 고가, 저가, 종가 |
| 거래 정보 | 거래량, 체결 수, VWAP |
| 출처 | `source=alpaca_replay`, `feed=sip` |

`종목 + 1분 시작 시각 + 주기 + 출처 + feed`를 고유 기준으로 사용한다. 같은 입력을 다시 처리하면 행을 새로 추가하지 않고 기존 결과를 갱신한다.

실제 결과와 로그:

- [Airflow 실행 증거 요약](evidence/airflow-market-replay/README.md)
- [실행 A·B 입력과 단계별 건수](evidence/airflow-market-replay/multi-symbol-run-summary.json)
- [두 실행의 Airflow 작업 상태](evidence/airflow-market-replay/multi-symbol-task-states.txt)
- [PostgreSQL 네 종목 집계와 1분봉 샘플](evidence/airflow-market-replay/postgres-result.txt)

## 현재 구현과 다음 단계

### 현재 구현

- 여러 종목과 시간 범위를 입력받는 수동 실행 DAG
- 종목별 수집 → Kafka → Spark → PostgreSQL 작업
- 단계별 건수 불일치와 잘못된 데이터 발견 시 실패 처리
- 같은 결과가 중복 저장되지 않는 Upsert
- 일시적인 실패에 대한 1회 재시도

### 아직 구현하지 않은 내용

- 정해진 시간에 실행하는 schedule
- 미국 거래일과 경제지표 일정에 맞춘 자동 실행 구간 선택
- 누락된 1분봉 탐지와 자동 backfill
- 최종 실패 알림

다음 단계에서는 Airflow가 미수집 구간을 찾아 같은 DAG로 다시 수집하도록 확장할 계획이다.

## 재현 방법

### 환경 준비

```bash
cp .env.example .env
uv sync

uv pip install --python .venv/bin/python \
  -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.14.txt
uv pip install --python .venv/bin/python -e .

docker compose up -d postgres kafka kafka-init
```

`.env`에는 `APCA_API_KEY_ID`와 `APCA_API_SECRET_KEY`가 필요하다. 실제 key는 Git에 올리지 않는다.

### 실행 A

```bash
export AIRFLOW_HOME="$PWD/airflow-runtime"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

.venv/bin/airflow db migrate
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"tickers":["SPY","QQQ","SMH","NVDA"],"start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

### 실행 B

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"tickers":["SPY","QQQ"],"start":"2026-08-12T12:25:00Z","end":"2026-08-12T12:35:00Z","feed":"sip"}'
```

실제 DAG 코드는 [`dags/market_replay_pipeline.py`](../dags/market_replay_pipeline.py)에서 확인할 수 있다.
