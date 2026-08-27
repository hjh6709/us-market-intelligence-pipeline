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
- 시간: 2026-08-12 오전 7시 30분 이상, 9시 31분 미만(미국 동부시간)
- 기준 이벤트: 오전 8시 30분 CPI 발표
- 첫 실행 종목: `SPY`, `QQQ`, `SMH`, `NVDA`

지난 Kafka·Spark 과제와 같은 CPI 발표 전 60분부터 발표 후 60분까지의 121분 구간을 사용했다. 따라서 NVDA는 기존 결과와 동일하게 원시 체결 58,036건을 처리해 121개 1분봉을 만들며, 이번에는 같은 범위를 세 종목에도 확장했다.

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
| `start` | `2026-08-12T11:30:00Z` | 조회 시작 시각(포함), 미국 동부시간 오전 7시 30분 |
| `end` | `2026-08-12T13:31:00Z` | 조회 종료 시각(미포함), 미국 동부시간 오전 9시 31분 |
| `feed` | `sip` | Alpaca 시장 데이터 범위 |

빈 종목 목록, 중복 종목, 잘못된 종목 형식과 시작·종료 시각 오류는 수집 전에 실패하도록 검사한다.

## 실제 실행 결과

### 실행 A — 네 종목

- 입력: `tickers=[SPY, QQQ, SMH, NVDA]`
- run ID: `manual__2026-08-27T07:56:50.333255+00:00`
- DAG 최종 상태: `success`

| 종목 | Alpaca 수집 | Kafka 발행 | Kafka 수신 | Spark 입력 | PostgreSQL 1분봉 | 종목 작업 상태 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| SPY | 21,270 | 21,270 | 21,270 | 21,270 | 119 | success |
| QQQ | 27,638 | 27,638 | 27,638 | 27,638 | 121 | success |
| SMH | 11,174 | 11,174 | 11,174 | 11,174 | 111 | success |
| NVDA | 58,036 | 58,036 | 58,036 | 58,036 | 121 | success |
| **합계** | **118,118** | **118,118** | **118,118** | **118,118** | **472** | **success** |

네 종목 모두 수집·발행·수신·Spark 입력 건수가 일치했고 형식 오류와 중복은 0건이었다. Spark가 만든 결과는 총 472개의 1분봉이다. NVDA와 QQQ는 121개 분에 모두 봉이 있었고, SPY는 119개, SMH는 111개였다. Spark의 provider 호환 거래 조건을 적용한 뒤 봉을 만들 수 없는 분은 임의로 채우지 않았기 때문에 종목별 행 수가 다르다.

### 왜 121개보다 적은 종목이 있는가

`[07:30, 09:31)`은 121개의 **예상 분 구간**이다. 그러나 한 분 안에 체결이 있더라도 시가·고가·저가·종가를 계산할 수 있는 체결이 하나도 없으면 유효한 1분봉을 만들 수 없다.

PostgreSQL의 예상 121개 시각과 실제 저장 시각을 대조한 뒤, 비어 있는 분의 Alpaca SIP 원시 체결과 Alpaca 1분봉 API를 다시 조회했다. 아래 시각은 모두 2026년 8월 12일 미국 동부시간(ET) 기준이다. 마지막 열은 저장 결과가 아니라 Spark 결과가 provider와 같은지를 확인한 **외부 교차 검증 결과**다.

| 종목 | 비어 있는 분(ET) | 원시 체결 | 거래량·거래 건수 반영 | OHLC·VWAP 가격 반영 | Alpaca 1분봉 API 교차 확인(저장 아님) |
| --- | --- | ---: | ---: | ---: | --- |
| SPY | 07:33 | 9 | 9 | 0 | 없음 |
| SPY | 07:37 | 31 | 31 | 0 | 없음 |
| SMH | 07:30 | 6 | 6 | 0 | 없음 |
| SMH | 07:31 | 4 | 4 | 0 | 없음 |
| SMH | 07:41 | 7 | 7 | 0 | 없음 |
| SMH | 07:42 | 2 | 2 | 0 | 없음 |
| SMH | 07:46 | 3 | 3 | 0 | 없음 |
| SMH | 07:49 | 2 | 2 | 0 | 없음 |
| SMH | 07:52 | 3 | 3 | 0 | 없음 |
| SMH | 08:17 | 4 | 4 | 0 | 없음 |
| SMH | 08:21 | 2 | 2 | 0 | 없음 |
| SMH | 08:22 | 3 | 3 | 0 | 없음 |

여기서 Alpaca payload의 소문자 `i` 필드는 개별 체결을 식별하는 **trade ID**다. Odd Lot을 뜻하는 값은 `c`(trade conditions) 배열 안의 대문자 `I`다. `I`는 우리가 수량을 보고 새로 붙인 분류가 아니라, 거래소가 SIP에 보고한 sale condition을 Alpaca가 원시 체결에 전달한 값이다. Odd Lot은 표준 거래단위인 round lot보다 작은 체결이며, 미국 주식에서는 일반적으로 100주 미만을 말한다.

[CTA CTS 명세](https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Input_Specification.pdf)·[UTP feed 명세](https://www.utpplan.com/DOC/UtpBinaryOutputSpec_3.0.pdf)와 [Alpaca Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)는 `I`를 Odd Lot Trade로 정의한다. Alpaca의 봉 계산 규칙에서는 `I` 조건 체결이 거래량에는 반영되지만 open·high·low·close 가격은 갱신하지 않는다. 여러 조건이 함께 있으면 가장 엄격한 조건을 적용한다.

이번 12개 분의 모든 체결에는 `c` 배열 안에 대문자 `I`가 포함돼 있었다. 따라서 SPY의 두 분에는 원시 체결 40건, SMH의 열 분에는 36건이 있었지만 OHLC와 VWAP 가격을 만들 수 있는 체결은 각각 0건이었다.

Spark는 가격 체결이 없는 분을 0원이나 직전 가격으로 채우지 않았다. 교차 확인한 Alpaca 1분봉 API에도 동일한 분이 존재하지 않았다. 따라서 SPY 119개와 SMH 111개는 수집 실패가 아니라 provider 규칙에 따른 **정상 공백**이다. 반대로 Alpaca 1분봉 API에는 봉이 있는데 Spark 또는 PostgreSQL에만 없다면 파이프라인 누락으로 판단해야 한다.

### 실행 B — 종목 목록 변경

- 변경 입력: `tickers=[SPY, QQQ]`
- run ID: `manual__2026-08-27T07:58:07.693993+00:00`
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
- 정상 공백과 실제 수집 누락을 구분하는 1분봉 품질 검사와 자동 backfill
- 최종 실패 알림

다음 단계에서는 Airflow가 예상 분 목록과 원시 체결·provider 1분봉·저장 결과를 비교하도록 확장할 계획이다. 가격 반영 체결이 없어 provider 봉도 없는 분은 정상 공백으로 기록하고, provider 봉은 있지만 저장 결과만 없는 분에만 같은 DAG로 backfill을 실행한다.

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
  -c '{"tickers":["SPY","QQQ","SMH","NVDA"],"start":"2026-08-12T11:30:00Z","end":"2026-08-12T13:31:00Z","feed":"sip"}'
```

### 실행 B

```bash
.venv/bin/airflow dags test market_sip_replay_pipeline \
  -c '{"tickers":["SPY","QQQ"],"start":"2026-08-12T11:30:00Z","end":"2026-08-12T13:31:00Z","feed":"sip"}'
```

실제 DAG 코드는 [`dags/market_replay_pipeline.py`](../dags/market_replay_pipeline.py)에서 확인할 수 있다.
