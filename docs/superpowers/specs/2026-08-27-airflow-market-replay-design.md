# Airflow 시장 데이터 재생 파이프라인 설계

## 목표

기존의 `Alpaca Historical Trades → Kafka → Spark → PostgreSQL` 흐름을 Airflow DAG 하나로 실행한다. 코드를 수정하지 않고 Airflow 실행 입력값으로 종목과 조회 구간을 바꿀 수 있어야 하며, NVDA와 다른 종목을 각각 실행한 결과를 과제 증거로 남긴다.

## 이번 과제 범위

- Airflow DAG에서 과거 SIP 체결 수집, Kafka 발행, Kafka 수신 건수 확인, Spark 1분봉 처리, PostgreSQL 저장 확인을 순서대로 실행한다.
- DAG 입력값은 `ticker`, `start`, `end`, `feed`로 받는다.
- 동일한 DAG를 NVDA와 SPY에 각각 실행한다.
- 발표용 실행은 CPI 발표 시각 주변의 짧은 동일 구간을 사용해 로컬에서도 반복 실행하기 쉽게 한다. 기존 121분 NVDA 검증 결과는 그대로 유지한다.
- 실행 로그에는 수집 건수, Kafka 발행·수신 건수, Spark 입력·오류·중복 건수, 생성된 1분봉 수와 DB 저장 확인 결과를 남긴다.
- 자동 스케줄, 복잡한 장애 복구, 누락 구간 자동 재수집은 이번 필수 범위에서 제외한다.

## 실행 흐름

```text
Airflow 실행 입력
(ticker, start, end, feed)
        ↓
입력값 검증 및 trace_id 생성
        ↓
Alpaca Historical Trades API 수집
        ↓
Kafka raw.market-sip.v1 발행
        ↓
동일 trace_id의 Consumer 수신 건수 확인
        ↓
Spark 검증·중복 제거·거래 조건 적용·1분봉 집계
        ↓
PostgreSQL market_bars Upsert
        ↓
저장 행과 실행 요약 검증
```

## DAG 입력과 식별 방식

| 입력 | 의미 | 검증 |
| --- | --- | --- |
| `ticker` | 처리할 미국 주식 또는 ETF 심볼 | 영문 대문자·숫자·점·하이픈만 허용 |
| `start` | 조회 시작 UTC 시각 | ISO 8601 형식 |
| `end` | 조회 종료 UTC 시각 | ISO 8601 형식, `start` 이후 |
| `feed` | Alpaca feed | `sip` 또는 `iex` |

Kafka 토픽은 종목마다 새로 만들지 않고 기존 공통 토픽 `raw.market-sip.v1`을 사용한다. 메시지의 `S` 필드로 종목을 구분하고 Kafka key도 ticker로 설정한다. 각 DAG 실행에는 고유한 `trace_id`를 부여해 같은 토픽에 여러 실행 데이터가 있어도 해당 실행의 데이터만 검증하고 Spark로 처리한다.

## Airflow 작업 구성

1. `validate_run_config`
   - Airflow 입력값을 검증한다.
   - DAG 실행 ID와 ticker를 이용해 실행별 `trace_id`를 만든다.
2. `replay_trades_to_kafka`
   - 기존 historical replay 코드를 실행한다.
   - 수집·발행 건수와 trace ID를 작은 JSON 요약으로 반환한다.
3. `verify_kafka_delivery`
   - 같은 trace ID의 메시지만 읽는다.
   - 수신 건수가 발행 건수와 다르면 작업을 실패시킨다.
4. `build_minute_bars_with_spark`
   - 기존 Spark SIP batch를 실행한다.
   - 검증, 중복 제거, 거래 조건 적용, 1분봉 집계와 PostgreSQL Upsert를 수행한다.
5. `verify_stored_result`
   - 대상 ticker, feed, 시간 범위의 `market_bars`를 조회한다.
   - Spark 결과와 DB 저장 결과가 일치하지 않으면 실패시킨다.

작업 간에는 원시 거래를 XCom으로 전달하지 않는다. XCom에는 trace ID와 건수 같은 작은 실행 요약만 전달하고, 실제 데이터는 Kafka와 PostgreSQL을 통해 이동한다.

## 재실행과 중복 방지

- 같은 입력으로 DAG를 다시 실행해도 실행별 trace ID가 달라 Kafka 검증 범위가 섞이지 않는다.
- PostgreSQL 저장은 기존 고유 기준인 `(symbol, bar_start, timeframe, source, feed)`를 사용해 Upsert한다.
- 따라서 같은 종목과 구간을 다시 처리해도 `market_bars`에 중복 행이 추가되지 않는다.
- Airflow 작업은 일시적 실패에 대해 1회 재시도한다. 재시도 후에도 발행·수신·저장 건수가 맞지 않으면 성공으로 처리하지 않는다.

## 두 번째 종목 검증

과제 제출 증거는 같은 짧은 시간 범위로 다음 두 번을 실행한다.

1. `ticker=NVDA`, `start=2026-08-12T12:25:00Z`, `end=2026-08-12T12:35:00Z`, `feed=sip`
2. `ticker=SPY`, `start=2026-08-12T12:25:00Z`, `end=2026-08-12T12:35:00Z`, `feed=sip`

두 번째 실행은 DAG 코드를 바꾸지 않고 Airflow 입력값의 ticker만 `SPY`로 변경한다. 데이터량이 로컬 실행 한도를 넘는 경우 두 실행의 시간 범위를 동일하게 더 줄이되, DAG와 처리 코드는 변경하지 않는다.

## 제출 증거

- DAG 코드
- NVDA 실행의 Airflow 작업 성공 화면 또는 로그 요약
- SPY 실행의 Airflow 작업 성공 화면 또는 로그 요약
- 두 실행의 입력값과 단계별 건수를 정리한 작은 JSON 또는 Markdown 결과
- PostgreSQL에 저장된 ticker별 1분봉 수 확인 결과
- README의 실행 명령, 저장 위치·형식, 실제 구현과 다음 계획 구분

로그와 문서에는 API key와 DB 비밀번호를 출력하지 않는다. 대용량 원시 체결 파일, Airflow 로컬 DB·로그 전체와 `.env`는 Git에 올리지 않는다.

## 완료 기준

- DAG가 import 오류 없이 로드된다.
- NVDA 실행에서 모든 작업이 성공하고 Kafka 발행·수신 건수가 일치한다.
- ticker만 바꾼 SPY 실행에서도 모든 작업이 성공한다.
- Spark가 각 실행의 trace ID 데이터만 처리하고 PostgreSQL 저장 결과를 검증한다.
- 테스트와 제출 문서가 실제 실행 결과만 설명하며 계획 중인 기능을 구현 완료로 표현하지 않는다.
