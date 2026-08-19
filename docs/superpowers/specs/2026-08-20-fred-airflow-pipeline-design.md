# FRED/ALFRED Airflow Pipeline Design

## 1. 목적

이 단계는 경제지표 영향 분석의 첫 배치 수직 슬라이스다. FRED/ALFRED의 9개 series metadata와 observation/vintage를 수집·검증해 PostgreSQL에 멱등 저장하고, Airflow가 logical date, 재시도, backfill과 품질 검사를 관리한다.

이번 단계는 시장 반응을 계산하지 않는다. 공식 BLS·BEA·Federal Reserve 발표 일정, historical SIP window, `economic_events`, `macro_event_impacts`는 다음 단계에서 현재 저장 결과에 연결한다.

## 2. 완료 기준

- 9개 series가 동일한 계약으로 수집된다: `CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE`, `UNRATE`, `DFF`, `DGS2`, `DGS10`, `VIXCLS`.
- FRED 응답의 `realtime_start`, `realtime_end`, observation date와 결측값을 손실 없이 정규화한다.
- `(series_id, observation_date, realtime_start)`가 같은 observation은 재실행해도 행이 증가하지 않는다.
- 같은 Airflow logical date를 두 번 실행한 결과의 business row 수가 같다.
- HTTP 429, timeout, 잘못된 값, `.` 결측값을 fixture 또는 task 테스트로 재현한다.
- fixture 기반 검증과 API key가 필요한 실제 FRED smoke test를 분리한다.
- Airflow Graph/Grid, task log, SQL 결과와 고정 JSON 결과를 과제 증거로 남길 수 있다.

## 3. 범위

### 포함

- FRED series metadata와 observations API client
- FRED/ALFRED real-time period와 vintage field 보존
- 9개 series registry
- `macro_series`, `macro_observations` migration
- normalization, validation, transaction upsert와 quality query
- Airflow 3.3.0 daily DAG와 수동 backfill parameter
- Docker Compose `batch` profile
- fixture, 단위 테스트, PostgreSQL 통합 테스트, optional live smoke test
- 입력·정규·결측·저장·재실행 수를 담은 evidence JSON과 SQL

### 제외

- 공식 발표 시각 수집과 `economic_events`
- historical SIP 수집과 IEX/SIP reconciliation
- macro event impact, forecast, surprise와 매매 신호
- Airflow 고가용성, Celery/Kubernetes executor, 운영용 metadata database
- 9개 series 외의 임의 series 검색 UI

## 4. 설계 선택

### 4.1 core logic과 Airflow 분리

FRED HTTP, normalization, PostgreSQL upsert는 일반 Python module로 구현한다. Airflow DAG는 이 함수를 호출하고 logical date, mapping, retry와 실행 순서만 관리한다. 따라서 Airflow가 없어도 fixture와 PostgreSQL 통합 테스트로 데이터 계약을 검증할 수 있다.

파일 책임은 다음과 같다.

```text
src/fred.py
  endpoint request, response parsing, typed transport errors

src/macro_models.py
  immutable series/observation value objects and validation

src/macro_repository.py
  PostgreSQL transaction upsert and quality queries

src/fred_pipeline.py
  one-series extract → normalize → upsert orchestration

dags/fred_macro_dag.py
  Airflow schedule, logical window, dynamic task mapping, quality gate
```

### 4.2 Airflow 실행 환경

Airflow는 `apache-airflow==3.3.0`으로 고정하고 애플리케이션 venv와 분리된 Docker image로 실행한다. Compose의 `batch` profile을 선택했을 때만 켜며 UI port는 `127.0.0.1`에만 bind한다.

로컬 과정 실습은 Airflow standalone과 전용 volume을 사용한다. 이는 교육·개발 환경이지 HA 운영 구성이 아니다. OCI 1 OCPU·6GB node에서는 Kafka/Spark 실시간 profile과 Airflow batch profile을 동시에 상시 실행하지 않고 순차적으로 실행한다. 운영 전환 시 Airflow metadata database와 executor 선택은 별도 ADR로 결정한다.

### 4.3 dynamic task mapping

9개 series는 한 series의 429나 schema 오류가 다른 series의 성공 이력과 섞이지 않도록 series별 mapped task로 실행한다. mapped task는 observation 전체를 XCom으로 반환하지 않는다. PostgreSQL에 직접 transaction upsert하고 다음 작은 summary만 반환한다.

```json
{
  "series_id": "DGS10",
  "raw_count": 7,
  "normalized_count": 7,
  "missing_count": 1,
  "upserted_count": 7
}
```

reduce quality task는 summary와 PostgreSQL을 함께 확인한다.

## 5. 수집 범위와 point-in-time 규칙

Daily DAG는 `0 14 * * *` UTC에 실행한다. logical date `D`에 대해 real-time period `[D-6일, D]`를 겹쳐 조회한다. 늦은 갱신과 revision을 다시 받더라도 unique key upsert가 중복을 막는다.

초기 historical backfill은 수동 parameter로 `realtime_start`, `realtime_end`, `observation_start`, `observation_end`를 명시한다. 최근 24개월 전체 vintage를 한 번에 무조건 호출하지 않고 series별 API 응답 크기를 기록하며 범위를 나눈다. daily window와 historical backfill은 같은 normalization과 repository 함수를 사용한다.

Observations 요청의 기본 parameter는 다음과 같다.

```text
file_type=json
series_id=<mapped series>
realtime_start=<logical D-6 or manual start>
realtime_end=<logical D or manual end>
observation_start=<configured analysis start>
observation_end=<logical D or manual end>
output_type=1
```

`observation_date`는 관측 대상일이며 공식 발표 timestamp가 아니다. 이를 `released_at`으로 변환하지 않는다. FRED의 real-time period는 정보가 알려져 있던 기간이며 원문 값을 보존한다. 공식 발표 시각은 후속 `economic_events` 단계에서 BLS·BEA·Federal Reserve 출처로 별도 결합한다.

## 6. 데이터 모델

### 6.1 `macro_series`

```sql
series_id TEXT PRIMARY KEY
title TEXT NOT NULL
frequency TEXT NOT NULL
units TEXT NOT NULL
seasonal_adjustment TEXT NOT NULL
observation_start DATE NOT NULL
observation_end DATE NOT NULL
last_updated TIMESTAMPTZ NOT NULL
notes TEXT
source TEXT NOT NULL CHECK (source = 'fred')
ingested_at TIMESTAMPTZ NOT NULL
```

Metadata는 `series_id`로 upsert한다. 변경된 title, 단위와 최근 갱신 시각을 최신 응답으로 갱신한다.

### 6.2 `macro_observations`

```sql
series_id TEXT NOT NULL REFERENCES macro_series(series_id)
observation_date DATE NOT NULL
value NUMERIC
realtime_start DATE NOT NULL
realtime_end DATE NOT NULL
source TEXT NOT NULL CHECK (source = 'fred')
ingested_at TIMESTAMPTZ NOT NULL
PRIMARY KEY (series_id, observation_date, realtime_start)
CHECK (realtime_start <= realtime_end)
```

FRED value `.`은 오류가 아니라 `NULL`로 저장한다. 그 외 value는 `Decimal`로 변환되지 않으면 batch validation 오류다. 동일 business key가 다시 들어오면 `value`, `realtime_end`, `ingested_at`을 갱신한다.

`released_at`은 두 테이블에 넣지 않는다. 정확한 공식 발표 시각을 FRED observation date에서 추정하지 않기 위해서다.

## 7. 실행 흐름

```text
resolve logical/manual window
→ validate FRED_API_KEY and database URL
→ map 9 series
   → fetch metadata
   → fetch observations/vintage window
   → normalize and validate
   → transaction upsert series + observations
   → return small count summary
→ aggregate summaries
→ query PostgreSQL business rows
→ fail quality gate on count mismatch or missing series
→ return quality summary
```

각 series transaction은 metadata와 observations를 함께 처리한다. validation 또는 DB constraint가 실패하면 해당 series transaction 전체를 rollback한다.

## 8. 실패 처리

- HTTP timeout: typed `FredTimeoutError`를 발생시켜 Airflow task 실패로 남긴다.
- HTTP 429: typed `FredRateLimitError`를 발생시킨다. 내부 중첩 retry는 하지 않고 Airflow가 재시도한다.
- 기타 4xx: configuration/contract 오류로 즉시 실패한다.
- 5xx: transport 오류로 task를 실패시켜 Airflow 재시도 대상이 된다.
- invalid JSON 또는 필수 field 누락: contract 오류로 실패하고 DB에 쓰지 않는다.
- `.` value: `NULL`로 정상 저장하고 `missing_count`에 포함한다.
- DB 연결·constraint 실패: transaction rollback 후 task 실패다.

Airflow task 기본값은 `retries=3`, `retry_exponential_backoff=True`, `max_retry_delay=15분`, `execution_timeout=2분`이다. secret과 전체 API URL의 `api_key` query는 log/XCom/evidence에 기록하지 않는다.

## 9. 테스트와 증거

### 단위 테스트

- request parameter가 logical/manual window를 정확히 반영한다.
- metadata와 observation fixture를 value object로 변환한다.
- `.`은 `None`, 정상 숫자는 `Decimal`이 된다.
- 잘못된 날짜, 숫자, real-time 순서는 거부한다.
- 429, timeout, 5xx를 올바른 typed error로 분류한다.
- DAG import, UTC schedule, 9개 mapping, retry 설정을 확인한다.

### PostgreSQL 통합 테스트

- 동일 fixture 2회 upsert 후 business row 수가 같다.
- 같은 key의 수정값은 새 행이 아니라 기존 행을 갱신한다.
- 한 invalid row가 포함되면 해당 series batch 전체가 rollback된다.
- `NULL` value와 index query가 정상 동작한다.

### optional live smoke test

`FRED_API_KEY`가 있을 때 `DGS10` 한 series의 metadata와 제한된 observation window만 조회한다. raw payload와 key는 저장하지 않고 response count, date range, missing count만 출력한다.

### 과제 증거

별도 evidence runner가 고정 fixture와 PostgreSQL 통합 결과를 실행해 `docs/evidence/fred-airflow/result.json` 정본을 만든다. Airflow task는 Git checkout을 수정하지 않는다.

```text
9 configured series
→ raw / normalized / missing count
→ PostgreSQL series / observation rows
→ same logical date second run
→ unchanged business row count
```

발표 시 Airflow Grid의 mapped task 9개, 성공 run, 의도적 429 retry run, PostgreSQL count/duplicate SQL을 캡처한다. 캡처는 자동 테스트·JSON·SQL을 보조하며 단독 증거로 사용하지 않는다.

## 10. 문서와 운영 경계

- `.env.example`에는 빈 `FRED_API_KEY`와 local database URL만 둔다.
- README에는 `batch` profile 실행, DAG trigger, SQL 확인과 optional smoke 명령을 추가한다.
- FRED required notice는 기존 README 문구를 유지한다.
- 이 단계가 끝나도 “경제지표가 시장을 움직였다”는 분석 결과를 주장하지 않는다. 저장된 vintage와 후속 공식 발표 시각·SIP 데이터를 결합한 뒤에만 event study를 수행한다.

## 11. GitHub 공개 저장소 보안 게이트

다음 항목은 생성 위치와 관계없이 Git에 추가하지 않는다.

```text
.env와 FRED_API_KEY
Airflow standalone 생성 파일, metadata DB와 인증 정보
Airflow task log와 scheduler/API server log
PostgreSQL·Airflow Docker volume
DB dump, backup과 restore 임시 파일
FRED 원본 응답·HTTP header·api_key가 포함된 URL
Spark checkpoint, Python cache와 로컬 가상환경
발표 캡처 중 terminal history, 환경변수, connection URL이 보이는 파일
```

`.gitignore`에는 `.env*` 중 `.env.example`만 허용하는 규칙, `airflow/` runtime directory, `logs/`, `*.db`, `*.sqlite*`, `*.dump`, `*.backup`과 로컬 evidence capture directory를 명시한다. Docker named volume은 host repository directory에 bind mount하지 않는다.

커밋 가능한 fixture는 API key·request id·개인 식별정보를 제거한 최소 FRED 응답만 포함한다. fixture의 숫자와 날짜는 공개 경제 데이터 계약 테스트용이며 실제 secret을 포함하지 않는다. `result.json`에는 series별 count, date range, missing count와 business row count만 기록하고 원본 payload·환경변수·전체 connection URL은 기록하지 않는다.

커밋 전 자동·수동 게이트는 다음과 같다.

1. `git status --short`로 의도한 파일만 stage되었는지 확인한다.
2. tracked file에서 `FRED_API_KEY=`, FRED `api_key=` query, 비기본 credential과 private key header를 검색한다.
3. `.env`, runtime log, DB·dump·capture 파일이 tracked되지 않았는지 확인한다.
4. `git diff --cached`를 읽고 fixture와 evidence에 원본 인증 정보가 없는지 확인한다.
5. 실제 key가 발견되면 단순 삭제로 끝내지 않고 key를 폐기·재발급한 뒤 Git history 포함 여부를 확인한다.
