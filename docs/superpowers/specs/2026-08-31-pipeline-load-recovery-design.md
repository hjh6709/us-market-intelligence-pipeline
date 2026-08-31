# 데이터 파이프라인 부하·장애·복구 실험 설계

## 목표

기존 `Alpaca SIP → Kafka → Spark → PostgreSQL` 경로를 실제 경제지표 발표 데이터와 연결한 상태에서 정상 입력과 더 큰 입력을 비교하고, 안전하게 장애를 재현한 뒤 재실행해 데이터 누락과 중복이 없음을 증명한다. 결과는 5차시 과제 문서와 약 4분 발표에서 코드 설명보다 데이터 범위, 처리 흐름, 측정값과 복구 결과를 중심으로 설명할 수 있어야 한다.

## 과제 요구사항과 제출 증거

| 과제 요구사항 | 구현 및 증거 |
| --- | --- |
| 현재 입력량과 정상 결과 | 기존 2026-08-12 CPI 1회, `SPY·QQQ·SMH·NVDA`, SIP 원시 체결 118,118건을 기준 실행으로 기록한다. 실행 시간, 단계별 건수와 PostgreSQL 1분봉 472건을 같은 실행 ID로 남긴다. |
| 더 많은 데이터 처리 | 2022-01-01 이상 2026-08-12 이하 CPI 발표 구간과 같은 4종목의 실제 SIP 체결을 저장한 뒤 Kafka로 재생한다. 최소 성공 조건은 기준 입력의 10배인 1,181,180건 이상이다. |
| 장애 안전 재현 | 로컬 모의 API 503, 잘못된 입력, Spark 작업 중단, PostgreSQL 중단과 동일 데이터 중복 실행을 외부 서비스에 부하를 주지 않는 방식으로 재현한다. |
| 복구 및 무결성 확인 | 동일 실행 명세와 Kafka offset 범위로 재실행해 발행·수신·Spark 입력 건수, 최종 저장 건수, 누락과 고유키 중복을 확인한다. |

공개 저장소에는 실행 코드, 집계 결과 JSON, 비밀정보가 제거된 짧은 로그, SQL 검증 결과와 화면 캡처만 포함한다. API key, DB 비밀번호, Airflow 메타데이터 DB, 전체 원시 payload와 대용량 Parquet은 포함하지 않는다.

## 접근법 결정

한 CPI 구간을 단순 복제하면 처리량 실험은 가능하지만 프로젝트의 2022~2026 경제지표 분석 범위를 검증하지 못한다. 반대로 2022~2026년 모든 시간대의 원시 체결을 수집하면 과제에 필요하지 않은 외부 호출, 저장 비용과 처리 시간이 커진다.

따라서 경제지표 발표 구간만 원시 체결로 보존한다. CPI 발표 전 60분부터 발표 후 60분과 정규장 첫 1분을 포함하는 반개구간 `[released_at - 60분, released_at + 61분)`을 사용한다. 4종목과 모든 대상 CPI 발표일을 독립 파티션으로 저장하므로 실패한 파티션만 다시 수집하거나 재생할 수 있다.

## 데이터 범위와 출처

### 경제 이벤트

- 기간: 2022-01-01 이상 2026-08-12 이하의 실제 CPI 발표일
- 정확한 발표 날짜·시각과 대상 월: BLS 공식 발표 일정과 보관 페이지
- 이벤트 식별자: `CPI|<reference_period>|<released_at_utc>`
- 취소되었거나 실제 발표가 확인되지 않은 일정은 이벤트로 만들지 않는다.

### FRED·ALFRED 경제지표

FRED API key를 사용하되 발표 당시 알 수 없었던 미래 수정값이 섞이지 않도록 ALFRED realtime/vintage 필드를 보존한다.

| 구분 | series ID | 용도 |
| --- | --- | --- |
| 물가 | `CPIAUCSL`, `CPILFESL` | CPI·근원 CPI 발표값 |
| 물가 환경 | `PCEPI`, `PCEPILFE` | PCE·근원 PCE의 당시 최신 공개값 |
| 고용 | `UNRATE`, `PAYEMS` | 실업률·비농업 고용의 당시 최신 공개값 |
| 정책·시장 금리 | `DFF`, `DGS2`, `DGS10` | 연방기금금리와 2년·10년 국채금리 환경 |
| 시장 불안 | `VIXCLS` | 발표 직전 이용 가능했던 VIX 종가 |

각 CPI 이벤트에는 해당 이벤트 시각 이전에 공개된 각 series의 최신 유효 관측값만 연결한다. FRED observation의 `date`를 실제 발표 시각으로 해석하지 않는다. CPI의 정확한 `released_at`은 BLS에서 가져온다. 전망치, 뉴스 감정과 옵션 Put/Call은 FRED가 제공하는 데이터가 아니므로 이번 실험에 포함하지 않는다.

### 시장 데이터

- 출처: Alpaca Historical Trades API
- feed: `sip`
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 레코드 단위: 개별 체결
- 시간 범위: 각 CPI 발표의 `[발표 60분 전, 발표 61분 후)`
- 보관 단위: `event_type=CPI/release_date=YYYY-MM-DD/symbol=SYMBOL`

원본 수집은 pagination이 끝까지 완료된 파티션만 성공으로 표시한다. API 수집은 속도 제한과 재시도를 적용해 한 번 수행하고, 부하 실험은 저장된 파일만 반복 재생한다.

## 실행 아키텍처

```text
BLS 발표 일정 ─┐
FRED/ALFRED ───┼─> 경제 이벤트·당시 macro context ───────────────┐
Alpaca SIP ────┘                                                 │
      │                                                          │
      └─> event/symbol별 원본 Parquet manifest                   │
                    │                                            │
                    └─> Kafka raw.market-sip.v1                  │
                              │                                  │
                              └─> Spark 검증·중복 제거·1분봉 집계
                                             │                   │
                                             └─> PostgreSQL ─────┘
                                                     │
                                                     └─> 무결성·성능 결과 JSON
```

Airflow는 수집과 실험 실행 순서를 관리한다. XCom에는 원시 데이터가 아니라 dataset ID, 파티션 경로, offset 범위와 집계 수치만 전달한다.

1. 실행 입력과 기간을 검증한다.
2. BLS 이벤트와 FRED/ALFRED 데이터를 수집·Upsert한다.
3. 없는 시장 데이터 파티션만 Alpaca에서 수집해 Parquet과 manifest를 만든다.
4. 선택한 manifest의 원시 체결을 Kafka에 재생한다.
5. Kafka 발행·수신 건수를 확인한다.
6. Spark가 동일 offset 범위를 처리해 1분봉을 Upsert한다.
7. 경제 이벤트와 당시 macro context를 연결한다.
8. 단계별 건수, 실행 시간, 오류와 DB 무결성을 결과 JSON으로 저장한다.

## 저장 계약

### 원본 manifest

각 파티션 manifest에는 다음을 기록한다.

- `dataset_id`, `economic_event_id`, `release_date`, `symbol`, `feed`
- 조회 시작·종료 UTC
- API page 수와 원시 체결 건수
- 파일 경로, 파일 크기와 SHA-256
- 수집 시작·종료 시각과 완료 상태

완료 manifest와 파일 hash가 일치하는 파티션은 외부 API를 다시 호출하지 않는다.

### PostgreSQL

- `macro_series`: FRED series 명세
- `macro_observations`: 값과 realtime/vintage 기간
- `economic_events`: 공식 발표 시각과 대상 월
- `market_bars`: Spark가 만든 SIP 1분봉
- `macro_event_impacts`: 이벤트별 발표 전후 반응
- 신규 `pipeline_experiment_runs`: 환경, dataset ID, 실행 단계, 시간과 단계별 건수
- 신규 `pipeline_experiment_failures`: 주입한 장애, 관측한 오류, 복구 실행 ID와 복구 결과

`market_bars`는 기존 `(symbol, bar_start, timeframe, source, feed)` 고유 기준과 Upsert를 유지한다. 실험 결과 테이블은 `experiment_run_id`로 실행을 구분한다.

## 기준 및 부하 실험

### 기준 실행

- dataset: `cpi-2026-08-12-four-symbols`
- CPI 이벤트: 1회
- 종목: 4개
- 원시 체결: 118,118건
- 기존 기대 결과: 1분봉 472건

새 측정에서는 시작·종료 monotonic time으로 전체 실행 시간을 다시 기록한다. 과거 문서의 건수는 기준 계약으로 사용하지만 새 실행의 시간을 과거 결과에서 추정하지 않는다.

### 부하 실행

- dataset: `cpi-2022-01-01_2026-08-12-four-symbols`
- 종목: 동일한 4개
- 입력: 해당 기간 실제 CPI 발표 구간의 저장된 SIP 체결
- 최소 입력: 1,181,180건
- Kafka 재생률: 무제한 발행 1회와 제어된 발행률 실행을 분리한다.
- Spark: 같은 schema, validation, provider 거래 조건과 1분봉 집계를 사용한다.

기준 실행과 부하 실행은 같은 GCP VM, Kafka partition 수, Spark 설정과 PostgreSQL 설정에서 수행한다. 비교표에는 원시 입력, Kafka 발행·수신, Spark 입력·유효·중복·오류, 생성·저장 1분봉, 전체 초, events/s와 오류를 기록한다.

## 장애와 복구 실험

### 1. API 503

실제 Alpaca나 FRED에 실패 요청을 반복하지 않고, 테스트용 로컬 HTTP 응답이 처음 한 번 `503`을 반환한 뒤 정상 manifest 응답을 반환하게 한다. collector가 제한된 횟수와 backoff로 재시도하고 API key를 로그에 출력하지 않는지 검증한다.

### 2. 잘못된 입력

`start >= end`, 빈 종목 목록과 허용되지 않은 feed를 각각 입력한다. Airflow validation 단계가 외부 API나 Kafka 호출 전에 실패해야 하며 PostgreSQL 행 수는 변하지 않아야 한다.

### 3. Spark 작업 중단

Kafka 발행과 offset 범위 기록이 끝난 뒤 Spark 프로세스를 종료한다. 실패 실행에서는 DB 결과를 성공으로 기록하지 않는다. 동일 manifest와 offset 범위로 Spark부터 재실행하고 발행·수신·Spark 입력 건수와 최종 저장 결과가 일치하는지 확인한다.

### 4. PostgreSQL 중단

전용 GCP 실험 VM에서 Spark 처리 전 PostgreSQL 컨테이너만 중지해 적재 실패를 기록한다. PostgreSQL을 다시 시작해 health check가 성공한 후 같은 처리 결과를 Upsert한다. 재실행 전후 `market_bars` 고유키 중복 수는 0이어야 한다.

### 5. 동일 데이터 중복 실행

같은 manifest를 두 번 재생한다. Kafka에는 두 실행의 메시지가 각각 존재할 수 있지만 Spark의 거래 식별 중복 검사와 PostgreSQL Upsert 후 최종 business row 수와 값은 첫 성공 실행과 같아야 한다.

## 측정 및 완료 기준

각 실행은 다음 항목을 하나의 machine-readable JSON으로 남긴다.

- 실행 ID, dataset ID, 환경과 장애 유형
- 경제 이벤트 수, 종목 수와 데이터 파티션 수
- 원시 입력, Kafka 발행·수신, Spark 입력·유효·중복·오류 건수
- Spark 출력과 PostgreSQL 저장 1분봉 수
- DB 저장 전·후 행 수와 고유키 중복 수
- 단계별 시작·종료 시각, 소요 초와 처리량
- 성공·실패 상태와 비밀정보가 제거된 오류 분류
- 복구 대상 실행 ID와 복구 후 무결성 결과

완료 조건은 다음과 같다.

1. 기준과 부하 실행의 Kafka 발행·수신·Spark 입력 건수가 일치한다.
2. 부하 입력은 1,181,180건 이상이며 기준과 같은 조건에서 실행 시간을 측정한다.
3. validation 오류와 허용되지 않은 중복은 0건이다.
4. 각 장애는 예상 단계에서 실패하며 성공으로 잘못 기록되지 않는다.
5. 복구 실행 후 누락된 기대 1분봉과 PostgreSQL 고유키 중복이 0건이다.
6. 동일 데이터 재실행 전후 최종 business row 수와 집계값이 같다.
7. 공개 증거에 API key, 비밀번호, DSN과 원시 시장 가격 payload가 없다.
8. 문서는 실제 실행과 이후 계획을 구분하며 측정하지 않은 값을 결과로 표현하지 않는다.

## GCP 실험 환경

- 계정: 현재 인증된 개인 GCP 계정
- 프로젝트: 접근 가능한 `project-6ebdf72b-a53c-4925-8d2`로 명시 설정
- 리전: `us-central1`
- VM: `e2-standard-4`, 4 vCPU, 16GB memory
- disk: 100GB Standard Persistent Disk
- 실행 구성: Docker Compose의 Kafka·PostgreSQL, Spark local mode, Airflow
- 공개 포트: SSH 이외에는 열지 않는다.

VM에는 최소 권한 서비스 계정만 사용한다. API key와 DB 접속값은 Git에 저장하지 않고 VM 환경 파일에만 둔다. 실험 완료 후 집계 증거를 로컬 저장소에 옮기고 VM을 삭제해 비용을 중단한다.

## 제출 문서와 발표 구성

`docs/load-recovery-assignment.md`는 아래 순서를 유지한다.

1. 이번 과제에서 무엇을 검증했는가
2. 데이터 범위와 2022~2026의 의미
3. 정상 기준 결과
4. 더 큰 데이터 결과와 비교
5. 장애별 실패·복구 결과
6. 누락·중복 검증
7. 실행 방법과 저장 위치
8. 실제 구현과 다음 단계

발표 대본은 약 4분 분량으로 작성한다. 코드 전체를 읽지 않고 아키텍처, 기준·부하 비교표, 대표 장애 하나의 실패·복구 증거와 최종 무결성 표만 보여준다. 나머지 장애 결과는 질문이 나올 때 열어볼 수 있게 문서에 남긴다.

## 제외 범위

- 외부 Alpaca·FRED API 자체에 대한 부하 테스트
- k6·Artillery·Locust 사용을 위해 새로운 HTTP API 생성
- 전체 미국 주식과 2022~2026 모든 시간대의 raw trade 수집
- 전망치, 뉴스 감정, 옵션 Put/Call과 자동매매 주문
- 다중 노드 Kafka·Spark cluster의 최대 처리량 산정
- 측정하지 않은 수익률이나 경제지표의 인과 효과 주장
