# 5차시 과제 — 데이터 파이프라인 부하·장애·복구 실험

## 한눈에 보는 결과

2026년 8월 12일 CPI 한 번의 4종목 SIP 체결 **118,118건**을 기준으로 삼고, 2022년부터 2026년 8월까지 실제 CPI 발표 55회로 범위를 넓혀 **7,360,804건**을 GCP에서 처리했습니다.

```text
저장된 실제 SIP 체결
  → Kafka 발행·수신 건수 확인
  → Spark 형식 검사·중복 제거·1분봉 집계
  → PostgreSQL Upsert
  → 누락·중복 확인
```

| 항목 | 기준 실행 | 부하 실행 |
| --- | ---: | ---: |
| CPI 발표 | 1회 | 55회 |
| 종목 | 4개 | 4개 |
| 원시 SIP 체결 | 118,118건 | 7,360,804건 |
| 입력 증가 | 1배 | 62.3배 |
| Kafka 발행 / 수신 | 118,118 / 118,118 | 7,360,804 / 7,360,804 |
| Spark 입력 | 118,118 | 7,360,804 |
| 형식 오류 | 0 | 0 |
| 원본 event ID 중복 | 0 | 49 |
| PostgreSQL 1분봉 | 472 | 22,260 |
| DB business key 중복 | 0 | 0 |
| GCP 실행 시간 | 76.5초 | 1,690.3초 |

기준 실행은 GCP 최초 Spark 실행이라 Kafka connector 다운로드 시간이 포함됐습니다. 따라서 두 시간을 입력량에 정비례하는 순수 성능 비교로 해석하지 않습니다.

## 1. 어떤 데이터를 사용했는가

### 시장 데이터

- 출처: Alpaca Historical Trades API
- feed: `sip`
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 이벤트: BLS에서 실제 발표를 확인한 CPI 55회
- 기간: 2022-01-12 발표부터 2026-08-12 발표까지
- 각 구간: CPI 발표 60분 전 이상, 발표 61분 후 미만
- 한 행: 주식 한 종목의 개별 체결 한 건

외부 Alpaca API에 736만 건의 부하를 반복해서 보낸 것이 아닙니다. API는 한 번만 호출해 220개 Parquet 파티션으로 저장했고, 기준·부하·복구 실험은 이 로컬 원본을 Kafka에 다시 넣었습니다.

| 종목 | 발표 파티션 | 원시 체결 | 최종 1분봉 |
| --- | ---: | ---: | ---: |
| SPY | 55 | 1,731,921 | 6,488 |
| QQQ | 55 | 1,685,204 | 6,536 |
| SMH | 55 | 173,513 | 2,891 |
| NVDA | 55 | 3,770,166 | 6,345 |
| 합계 | 220 | 7,360,804 | 22,260 |

### 경제지표 데이터

FRED API key를 사용해 각 CPI 발표일 당시 이용할 수 있었던 최근 값을 ALFRED의 realtime 범위와 함께 저장했습니다.

| 구분 | series ID |
| --- | --- |
| 물가 | `CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE` |
| 고용 | `UNRATE`, `PAYEMS` |
| 금리 | `DFF`, `DGS2`, `DGS10` |
| 시장 불안 | `VIXCLS` |

10개 지표 × CPI 55회 = `macro_event_contexts` 550행입니다. 발표일보다 미래인 관측일 또는 realtime 시작일을 연결한 건수는 0건입니다. `fdnpy`는 이번 구현에 사용하지 않았습니다.

## 2. 실행 환경

| 항목 | 값 |
| --- | --- |
| GCP zone | `us-central1-a` |
| VM | `e2-standard-4` |
| CPU / memory | 4 vCPU / 16GB |
| disk | 100GB `pd-standard` |
| 실행 구성 | Docker Compose Kafka·PostgreSQL, Spark local mode |
| Spark driver heap | 6GB |

API key와 `.env`는 VM으로 복사하지 않았습니다. GCP에서는 이미 수집한 Parquet만 처리했습니다. 결과 JSON을 내려받은 뒤 VM과 부팅 디스크를 삭제해 추가 비용을 중단했습니다.

## 3. 정상 기준 실행

기준은 이전 Airflow 과제와 같은 `2026-08-12 CPI × SPY·QQQ·SMH·NVDA`입니다.

| 단계 | 결과 |
| --- | ---: |
| 원시 입력 | 118,118 |
| Kafka 발행 | 118,118 |
| Kafka 수신 | 118,118 |
| Spark 입력 | 118,118 |
| invalid / duplicate | 0 / 0 |
| 생성·Upsert 1분봉 | 472 |
| DB key 중복 | 0 |
| 전체 시간 | 76.480초 |

실행 ID는 `gcp-baseline-20260831`입니다.

## 4. 더 많은 데이터 실행

같은 처리 규칙으로 55개 CPI 발표와 4종목 전체를 실행했습니다.

| 단계 | 결과 |
| --- | ---: |
| 원시 입력 | 7,360,804 |
| Kafka 발행 | 7,360,804 |
| Kafka 수신 | 7,360,804 |
| Spark 입력 | 7,360,804 |
| 형식 오류 | 0 |
| 원본 event ID 중복 탐지·제거 | 49 |
| 생성·Upsert 1분봉 | 22,260 |
| DB key 중복 | 0 |
| 전체 시간 | 1,690.250초 |
| 전체 처리량 | 4,354.861 events/s |

49건은 Kafka나 재실행 때문에 생긴 중복이 아닙니다. 선택한 220개 원본 파티션 안에서 `event_id`가 같은 체결을 Spark가 발견한 수입니다. 1분봉을 만들기 전에 한 번만 남겼고 PostgreSQL 고유키 중복은 0건입니다.

### 실제로 발견한 병목

처음 로컬 전체 실행에서는 Spark의 중간 DataFrame을 메모리에 캐시하다 `Java heap space`로 실패했습니다. 중간 결과를 `DISK_ONLY`로 보관하고 driver heap을 6GB로 명시한 뒤 같은 7,360,804건을 재실행해 성공했습니다.

GCP의 기준·부하 실행 누적 offset을 확인하니 파티션별 메시지는 `0 / 7,294,235 / 184,687건`이었습니다. 종목코드를 Kafka key로 사용한 결과 3개 파티션 중 한 곳에 약 97.5%가 몰린 것입니다. 종목 내부 순서는 지켰지만 병렬 처리 효율은 낮았습니다. 다음 실험에서는 공통 토픽을 유지하면서 `symbol + release_date` 복합 key와 파티션 수를 함께 비교해야 합니다.

## 5. 장애와 복구

### PostgreSQL 적재 실패

GCP의 PostgreSQL 컨테이너만 중지한 뒤 기준 데이터를 다시 실행했습니다.

| 단계 | 상태 | Kafka 발행 / 수신 | PostgreSQL 저장 | 전체 business row | key 중복 |
| --- | --- | ---: | ---: | ---: | ---: |
| 장애 전 | 성공 | 7,360,804 / 7,360,804 | 22,260 | 22,260 | 0 |
| DB 중지 후 실행 | `failed` | 118,118 / 118,118 | 0 | 22,260 | 0 |
| DB 재시작 후 동일 입력 | `succeeded` | 118,118 / 118,118 | 472 Upsert | 22,260 | 0 |

실패 실행은 `OperationalError`로 기록됐으며 성공으로 잘못 표시되지 않았습니다. DB를 다시 시작하고 health check가 통과한 뒤 같은 입력을 실행했습니다. 기존 472개 key를 Upsert했으므로 최종 행은 22,260개에서 늘지 않았고 중복도 0건이었습니다.

### 안전하게 확인한 다른 장애

| 장애 | 재현 방식 | 결과 |
| --- | --- | --- |
| API 503 | 외부 API가 아닌 mock 응답 | 실패 탐지 후 완성 Parquet과 checksum으로 복구 |
| 잘못된 기간 | `start >= end` 입력 | 외부 호출·파일 생성 전에 `ValueError`, 부수 효과 0 |
| DB endpoint 오류 | 로컬의 사용하지 않는 port 1 | `OperationalError` 후 정상 endpoint health query 통과 |

외부 Alpaca·FRED 서비스에는 부하나 고의 실패 요청을 보내지 않았습니다.

## 6. 누락과 중복을 어떻게 확인했는가

1. 아카이브 manifest의 원시 행 수를 Kafka 기대 건수로 정합니다.
2. Producer가 받은 partition별 시작·끝 offset을 기록합니다.
3. Consumer와 Spark는 그 offset 범위만 읽습니다.
4. `원시 입력 = Kafka 발행 = Kafka 수신 = Spark 입력`인지 확인합니다.
5. Spark는 `event_id`로 원본 중복을 제거합니다.
6. PostgreSQL은 `(symbol, bar_start, timeframe, source, feed)` 고유키로 Upsert합니다.
7. 재실행 전후 전체 행 수와 `GROUP BY ... HAVING COUNT(*) > 1` 결과를 비교합니다.

최종 결과는 시장 1분봉 22,260행, DB business key 중복 0건, macro point-in-time 위반 0건입니다.

## 7. 재현 명령

```bash
# 로컬 서비스
docker compose up -d --wait postgres kafka kafka-init

# 이미 수집된 아카이브에서 기준 실행
.venv/bin/python scripts/run_pipeline_experiment.py \
  --release-from 2026-08-12 \
  --release-to 2026-08-12 \
  --environment local \
  --output data/local/experiment-results/local-baseline.json

# 전체 부하 실행
SPARK_DRIVER_MEMORY=6g \
.venv/bin/python scripts/run_pipeline_experiment.py \
  --release-from 2022-01-01 \
  --release-to 2026-08-12 \
  --environment local \
  --output data/local/experiment-results/local-load.json

# 외부 서비스에 부하를 주지 않는 안전 장애 확인
.venv/bin/python scripts/run_safe_fault_checks.py \
  --output data/local/experiment-results/local-safe-faults.json
```

Parquet 원본과 전체 로그는 Git에서 제외합니다. 공개 증거는 [`docs/evidence/load-recovery`](evidence/load-recovery/README.md)에 있습니다.

## 8. 과제 요구사항 확인

| 필수 내용 | 제출 위치 | 완료 |
| --- | --- | --- |
| 현재 입력량·시간·처리·저장 건수 | 이 문서의 기준 실행 | 완료 |
| 더 많은 입력과 시간·건수·오류 | 이 문서의 부하 실행 | 완료 |
| 실제 가능한 장애 안전 재현 | PostgreSQL 중단, mock 503, 잘못된 입력 | 완료 |
| 복구 후 누락·중복 확인 | 장애·복구 표와 무결성 파일 | 완료 |
| 실행 화면 또는 기록 | 캡처 3장과 JSON·텍스트 | 완료 |
| 대용량 원본·키 제외 | Parquet·`.env` Git ignore | 완료 |

## 9. 다음 단계

- Kafka key를 `symbol + release_date`로 바꾼 실험과 파티션 분포 비교
- `pd-standard`와 SSD에서 Spark spill·처리 시간 비교
- Airflow에서 파티션 단위 재실행과 실패 알림 연결
- 55회 CPI 시장 반응 분석 테이블을 다시 계산해 자동매매 백테스트 입력으로 제공

현재 완료한 것은 데이터 파이프라인의 수집 원본 보존, 대량 재생, 전처리, 저장과 장애 복구 검증입니다. 경제지표가 수익률에 미친 인과효과나 자동매매 수익률은 이번 결과로 단정하지 않습니다.
