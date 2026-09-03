# 6차시 과제 — 부하·복구 결과 보완 및 전체 흐름 점검

> 이 문서는 실제 실행과 남은 작업을 분리합니다. 실행하지 않은 기능은 완료로 표시하지 않습니다.

## 먼저 보는 결론

현재 파이프라인에는 목적이 다른 두 개의 실제 경로가 있습니다.

```text
원시 체결 검증: Raw trades → Parquet → Kafka → Spark → PostgreSQL
분석용 시장 문맥: Official events → Airflow → Alpaca bars → 1m·3m·5m·1d → PostgreSQL
```

- 원시 경로는 CPI 55회 × 4종목의 실제 SIP 체결 **7,360,804건**을 GCP에서 처리해 1분봉 **22,260행**을 저장했습니다.
- 분석용 경로는 CPI·고용·PCE·FOMC **77회 × 10종목**을 수집했습니다. 실행 범위에서 1분봉 **117,566행**, 3분봉 **43,184행**, 5분봉 **26,883행**, 일봉 고유 **8,740행**을 확인했습니다.
- 이번 보완에서는 분석용 경로를 Airflow의 **경제발표 1건 × 종목 1개** 작업으로 연결했습니다. FOMC 1회 × SPY·TLT를 실제로 재실행해 전후 모두 **588행**, 같은 hash, 중복 0건을 확인했습니다.
- 안전한 모의 503은 `FAILED / OPEN → SUCCEEDED / RESOLVED`, 즉 **OPEN → RESOLVED**로 기록됐습니다.

![최신 전체 데이터 파이프라인](diagrams/pipeline-architecture.png)

## 과제 요구사항 한눈에 보기

| 요구사항 | 실제 결과 | 증거 |
| --- | --- | --- |
| 기준·부하 입력, 시간, 처리·저장 | 118,118건 / 76.480초 / 472행 → 7,360,804건 / 1,690.250초 / 22,260행 | [GCP 증거](evidence/load-recovery/README.md) |
| 실패 단계와 복구 위치 | Spark heap, PostgreSQL 적재, API 503를 재현·복구 | 3·4절 |
| fallback 또는 alert | mock 503 후 DB alert `OPEN → RESOLVED` | [6차시 증거](evidence/sixth-assignment/README.md) |
| 최신 구성도·데이터 모델 | 두 실제 경로와 점선 후속 경계 | 구성도·5절 |
| 단계별 건수·최종 확인법 | Kafka·Spark·DB, Airflow work item, SQL·hash | 1·2·4절 |
| 아직 실행되지 않는 단계 | 전체 770개 Airflow 재실행, 전체 지표 결합·영향 계산·백테스트 | 5절 |

## 1. 정상 입력과 결과

기준은 2026-08-12 CPI 발표 전후 121분의 `SPY`, `QQQ`, `SMH`, `NVDA` 실제 SIP 개별 체결입니다.

| 단계 | 기준 실행 |
| --- | ---: |
| CPI 발표 / 종목 | 1회 / 4개 |
| Parquet 원시 체결 | 118,118 |
| Kafka 발행 / 수신 | 118,118 / 118,118 |
| Spark 입력 | 118,118 |
| 형식 오류 / 원본 중복 | 0 / 0 |
| PostgreSQL 1분봉 | 472 |
| DB business key 중복 | 0 |
| GCP 실행 시간 | 76.480초 |

472행이 이론상 `4 × 121 = 484행`보다 적은 것은 수집 누락이라고 단정할 수 없습니다. 한 분에 Odd Lot처럼 가격을 만들지 않는 조건의 체결만 있으면 원시는 보존하지만 완성된 OHLC·VWAP 1분봉은 만들지 않습니다. 없는 가격을 임의로 채우지 않는 정책입니다.

## 2. 더 큰 입력과 실행 환경

### 원시 체결 부하 실행

동일한 규칙을 2022-01-12부터 2026-08-12까지 CPI 55회로 확대했습니다. 외부 API에 736만 건의 동시 부하를 준 것이 아니라, 미리 수집해 checksum을 확인한 Parquet을 Kafka에 재생했습니다.

| 항목 | 기준 | 부하 |
| --- | ---: | ---: |
| CPI 발표 / 종목 | 1회 / 4개 | 55회 / 4개 |
| 실제 SIP 체결 | 118,118 | 7,360,804 |
| Kafka 발행 / 수신 | 118,118 / 118,118 | 7,360,804 / 7,360,804 |
| Spark 입력 | 118,118 | 7,360,804 |
| 형식 오류 | 0 | 0 |
| 원본 `event_id` 중복 제거 | 0 | 49 |
| PostgreSQL 1분봉 | 472 | 22,260 |
| DB business key 중복 | 0 | 0 |
| 실행 시간 | 76.480초 | 1,690.250초 |

부하 실행은 약 28분 10초, 전체 평균은 초당 약 4,355개 체결입니다. 기준에는 최초 Spark connector 준비 시간이 포함돼 단순 선형 성능비로 해석하지 않습니다.

GCP `e2-standard-4` 한 대에서 4 vCPU, RAM 16GB, 100GB `pd-standard`를 사용했습니다. Spark Driver heap은 RAM 중 6GB이고 나머지는 OS·Docker·Kafka·PostgreSQL이 함께 사용했습니다. `DISK_ONLY`는 반복 사용하는 중간 DataFrame을 RAM 대신 VM 디스크에 두는 설정이며 작업 후 `unpersist()` 대상입니다.

### 경제 이벤트·10종목 분석용 bar

| 데이터 | 실제 범위 | 결과 |
| --- | --- | ---: |
| 공식 발표 catalog | CPI 55 + 고용 8 + PCE 9 + FOMC 5 | 77회 |
| 종목 | SPY·QQQ·IWM·TLT·XLF·SMH·GLD·NVDA·AAPL·JPM | 10종목 |
| Alpaca SIP | 발표 T-60분~T+120분 | 1분봉 117,566 |
| 파생봉 | 실제 존재하는 1분봉만 집계 | 3분봉 43,184 / 5분봉 26,883 |
| 일봉 | 이전 7거래일 + 발표일 + 이후 7거래일 | 일봉 고유 8,740 |

`117,566`은 이 실행에서 선택한 발표-종목 구간의 합입니다. PostgreSQL에는 이전 실험의 다른 구간도 있어 테이블 전체 `alpaca/sip/1m` 행 수와 같지 않습니다. 같은 발표 범위로 조회해야 재현됩니다.

3분봉과 5분봉은 결측을 숨기는 대체물이 아닙니다. 포함된 1분봉 수를 저장해 완전하면 `COMPLETE`, 일부만 있으면 `PARTIAL`로 표시합니다.

## 3. 실패 원인과 탐지

### Spark heap 부족

7,360,804건 처리 중 검증 결과와 거래 조건 결과를 RAM에 `cache()`하면서 `Java heap space`가 발생했습니다. 입력 전체가 6GB라는 뜻이 아니라, 여러 단계가 다시 읽는 중간 DataFrame이 Spark JVM heap을 넘은 것입니다. `DISK_ONLY`로 바꾼 뒤 같은 전체 범위를 GCP에서 끝까지 처리했습니다.

### PostgreSQL 적재 실패

GCP에서 PostgreSQL 컨테이너만 중지했습니다. Kafka는 118,118건을 발행·수신했지만 DB 적재에서 `OperationalError`가 발생했고 실행은 `failed`, 신규 저장은 0건으로 기록됐습니다.

### API 503와 alert

외부 서비스에는 고의 장애를 보내지 않았습니다. 로컬 모의 client의 첫 응답을 503으로 만들고 실패 work item과 `FAIL / OPEN` check를 DB에 기록했습니다. 커밋된 합성 fixture로 재시도한 뒤 `PASS / RESOLVED`로 갱신했습니다.

| 시점 | work | check | alert | 저장 |
| --- | --- | --- | --- | ---: |
| 첫 요청 | FAILED | FAIL | OPEN | 0 |
| 재시도 | SUCCEEDED | PASS | RESOLVED | 1 fixture row |

fixture는 장애 제어만 검증하며 실제 시장 데이터로 사용하지 않습니다. 검증되지 않은 archive를 쓰는 fallback은 미구현이고 현재는 검증 실패 시 닫힌 상태로 실패합니다.

### Airflow 로컬 설정

신규 DAG의 첫 CLI 시도는 `dags_folder`가 `airflow-runtime/dags`를 가리켜 task 실행 전에 실패했습니다. 저장소의 실제 `dags/`를 명시한 뒤 동일 명령을 다시 실행해 성공했습니다. 데이터 장애가 아니라 실행 환경 설정 오류입니다.

## 4. 재실행 위치와 무결성

### GCP DB 장애 복구

DB 컨테이너를 다시 시작하고 health check 후 실패한 기준 입력부터 재실행했습니다. 기존 key에 Upsert해 최종 22,260행과 중복 0건이 유지됐습니다.

| 단계 | 상태 | DB 처리 | 최종 행 | 중복 |
| --- | --- | ---: | ---: | ---: |
| 장애 전 부하 | 성공 | 22,260 | 22,260 | 0 |
| DB 중지 | 실패 | 0 | 22,260 | 0 |
| DB 복구 후 기준 재실행 | 성공 | 472 Upsert | 22,260 | 0 |

### 실제 Airflow event-symbol 실행

`market_context_backfill_pipeline`은 event type, 날짜 범위, symbols, feed, data cutoff을 입력받습니다. bar 배열은 XCom에 넣지 않고 식별자와 건수만 전달합니다.

FOMC 2026-07-29 × SPY·TLT를 실제 Alpaca SIP와 로컬 PostgreSQL로 실행했습니다.

| 종목 | 1m | 3m | 5m | 1d | coverage | 상태 |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| SPY | 181 | 61 | 37 | 15 | COMPLETE | SUCCEEDED |
| TLT | 181 | 61 | 37 | 15 | COMPLETE | SUCCEEDED |
| 합계 | 362 | 122 | 74 | 30 | 2 COMPLETE | 2 SUCCEEDED |

| 검증 | 비교 실행 전 | 동일 입력 재실행 후 |
| --- | ---: | ---: |
| 정확한 결과 범위 | 588행 | 588행 |
| 내용 hash | `ee58892b2b1c6fab311d773b32722f52` | 동일 |
| open alert / business key 중복 | 0 / 0 | 0 / 0 |

588행은 `2종목 × (181개 1분봉 + 61개 3분봉 + 37개 5분봉 + 15개 일봉)`입니다. [run ID와 원본 요약](evidence/sixth-assignment/README.md)도 공개했습니다.

### 확인 기준

1. `Parquet manifest = Kafka published = Consumer received = Spark input`
2. Spark invalid와 원본 `event_id` 중복을 따로 기록
3. `(symbol, bar_start, timeframe, source, feed)`로 PostgreSQL Upsert
4. 재실행 전후 행 수와 정렬된 OHLCV·거래 건수·VWAP hash 비교
5. `pipeline_runs`, `pipeline_work_items`, `pipeline_run_checks`에서 실패와 미해결 alert 확인

## 5. 현재 실제 연결과 남은 작업

### 현재 실제 구현

- Raw validation: Parquet → Kafka → Spark Batch / Streaming → `market_bars`
- Market context: 공식 발표 → Airflow orchestration → Alpaca 1m·1d → 3m·5m → `market_bars`
- 실행 관측성: `pipeline_runs` → `pipeline_work_items` → `pipeline_run_checks`
- CPI point-in-time: CPI 55회 × FRED·ALFRED 10 series = `macro_event_contexts` 550행

### 최신 데이터 모델

| 테이블 | 한 행 | business key |
| --- | --- | --- |
| `economic_events` | 공식 발표 한 번 | type·reference period·released at |
| `macro_observations` | 특정 vintage의 지표값 | series·date·realtime start |
| `macro_event_contexts` | 발표 시점에 이용 가능한 series | event ID·series ID |
| `market_bars` | 종목별 1m/3m/5m/1d | symbol·start·timeframe·source·feed |
| `pipeline_runs` | 실행 한 번 | pipeline run ID |
| `pipeline_work_items` | 경제발표 1건 × 종목 1개 × 단계 | run·event·symbol·stage |
| `pipeline_run_checks` | 품질검사·alert | run·event·symbol·stage·check |

### 아직 실행되지 않은 단계

- 신규 DAG으로 전체 77회 × 10종목, 즉 770개 work item을 다시 실행
- **Kafka v2 파티션 비교**: `symbol` key와 `event_id + symbol + segment` key, 3개와 6개 파티션 비교
- 고용·PCE·FOMC의 실제 발표값을 point-in-time으로 결합
- **전체 경제 이벤트 영향 계산**, 비발표일 비교군과 통계 검정
- 거래비용·슬리피지·시점 누수를 포함한 **백테스트**
- 검증 archive fallback, 운영 채널 alert, 스케줄 등록

수집·전달·전처리·저장·재실행·관측성은 실제로 연결했습니다. 경제지표가 주가를 움직였다는 인과 결론과 예상 수익률은 아직 만들지 않았습니다.

## 6. 실행 방법과 증거

```bash
docker compose up -d --wait postgres kafka kafka-init
AIRFLOW_HOME="$PWD/airflow-runtime" .venv/bin/python scripts/configure_airflow_pools.py

AIRFLOW_HOME="$PWD/airflow-runtime" \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
.venv/bin/airflow dags test market_context_backfill_pipeline \
  -f "$PWD/dags/market_context_backfill_pipeline.py" \
  -c '{"event_types":["FOMC"],"release_from":"2026-07-29","release_to":"2026-07-29","symbols":["SPY","TLT"],"feed":"sip","data_cutoff":"2026-09-03T00:00:00Z"}'

.venv/bin/python scripts/run_pipeline_alert_drill.py \
  --output-dir docs/evidence/sixth-assignment

.venv/bin/python -m unittest discover -s tests -v
```

- [기존 GCP 부하·복구 증거](evidence/load-recovery/README.md)
- [Airflow·alert·멱등성 증거](evidence/sixth-assignment/README.md)
- [검증 SQL](../scripts/evidence/sixth_assignment_summary.sql)
- [4분 발표 대본](09.03_대본.md)
