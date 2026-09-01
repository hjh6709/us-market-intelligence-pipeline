# 5차시 과제 — 데이터 파이프라인 부하·장애·복구 실험

> 이 문서만 읽어도 **어떤 데이터를 얼마나 처리했고, 어떤 장애를 만들었으며, 복구 후 무엇을 검증했는지** 알 수 있도록 실행 결과 중심으로 정리했습니다. 원본 JSON과 SQL은 마지막에 증거로 연결합니다.

## 발표할 때 먼저 말할 핵심

> 이번 과제에서는 한 번의 CPI 발표 구간에서 사용한 실제 주식 체결 118,118건을 기준으로 삼고, 동일한 범위를 CPI 발표 55회로 확대해 7,360,804건을 GCP에서 처리했습니다. 저장해 둔 실제 체결을 Kafka에 재생하고 Spark로 1분봉을 만든 뒤 PostgreSQL에 저장했습니다. DB 장애 복구 후 최종 22,260행과 DB 고유키 중복 0건을 확인했습니다. 6차시 점검에서 거래 식별키의 누락을 발견해 수정했으며, 정정 내용은 아래에 함께 기록합니다.

이 실험에서 확인하려는 것은 주가 예측 정확도가 아니라 다음 네 가지입니다.

1. 입력량이 약 62배로 늘어나도 Kafka와 Spark가 같은 건수를 처리하는가
2. 잘못된 형식과 중복 거래를 Spark가 구분하는가
3. PostgreSQL 장애가 성공으로 잘못 기록되지 않는가
4. 복구 후 같은 입력을 다시 처리해도 누락되거나 중복되지 않는가

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
| 당시 코드가 출력한 event ID 충돌 | 0 | 49주) |
| PostgreSQL 1분봉 | 472 | 22,260 |
| DB business key 중복 | 0 | 0 |
| GCP 실행 시간 | 76.5초 | 1,690.3초 |

기준 실행은 GCP 최초 Spark 실행이라 Kafka connector 다운로드 시간이 포함됐습니다. 따라서 두 시간을 입력량에 정비례하는 순수 성능 비교로 해석하지 않습니다.

주) 6차시 점검에서 이 49건은 실제 중복이 아니라 거래소가 다른 체결을 기존 식별키가 같은 거래로 오인한 것으로 확인했습니다. 거래소를 식별키에 포함한 뒤 전체를 다시 처리한 현재 결과는 **실제 중복 0건**입니다. GCP 표는 당시 실행 시간과 장애 실험의 원본 기록으로 남기고, 현재 정확성 결과는 [6차시 점검 문서](pipeline-review-assignment.md)를 기준으로 합니다.

부하 실행 시간 1,690.3초는 약 **28분 10초**이고, 전체 평균 처리량은 초당 약 **4,355개 체결**입니다. 기준 실행과 부하 실행은 입력 범위가 다르고 최초 실행 준비 비용도 달라, `62배 입력을 정확히 몇 배 빠르게 처리했다`는 성능 결론은 내리지 않습니다.

## 전체 아키텍처

![CPI 시장 데이터 수집·부하·복구 아키텍처](diagrams/pipeline-architecture.png)

이번 과제에서 실제로 실행한 중심 경로는 다음과 같습니다.

```text
BLS CPI 발표일 + FRED·ALFRED 당시 배경지표
                         ↓
Alpaca SIP 체결 → Parquet 원본 보관 → Kafka 재생
                                      ↓
                           Spark 검증·중복 제거
                                      ↓
                         provider 규칙 기반 1분봉
                                      ↓
                         PostgreSQL Upsert·검증
```

Airflow 다종목 실행은 앞선 과제에서 구현했습니다. 이번 대량 실험은 동일한 수집·Spark·DB 코드를 GCP 실험 실행기로 구동해 부하와 장애 복구를 집중 검증했습니다.

## 1. 어떤 데이터를 사용했는가

### 시장 데이터

- 출처: Alpaca Historical Trades API
- feed: `sip`
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 이벤트: BLS에서 실제 발표를 확인한 CPI 55회
- 기간: 2022-01-12 발표부터 2026-08-12 발표까지
- 각 구간: CPI 발표 60분 전 이상, 발표 61분 후 미만
- 한 행: 주식 한 종목의 개별 체결 한 건

외부 Alpaca API에 736만 건의 부하를 반복해서 보낸 것이 아닙니다. 수집 단계에서는 220개 종목·발표일 구간과 API pagination에 따라 여러 번 요청해 Parquet으로 저장했습니다. 기준·부하·복구 실험에서는 API를 다시 호출하지 않고 이 원본을 Kafka에 넣었습니다.

즉, 이번 부하 테스트의 대상은 외부 API가 아니라 **우리 내부의 Kafka → Spark → PostgreSQL 처리 경로**입니다.

```text
외부 Alpaca API
→ 실제 SIP 체결을 사전에 수집
→ Parquet으로 보관

부하 테스트
→ Parquet을 읽어 7,360,804건을 Kafka에 재생
→ Spark 전처리·집계
→ PostgreSQL 저장
```

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

10개 지표 × CPI 55회 = `macro_event_contexts` 550행입니다. 여기서 이벤트는 **CPI 발표 55회**이고, 나머지 10개 지표는 각 CPI 발표 당시 시장이 알고 있던 배경 정보입니다. FOMC·고용·PCE 발표일을 별도 이벤트로 분석한 결과는 아닙니다.

월별 지표는 발표일 당시 ALFRED vintage를 사용하고, 일별 금리·VIX는 오전 8시 30분 이후 확정되는 당일 값을 피하려고 발표 전날 이하의 최신 관측값만 사용합니다. 발표일보다 미래인 관측일 또는 realtime 시작일을 연결한 건수는 0건입니다. `fdnpy`는 이번 구현에 사용하지 않았습니다.

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

### 16GB·6GB·100GB는 각각 무엇인가

`6GB`는 디스크 용량이 아니라 **Spark Driver가 Java 작업 메모리로 사용할 수 있도록 지정한 RAM 크기**입니다.

```text
GCP VM 전체
├─ CPU: 4 vCPU
├─ RAM: 16GB
│  ├─ Spark Driver heap: 최대 6GB
│  └─ 나머지: 운영체제·Docker·Kafka·PostgreSQL 등
└─ 디스크: 100GB pd-standard
   ├─ Parquet 원본
   ├─ Kafka·PostgreSQL 파일
   └─ Spark DISK_ONLY 중간 결과
```

Spark에 VM의 16GB 전체를 주지 않은 이유는 같은 VM에서 운영체제, Kafka, PostgreSQL과 Docker도 함께 실행했기 때문입니다. Spark 중간 결과가 6GB RAM 안에 모두 들어가야 하는 구조가 아니라, 필요한 중간 결과를 100GB 디스크의 사용 가능한 공간에 내려놓도록 변경했습니다. 100GB 전체를 Spark가 독점했다는 뜻은 아닙니다.

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
| 당시 event ID 충돌 출력 | 49(실제 중복 아님) |
| 생성·Upsert 1분봉 | 22,260 |
| DB key 중복 | 0 |
| 전체 시간 | 1,690.250초 |
| 전체 처리량 | 4,354.861 events/s |

6차시 전체 점검에서 49건의 payload를 다시 비교한 결과, 동일한 체결이 반복된 것이 아니었습니다. 같은 종목·거래 ID·시각이더라도 거래소 `x`가 서로 다른 별개의 체결이었습니다. 기존 식별키가 거래소를 포함하지 않아 Spark가 이를 중복으로 오인하고 한 건만 남긴 결함이었습니다.

```text
기존 event_id: source + feed + type + symbol + trade_id + timestamp
수정 event_id: source + feed + type + symbol + exchange + trade_id + timestamp
```

수정 후 7,360,804건 전수 검사와 Kafka → Spark → PostgreSQL 전체 재실행에서 `duplicate=0`, 최종 1분봉 22,260행, DB 고유키 중복 0건을 확인했습니다. 수정 전 결과 hash는 폐기하고 수정 후 hash를 새 기준으로 사용합니다. 상세 증거는 [식별키 수정 후 실행 증거](evidence/pipeline-review/README.md)에 있습니다.

### 7,360,804건이 왜 22,260개의 1분봉이 되는가

7,360,804건은 **개별 체결 행**, 22,260건은 **종목별 1분 OHLCV 행**이므로 서로 단위가 다릅니다. 각 CPI 구간은 121개의 예상 분을 가지지만 모든 분에 가격을 만들 수 있는 체결이 존재하는 것은 아닙니다.

Odd Lot 조건의 체결도 실제 거래이므로 원시 입력에는 그대로 보존됩니다. 같은 분에 가격 반영 가능한 체결이 하나라도 있으면 Odd Lot의 수량과 건수도 그 1분봉의 거래량·거래 건수에 포함되지만, OHLC·VWAP 가격은 갱신하지 않습니다. 한 분의 모든 체결이 Odd Lot이면 가격을 만들 수 없어 `market_bars` 행 자체를 생성하지 않습니다. 이 경우 Odd Lot의 거래량은 원본에는 남지만 완성된 1분봉 행에는 나타나지 않습니다. 이는 수집 유실이 아니라 provider 집계 규칙에 따른 무가격 구간입니다.

```text
해당 분에 가격 반영 체결 존재
→ OHLCV 1분봉 생성 → PostgreSQL 저장

해당 분에 Odd Lot만 존재
→ 원시 체결은 보존 → 완성된 OHLCV 행은 생성하지 않음
```

현재 1분봉은 원시 결과를 검증하는 정본입니다. 희소한 장전 구간을 분석할 때는 이후 5분봉을 **추가 파생**할 계획입니다. 1분봉을 삭제하거나 없는 가격을 채우지는 않으며, 5분봉에도 포함된 원본 분 수와 coverage를 함께 기록합니다. 5분 전체에도 가격 체결이 없으면 5분봉 역시 없는 것이 정상입니다.

### 실제로 발견한 병목

#### 1) Spark `Java heap space`

처음 로컬에서 7,360,804건 전체를 실행했을 때 Spark가 검증 결과와 거래 조건 적용 결과를 RAM에 `cache()`했습니다. 여러 단계에서 같은 중간 데이터를 다시 사용하기 위한 선택이었지만, Java 프로세스가 사용할 수 있는 heap보다 중간 결과가 커져 `Java heap space` 오류가 발생했습니다.

```text
기존 방식
736만 건 읽기
→ 검증 중간 결과를 RAM에 cache
→ 거래 조건 중간 결과도 RAM에 cache
→ Java heap 부족으로 Spark JVM 종료

수정 방식
736만 건 읽기
→ 중간 결과를 DISK_ONLY로 보관
→ Spark Driver heap을 6GB로 명시
→ 같은 입력을 GCP에서 끝까지 처리
```

`DISK_ONLY`는 원본 데이터를 삭제하거나 6GB로 줄였다는 뜻이 아닙니다. Spark가 반복해서 사용할 중간 계산 결과를 RAM 대신 로컬 디스크에 저장하는 설정입니다. RAM보다 느릴 수 있지만, 메모리 부족으로 전체 작업이 중단되는 위험을 줄였습니다.

수정 후 GCP에서 7,360,804건을 다시 실행해 Kafka 발행·수신·Spark 입력이 모두 일치했고, 1,690.250초 만에 22,260개의 1분봉을 저장했습니다. 따라서 부하 테스트에서 발견한 메모리 병목을 코드와 실행 설정에 반영하고 같은 전체 범위로 복구한 것입니다.

#### 2) Kafka 파티션 쏠림

GCP의 기준·부하 실행 누적 offset을 확인하니 파티션별 메시지는 `0 / 7,294,235 / 184,687건`이었습니다. 종목코드를 Kafka key로 사용한 결과 3개 파티션 중 한 곳에 약 97.5%가 몰린 것입니다. 종목 내부 순서는 지켰지만 병렬 처리 효율은 낮았습니다. 다음 실험에서는 공통 토픽을 유지하면서 `symbol + release_date` 복합 key와 파티션 수를 함께 비교해야 합니다.

이 분포는 기준 실행 118,118건과 부하 실행 7,360,804건을 합한 Kafka 누적 offset입니다. 네 개의 종목코드만 key로 사용했기 때문에 세 파티션에 고르게 나뉜다는 보장이 없었습니다. 데이터 유실은 없었지만, 부하가 한 파티션에 집중돼 Kafka와 Spark 병렬 처리 능력을 충분히 사용하지 못했습니다.

## 5. 장애와 복구

### PostgreSQL 적재 실패

GCP의 PostgreSQL 컨테이너만 중지한 뒤 기준 데이터를 다시 실행했습니다.

| 단계 | 상태 | Kafka 발행 / 수신 | PostgreSQL 저장 | 전체 business row | key 중복 |
| --- | --- | ---: | ---: | ---: | ---: |
| 장애 전 | 성공 | 7,360,804 / 7,360,804 | 22,260 | 22,260 | 0 |
| DB 중지 후 실행 | `failed` | 118,118 / 118,118 | 0 | 22,260 | 0 |
| DB 재시작 후 동일 입력 | `succeeded` | 118,118 / 118,118 | 472 Upsert | 22,260 | 0 |

실패 실행은 `OperationalError`로 기록됐으며 성공으로 잘못 표시되지 않았습니다. 실패 JSON의 `spark_input`과 Spark 출력 수치는 0입니다. 이는 Spark가 읽지 않았다는 측정값이 아니라, DB Upsert에서 예외가 발생해 Spark 함수가 요약값을 반환하지 못했기 때문에 실패 결과의 기본값으로 기록된 것입니다. 이 장애 실험에서 확정적으로 비교하는 수치는 Kafka 발행·수신 118,118건, DB 저장 0건과 실패 상태입니다.

DB를 다시 시작하고 health check가 통과한 뒤 같은 입력을 실행했습니다. 기존 472개 key를 Upsert했으므로 최종 행은 22,260개에서 늘지 않았고 중복도 0건이었습니다.

### 안전하게 확인한 다른 장애

| 장애 | 재현 방식 | 결과 |
| --- | --- | --- |
| API 503 | 외부 API가 아닌 mock 응답 | 첫 요청 503 후 1회 재시도해 정상 JSON 수신 |
| 잘못된 입력 | `limit=0` 입력 | 파일 생성 전 `ValueError`, 수정값 재실행 후 정상 manifest 생성 |
| DB endpoint 오류 | 로컬의 사용하지 않는 port 1 | `OperationalError` 후 정상 endpoint health query 통과 |

외부 Alpaca·FRED 서비스에는 부하나 고의 실패 요청을 보내지 않았습니다.

## 6. 누락과 중복을 어떻게 확인했는가

1. 아카이브 manifest의 원시 행 수를 Kafka 기대 건수로 정합니다.
2. Producer가 받은 partition별 시작·끝 offset을 기록합니다.
3. Consumer와 Spark는 그 offset 범위만 읽습니다.
4. `원시 입력 = Kafka 발행 = Kafka 수신 = Spark 입력`인지 확인합니다.
5. Spark는 거래소를 포함한 결정적 `event_id`로 동일 체결의 재전송만 중복으로 판별합니다.
6. PostgreSQL은 `(symbol, bar_start, timeframe, source, feed)` 고유키로 Upsert합니다.
7. 재실행 전후 전체 행 수와 `GROUP BY ... HAVING COUNT(*) > 1` 결과를 비교합니다.
8. OHLC·거래량·거래 건수·VWAP까지 정렬해 만든 결과 hash가 재실행 전후 같은지 확인합니다.

GCP DB 장애·복구에서는 최종 시장 1분봉 22,260행과 DB business key 중복 0건을 확인했습니다. 이후 식별키 결함을 수정해 전체 입력을 다시 처리한 현재 결과는 22,260행, DB 고유키 중복 0건, 결과 hash `0e40961df007996bcb812532a4049193`입니다. 기존 hash는 서로 다른 체결 49건을 제외한 잘못된 결과이므로 더 이상 정확성 기준으로 사용하지 않습니다. macro point-in-time 위반도 0건입니다.

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

Parquet 원본과 전체 로그는 Git에서 제외합니다. 공개 증거는 [`docs/evidence/load-recovery`](evidence/load-recovery/README.md)에 있으며, 기준·부하·DB 실패·복구의 비밀정보 제거 실행 JSON도 각각 확인할 수 있습니다. 행 수·결과 hash·경제지표 시점 검증 SQL은 [`scripts/evidence/load_recovery_integrity.sql`](../scripts/evidence/load_recovery_integrity.sql)입니다.

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
