# 6차시 과제 — 부하·복구 결과 보완 및 전체 흐름 점검

> 실행하지 않은 기능은 완료로 표시하지 않습니다. 숫자는 원시 체결, 이벤트별 bar 선택 합계, PostgreSQL 고유 행을 구분해 표기합니다.

## 먼저 보는 결론

이번 보완으로 경제 이벤트를 CPI만이 아니라 고용·PCE·FOMC까지 **202회**, 시장 대상을 **10종목**으로 확대했습니다. 전체 분석용 시장 데이터는 실제 수집했으며, Kafka 파티션 쏠림 개선도 실제 재실행으로 검증했습니다.

![최신 전체 데이터 파이프라인](diagrams/pipeline-architecture.png)

현재 실행 경로는 네 개입니다.

```text
A. 원시 체결 검증
Parquet → Kafka v2 → Spark → PostgreSQL

B. 분석용 시장 데이터
공식 발표 202회 → Airflow/Python → Alpaca 1m·1d → 3m·5m → PostgreSQL

C. 발표 당시 경제 상황
공식 발표 202회 → Airflow/Python → FRED·ALFRED 10 series → PostgreSQL

D. 이벤트 분석 결과
PostgreSQL 1m → 이벤트 전후 지표 8,080행 → 탐색 전략 결과 2,020행
```

## 과제 요구사항 한눈에 보기

| 요구사항 | 실제 결과 | 증거 |
| --- | --- | --- |
| 기준·부하 실행 비교 | 118,118건 / 76.480초 / 472행 → 7,360,804건 / 1,690.250초 / 22,260행 | [GCP 부하 증거](evidence/load-recovery/README.md) |
| 오류·미처리 건수 | 형식 오류 0, 수정 후 실제 중복 0, DB business key 중복 0 | [식별키 수정 증거](evidence/pipeline-review/README.md) |
| 실패 단계와 복구 | Spark heap, DB 중단, mock API 503를 재현하고 해당 단계부터 재실행 | 3절 |
| fallback 또는 alert | mock 503에서 `FAILED / OPEN → SUCCEEDED / RESOLVED` | [Airflow·alert 증거](evidence/sixth-assignment/README.md) |
| 최신 구성도·데이터 모델 | 위 구성도와 6절 | 이 문서 |
| Kafka·Spark·저장·Airflow 확인 | 단계별 건수, 실제 run ID, SQL 확인법 | 1·2·4·7절 |
| 이번 신규 확장 | 공식 발표 202회 × 10종목, 1m·3m·5m·1d, Kafka v2 | [확장 요약](evidence/multi-event-expansion/README.md) |
| 최종 결과 연결 | 이벤트 구간 지표 8,080행과 비용 포함 탐색 전략 2,020행 | 5절 |
| 아직 실행되지 않는 단계 | 전망치·surprise, 통제군 검정, paper/live 주문 | 6절 |

## 1. 기준 실행과 부하·복구 결과

### 1.1 기준과 부하

두 실행 모두 Alpaca에 부하를 준 것이 아닙니다. API로 미리 수집해 checksum을 확인한 Parquet 원본을 Kafka에 재생했습니다.

| 항목 | 기준 실행 | GCP 부하 실행 | 식별키 수정 후 정확성 재실행 |
| --- | ---: | ---: | ---: |
| 범위 | CPI 1회 × 4종목 | CPI 55회 × 4종목 | 동일 |
| 실제 SIP 개별 체결 | 118,118 | 7,360,804 | 7,360,804 |
| Kafka 발행 / 수신 | 118,118 / 118,118 | 7,360,804 / 7,360,804 | 7,360,804 / 7,360,804 |
| Spark 입력 | 118,118 | 7,360,804 | 7,360,804 |
| 형식 오류 | 0 | 0 | 0 |
| 실제 중복 | 0 | 당시 측정 오류 | 0 |
| PostgreSQL 1분봉 | 472 | 22,260 | 22,260 |
| DB business key 중복 | 0 | 0 | 0 |
| 실행 시간 | 76.480초 | 1,690.250초 | 436.653초(로컬) |
| 처리량 | 약 1,544건/초 | 약 4,355건/초 | 약 16,857건/초 |

GCP와 로컬 결과는 CPU·메모리·디스크 환경이 다르므로 속도 우열을 직접 비교하지 않습니다. GCP 실행 환경은 `e2-standard-4` 4 vCPU, RAM 16GB, 100GB `pd-standard`이며 Spark Driver heap은 그중 6GB로 설정했습니다.

### 1.2 49건을 다시 확인한 결과

GCP 실행 당시 중복으로 표시된 49건은 실제 중복이 아니었습니다. 같은 종목·거래 ID·시각이지만 거래소가 다른 별개의 체결을 기존 식별키가 같은 값으로 만든 것이 원인이었습니다.

```text
기존 event_id: source + feed + type + symbol + trade_id + timestamp
수정 event_id: source + feed + type + symbol + exchange + trade_id + timestamp
```

식별키를 수정하고 7,360,804건 전체를 Kafka부터 다시 처리해 실제 중복 0건, 1분봉 22,260행, DB 중복 0건을 확인했습니다.

## 2. 이번에 확장한 실제 데이터

### 2.1 범위

| 이벤트 | 기간 | 횟수 |
| --- | --- | ---: |
| CPI | 2022-01-12~2026-08-12 | 55 |
| Employment Situation | 2022-01-07~2026-08-07 | 55 |
| PCE / Personal Income and Outlays | 2022-01-28~2026-08-26 | 55 |
| FOMC statement | 2022-01-26~2026-07-29 | 37 |
| **합계** | 2022-01-07~2026-08-26 | **202** |

종목은 `SPY, QQQ, IWM, TLT, XLF, SMH, GLD, NVDA, AAPL, JPM` 10개입니다. 따라서 분석 단위는 **202회 × 10종목 = 2,020개 발표-종목 구간**입니다.

### 2.2 시장 데이터 실행 결과

한 발표마다 Alpaca 다종목 API로 10종목을 묶어 1분봉과 일봉을 각각 요청했습니다. 종목마다 따로 요청하면 4,040회가 필요하지만, 현재 구현은 **404회(페이지 추가 전 기준)**입니다.

| 저장 계층 | 이벤트별 선택·생성 합계 | 의미 |
| --- | ---: | --- |
| SIP 1분봉 | 308,512 | 발표 T-60~T+120분에 실제 존재한 봉 |
| 파생 3분봉 | 112,593 | PARTIAL 19,178 포함 |
| 파생 5분봉 | 70,090 | PARTIAL 16,215 포함 |
| SIP 일봉 | 30,250 | 이전 7 + 발표일 + 이후 7거래일의 선택 합계 |

이 표는 **각 이벤트 관점의 합계**입니다. 인접한 이벤트가 같은 시각이나 거래일을 공유하면 PostgreSQL은 `symbol + bar_start + timeframe + source + feed`로 한 번만 저장합니다. 따라서 DB 테이블 전체 고유 행 수와 위 합계가 다른 것은 정상입니다.

1분봉은 이론상 구간당 최대 181개지만 장전에는 거래가 없는 분이 있을 수 있습니다. 없는 가격을 만들지 않습니다. 3분봉과 5분봉도 포함된 1분 수를 `source_bar_count`로 남기고, 기대 개수보다 적으면 `PARTIAL`로 저장합니다.

일봉 coverage 2,020건은 다음과 같습니다.

| 상태 | 건수 | 이유 |
| --- | ---: | --- |
| COMPLETE | 1,980 | 이전 7 + 발표일 1 + 이후 7거래일 존재 |
| MARKET_CLOSED | 30 | Good Friday 발표 3회 × 10종목 |
| FUTURE_SESSION_UNAVAILABLE | 10 | 2026-08-26 PCE 이후 거래일이 실행 시점에 5일만 존재 |

### 2.3 Kafka 파티션 개선

기존 v1은 `symbol`만 key로 사용했습니다. 네 종목의 거래량 차이와 hash 배치 때문에 세 파티션 중 하나에 97.5%가 몰렸습니다.

v2는 `event type + 발표일 + symbol + 15분 segment`를 key로 사용하고 파티션을 6개로 늘렸습니다. 같은 기준 입력 118,118건을 실제 재실행한 결과입니다.

| 파티션 | 메시지 수 |
| ---: | ---: |
| 0 | 40,069 |
| 1 | 29,314 |
| 2 | 15,098 |
| 3 | 5,580 |
| 4 | 9,793 |
| 5 | 18,264 |

가장 큰 파티션 비중은 **33.9%**입니다. 기존보다 개선됐지만 6개 파티션에 완전히 균등한 것은 아닙니다. 발행 118,118건, 수신 118,118건, Spark 입력 118,118건, 형식 오류·중복 0건, 최종 1분봉 472행도 함께 확인했습니다.

## 3. 장애 재현과 복구

### 3.1 Spark heap 부족

7,360,804건에서 Spark의 검증 결과와 거래 조건 적용 결과를 RAM에 `cache()`하자 JVM heap이 부족했습니다. 원본 파일 전체가 6GB라는 뜻이 아닙니다. 여러 단계가 다시 사용하는 중간 DataFrame이 6GB로 제한한 Spark heap을 넘었다는 뜻입니다.

`DISK_ONLY`로 바꿔 중간 DataFrame을 VM 디스크에 두고 같은 전체 입력을 완료했습니다. 처리가 끝나면 코드가 `unpersist()`를 호출해 Spark가 관리하던 중간 블록을 해제합니다. 원본 Parquet과 PostgreSQL 결과는 지우지 않습니다.

### 3.2 PostgreSQL 중단

GCP VM 전체가 아니라 PostgreSQL 컨테이너만 중지했습니다. Kafka 발행·수신 후 DB 적재에서 `OperationalError`가 발생했고 실행 상태는 `failed`, 신규 저장은 0건이었습니다. 컨테이너를 다시 시작하고 health check를 통과한 뒤 실패한 입력을 재실행했습니다. Upsert 결과는 22,260행, business key 중복 0건으로 유지됐습니다.

### 3.3 API 503와 alert

외부 API에 고의 장애를 보내지 않았습니다. 로컬 mock client가 첫 요청에 503을 반환하도록 만들었습니다.

| 시점 | 작업 상태 | 품질검사 | alert | 저장 |
| --- | --- | --- | --- | ---: |
| 첫 요청 | FAILED | FAIL | OPEN | 0 |
| 재시도 | SUCCEEDED | PASS | RESOLVED | 합성 fixture 1행 |

fixture는 재시도·상태 전이만 검증하며 실제 시장 데이터나 fallback 데이터로 사용하지 않습니다. 현재 검증되지 않은 원본으로 자동 대체하는 fallback은 구현하지 않았고, 검증 실패 시 작업을 실패 처리합니다.

## 4. Airflow 자동화와 확인 결과

`market_context_backfill_pipeline`은 event type, 날짜 범위, 종목 목록, feed, 데이터 기준시각을 입력받습니다.

- Airflow task 단위: 경제발표 1건
- task 내부 API 호출: 10종목을 묶은 1분봉 1회 + 일봉 1회
- DB 관측 단위: 경제발표 × 종목별 work item과 품질검사
- 다년 실행: orchestrator가 연도별 child DAG run으로 분리

먼저 smoke run은 FOMC 2026-07-29의 SPY·TLT로 수행했습니다.

| 결과 | 값 |
| --- | ---: |
| 1m / 3m / 5m / 1d | 362 / 122 / 74 / 30 |
| 종목별 work item | 2개 성공 |
| 미해결 alert | 0 |
| provider 요청 | 2회(페이지 추가 전) |

`macro_context_backfill_pipeline`은 발표별 FRED·ALFRED 수집을 자동화하고 `fred_api_pool`로 호출 동시성을 제한합니다. FOMC 2026-07-29 × DGS2 한 건의 실제 Airflow 실행과 저장을 확인했습니다.

전체 202회 × 10종목 시장 데이터와 202회 × 10 series 경제 맥락은 먼저 동일한 구현 함수를 사용하는 CLI로 전수 실행했습니다. 경제 맥락은 CPI 550행, 고용 550행, PCE 550행, FOMC 370행으로 총 2,020행입니다.

그다음 시장 DAG도 **202개 발표 task 전체**를 실제 실행했습니다. 각 task는 10종목을 묶어 조회하고 DB에는 종목별 2,020개 work item을 기록합니다. 522.660초 뒤 1,980개 성공, 30개 휴장, 10개 미래 거래일 미도래, 실패 0개, 미해결 alert 0개로 검증됐습니다. 경제 맥락 DAG는 이미 저장된 point-in-time 값을 재사용하는 멱등 모드로 202개 발표 task를 실행하고, 마지막 검증 task가 2,020개 context를 모두 확인했습니다. 이 실행은 14.835초였고 외부 API를 다시 호출하지 않았습니다. 실행 run ID와 task 상태는 [전체 Airflow 증거](evidence/multi-event-expansion/airflow-full-run.json)에 남겼습니다.

## 5. 이벤트 반응 분석과 탐색용 백테스트

`market_bars`와 `economic_events.released_at`을 연결해 발표·종목별 네 구간을 계산했습니다.

| 구간 | 의미 |
| --- | --- |
| PRE_60M | 발표 전 60분의 첫 실제 시가부터 마지막 실제 종가 |
| POST_5M / 30M / 60M | 발표 직전 마지막 종가를 기준으로 발표 후 각 시점까지 |

수익률·거래량·분 단위 수익률 변동성·SPY 대비 수익률과 coverage를 `macro_event_impacts`에 **8,080행** 저장했습니다. 발표 전 데이터가 없는 32행, 발표일 휴장 등 발표 후 60분 데이터가 없는 30행도 값을 만들지 않고 상태로 남겼습니다.

전망치나 surprise가 없으므로 경제지표 방향을 맞히는 전략으로 만들지 않았습니다. 시점 누수가 없는 가장 단순한 기준으로 발표 전 60분 가격 방향을 따라 진입하고 발표 60분 후 청산했으며, 왕복 거래비용 10bp를 차감했습니다.

| 결과 | 값 |
| --- | ---: |
| 전체 / 실행 가능 | 2,020 / 1,988 |
| 전·후 coverage 모두 COMPLETE | 911 |
| 평균 / 중앙값 순수익률 | -0.1565% / -0.1251% |
| 순수익 양수 비율 | 39.34% |

네 발표 유형 모두 평균 순수익률이 음수였습니다(CPI -0.1858%, 고용 -0.1781%, PCE -0.1360%, FOMC -0.1118%). 따라서 현재 규칙에는 수익성이 없다고 기록합니다. 이 값은 독립적인 발표-종목별 결과이며 동시 포지션과 자본 배분을 합친 포트폴리오 수익률이 아닙니다. 경제지표의 인과 효과나 미래 예상 수익률로도 해석하지 않습니다.

## 6. 최신 데이터 모델과 남은 작업

| 테이블 | 한 행 | business key |
| --- | --- | --- |
| `economic_events` | 공식 발표 한 번 | type·reference period·released at |
| `macro_event_contexts` | 발표 당시 이용 가능한 경제지표 하나 | event ID·series ID |
| `market_bars` | 종목별 1m·3m·5m·1d | symbol·start·timeframe·source·feed |
| `pipeline_runs` | 실행 한 번 | pipeline run ID |
| `pipeline_work_items` | 실행의 event·symbol·stage | run·event·symbol·stage |
| `pipeline_run_checks` | 품질검사·alert | run·event·symbol·stage·check |
| `macro_event_impacts` | 발표·종목·구간별 반응 | event·symbol·window·analysis version |
| `event_strategy_results` | 발표·종목별 기준 전략 결과 | event·symbol·strategy·version |

현재 완료:

- CPI·고용·PCE·FOMC 공식 발표 202회 manifest
- 10종목의 발표 장중·전후 7거래일 데이터 전수 실행
- 1분봉에서 3분봉·5분봉 생성과 coverage 저장
- Kafka v2 파티션 개선 실험과 Spark·PostgreSQL 검증
- Airflow 시장·거시 DAG의 202개 발표 task 전체 실행과 최종 건수 검증
- 8,080개 이벤트 구간 지표와 비용 포함 탐색 전략 2,020개 실행
- 실패·재시도·alert·멱등 Upsert 확인

아직 미완료:

- 발표별 시장 전망치와 실제값으로 surprise 계산
- 비발표일 비교군·통계적 유의성·다른 사건 통제
- 호가 기반 슬리피지·동시 포지션·자본 배분을 포함한 포트폴리오 백테스트
- 운영 알림 채널과 검증 archive fallback
- paper/live 주문 실행

## 7. 실행과 최종 결과 확인

```bash
# 서비스
docker compose up -d --wait postgres kafka kafka-init

# 전체 시장 데이터
.venv/bin/python scripts/collect_market_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26 \
  --symbols SPY QQQ IWM TLT XLF SMH GLD NVDA AAPL JPM

# 전체 발표 시점 경제 맥락
.venv/bin/python scripts/collect_macro_event_context.py \
  --event-types CPI EMPLOYMENT PCE FOMC \
  --release-from 2022-01-01 --release-to 2026-08-26

# 공개 가능한 집계와 DB 중복 확인
.venv/bin/python scripts/evidence/export_multi_event_summary.py

# 이벤트 반응과 탐색 전략
.venv/bin/python -m src.macro_event_impact
.venv/bin/python -m src.event_strategy_backtest
.venv/bin/python -m scripts.evidence.export_event_analysis

# 테스트
.venv/bin/python -m unittest discover -s tests -v
```

증거:

- [기준·GCP 부하·DB 복구](evidence/load-recovery/README.md)
- [식별키 수정 후 전체 재실행](evidence/pipeline-review/README.md)
- [202회 × 10종목 확장 결과](evidence/multi-event-expansion/README.md)
- [Airflow·alert 실행](evidence/sixth-assignment/README.md)
- [Kafka v2 기계 판독 결과](evidence/load-recovery/v2-partition-routing.json)
- [4분 발표 대본](09.03_대본.md)
