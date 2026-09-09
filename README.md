# U.S. Economic Event Market Intelligence Platform

미국 경제 발표를 공식 시각에 맞춰 시장 데이터와 연결하고, 수집 품질·재현 가능한 연구 결과·격리된 모의주문까지 한곳에서 확인하는 데이터 플랫폼입니다.

> **Data engineering first.** 연구 결과는 과거 분석이며 투자 추천이 아닙니다. 연구 신호는 주문 입력으로 사용할 수 없고, 주문 기능은 사용자가 직접 검토하고 승인하는 Alpaca Paper 전용 경로로 격리돼 있습니다.

[Overview](http://127.0.0.1:8000/overview) · [Research](http://127.0.0.1:8000/) · [Pipelines](http://127.0.0.1:8000/pipelines) · [Paper Execution](http://127.0.0.1:8000/paper) · [API docs](http://127.0.0.1:8000/docs)

![실제 PostgreSQL 집계를 읽는 플랫폼 Overview](docs/images/portfolio/overview.jpg)

### Architecture

![전체 프로젝트 데이터 파이프라인 아키텍처](docs/diagrams/pipeline-architecture.png)

## 핵심 결과

| 범위 | 검증된 결과 | 단위와 의미 |
| --- | ---: | --- |
| 공식 발표 | **202** | CPI 55 + Employment 55 + PCE 55 + FOMC 37 |
| 분석 자산 | **10** | ETF·주식 symbols |
| 발표-종목 구간 | **2,020** | 202 releases × 10 symbols |
| 실제 SIP 1분봉 | **308,512** | PostgreSQL 고유 bar rows |
| 파생 3분봉 / 5분봉 | **112,593 / 70,090** | 실제 1분봉만 집계, 가격 채움 없음 |
| 발표 시점 거시 맥락 | **2,020** | point-in-time context rows |
| 이벤트 영향 결과 | **8,080** | 202 × 10 × 4 windows |
| 기준 전략 결과 | **2,020 / 1,988** | 전체 / 수익률 계산 가능 observations |
| 기준 전략 평균 | **-0.1565%** | 왕복 비용 10 bp 차감 후, 수익 전략 아님 |
| 양수 관측 비율 | **39.34%** | 미래 기대수익률 아님 |
| 중복 business key | **0** | 최종 재수집·재계산 검증 |

## 왜 만들었나

경제 발표 직후 가격을 보는 것만으로는 결과를 재현하기 어렵습니다. 발표 시각, 당시 알 수 있었던 거시 정보, 시장 데이터의 feed와 시간 범위, 결측 이유, 분석 버전이 함께 남아야 합니다.

- 어떤 공식 발표를 어떤 UTC 시각으로 사용했는가?
- 공급자 요청은 정상 종료됐는가, 가격 봉은 얼마나 관측됐는가?
- 그 시점에 실제로 알 수 있었던 경제 정보는 무엇인가?
- 같은 입력을 다시 실행해도 동일한 business key로 저장되는가?
- 결과가 API와 웹에서 실제로 다시 읽히는가?
- 연구 신호와 주문 행동이 확실히 분리되는가?

## 플랫폼 구조

```mermaid
flowchart LR
  subgraph Research[Research plane]
    CAL[Official BLS · BEA · Fed releases]
    ALP[Alpaca SIP bars]
    FRED[FRED · ALFRED vintages]
    AF[Airflow mapped backfills]
    CAL --> AF
    ALP --> AF
    FRED --> AF
  end

  subgraph Validation[Validation plane]
    PQ[Archived SIP trades]
    K[Kafka v2 · 6 partitions]
    S[Spark event-time validation]
    PQ --> K --> S
  end

  subgraph Product[Product plane]
    PG[(PostgreSQL)]
    A[Versioned impact analysis]
    API[FastAPI serving layer]
    UI[Overview · Research · Pipelines]
    PAPER[Isolated Paper Execution]
    PG --> A --> API --> UI
    PAPER -. manual only .-> API
  end

  AF --> PG
  S --> PG
```

핵심 데이터 계약은 다음과 같습니다.

```text
provider collection integrity != observed price-bar coverage != analysis eligibility
```

API·pagination·요청 범위가 정상 종료된 성긴 bar 응답은 수집 성공입니다. 관측 봉 수와 `COMPLETE/PARTIAL/NO_MARKET_DATA`는 별도 품질 정보로 보존하고, 분석 가능 여부는 기존 90% 규칙이 따로 판단합니다. 3분봉과 5분봉도 forward fill 없이 실제 source count를 남깁니다.

## 제품 화면

- **Overview** — 실제 DB 집계, 이벤트·자산 범위, 음수 기준 전략, 최신 파이프라인 상태
- **Research** — 발표 marker가 있는 candlestick, 1m/3m/5m, 영향 구간, 거시 맥락, 과거·cross-asset 비교
- **Pipelines** — 실행 목록·상세, work item, 품질 검사, alert, 프로젝트 lineage
- **Paper Execution** — 계정/장 시각, 주문 작성, 검토, 정확한 문구 확인, 제출, 상태 갱신, 취소, GET-only recovery

웹 Paper 주문은 `BUY LIMIT DAY`, 정규장, 1–10주, 최대 USD 1,000만 허용합니다. `ENABLE_PAPER_WEB_ORDERS=true`가 없으면 쓰기 동작은 서버에서 거부합니다. live endpoint 선택 기능은 없으며 연구 객체도 주문 API에 들어갈 수 없습니다.

## 프로젝트 목표

- 공식 발표와 point-in-time 경제 환경을 미래 정보 없이 연결
- source/feed/version을 명시한 시장 데이터와 분석 결과 보존
- Airflow 실행·work item·품질 검사·alert를 durable telemetry로 기록
- Kafka/Spark 원시 체결 검증과 분석용 provider-bar 경로의 의미 분리
- PostgreSQL business key와 upsert로 재실행 중복 방지
- 저장 결과를 FastAPI와 웹 UI로 실제 조회
- 연구 신호, 과거 시뮬레이션, 주문 행동을 서로 다른 계약으로 유지

## 현재 분석 범위

| 구분 | 범위 |
| --- | --- |
| 공식 발표 | 2022-01-07~2026-08-26, CPI·Employment·PCE·FOMC 202회 |
| 종목 | `SPY`, `QQQ`, `IWM`, `TLT`, `XLF`, `SMH`, `GLD`, `NVDA`, `AAPL`, `JPM` |
| 장중 구간 | 발표 T-60부터 T+120, 최대 181개 후보 timestamp |
| 서빙 차트 | `[T-60, T+120)` 180분 |
| 경제 맥락 | FRED·ALFRED 10개 series의 발표 당시 이용 가능 값 |
| 분석 identity | `alpaca / sip / multi_event_sip_v1` |
| 전략 identity | `pre60_momentum_post60 / v1`, 비용 10 bp |

## 데이터 흐름

1. **분석 수집:** 공식 일정 → Airflow → Alpaca SIP bar와 FRED/ALFRED vintage → PostgreSQL
2. **원시 체결 검증:** archived Parquet → Kafka → Spark event-time 검증·집계 → PostgreSQL
3. **연구:** event + bar + macro context → versioned impact → 기준 전략 결과
4. **서빙:** PostgreSQL → repository/service → FastAPI → 네 개 제품 화면
5. **모의주문:** 사용자 입력 → server-side 검증 → explicit confirmation → Alpaca Paper → durable journal

원시 체결 7,360,804건은 제한된 CPI 55회×4종목의 부하·복구 입력이고, 308,512행은 202회×10종목의 분석용 1분봉입니다. 행 의미가 달라 서로 더하거나 직접 비교하지 않습니다.

## 데이터 출처

| 데이터 | 출처 | 저장 의미 |
| --- | --- | --- |
| CPI·Employment | BLS official release schedule | 공식 발표 event |
| PCE | BEA official release archive | 공식 발표 event |
| FOMC | Federal Reserve calendar | statement event |
| 시장 데이터 | Alpaca Historical Stock Data, SIP feed | 1m·3m·5m·1d bars |
| 경제 맥락 | FRED/ALFRED | 발표 시점 vintage context |

## 실제 구현 결과

### A. Kafka·Spark 원시 체결 처리

별도 118,118건 검증에서는 Kafka 발행 / 수신이 **118,118 / 118,118**, Spark validation error와 실제 중복이 0이었습니다. v1의 symbol-only key에서 최대 파티션 비중이 97.5%였고, v2의 event/release/symbol/15-minute segment key와 6 partitions에서 33.9%로 낮아졌습니다. 7,360,804건 부하·복구 결과와 118,118건 partition 결과는 서로 다른 실행입니다.

### B. 202개 발표·10종목 시장 데이터

Alpaca 다종목 요청을 발표별로 묶었습니다. 2026-09-08 재실행 기준 session provider collection 2,020건은 모두 정상 종료됐고, 1분봉 관측 품질은 `COMPLETE 581 / PARTIAL 1,409 / NO_MARKET_DATA 30`이었습니다. 휴장이나 정상적인 sparse bar를 수집 실패로 바꾸지 않았습니다.

### C. Airflow 자동화

`market_context_backfill_pipeline`은 발표 한 건을 mapped task 하나로 만들고 내부에서 종목을 묶어 요청합니다. 입력·시도 횟수·상태·오류·quality checks·alerts가 PostgreSQL에 남습니다. 다년 실행은 `market_context_backfill_orchestrator`가 연도별 child run으로 나눕니다.

### D. 이벤트 분석과 기준 전략

`macro_event_impacts` 8,080행과 `event_strategy_results` 2,020행을 versioned upsert로 재계산했습니다. 1,988개 계산 가능 관측의 비용 차감 평균은 -0.1565%, 양수 비율은 39.34%였습니다. 실패한 기준도 숨기지 않고 데이터·가정·한계와 함께 제공합니다.

## 실행 방법

### 1. 준비

```bash
cp .env.example .env
uv sync --extra airflow
docker compose up -d --wait postgres kafka kafka-init
```

`.env`에 데이터 수집용 `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `FRED_API_KEY`를 넣습니다. 웹 모의주문은 별도 `ALPACA_PAPER_KEY_ID`, `ALPACA_PAPER_SECRET_KEY`를 사용합니다. 비밀키·원본 응답·대용량 Parquet은 Git에 올리지 않습니다.

### 2. 웹 애플리케이션

```bash
.venv/bin/uvicorn src.serving_api:app --host 127.0.0.1 --port 8000
```

`http://127.0.0.1:8000/overview`에서 시작합니다. 조회 화면은 외부 API를 호출하지 않고 PostgreSQL에 저장된 결과만 읽습니다. Paper 제출을 활성화하려면 서버에서 명시적으로 `ENABLE_PAPER_WEB_ORDERS=true`를 설정해야 하며, 그렇지 않으면 review까지만 가능합니다.

### 3. 발표용 입력 → 처리 → 저장 → 읽기

```bash
.venv/bin/python -m scripts.run_serving_demo \
  --event-id 'CPI|2026-07|2026-08-12T12:30:00Z' \
  --symbol NVDA
```

한 이벤트·한 종목을 재계산해 영향 4행과 전략 1행을 upsert하고 같은 서빙 계층으로 다시 읽습니다. 검증 실행은 약 0.43초, 중복 0, `/health`와 상세 API HTTP 200이었습니다.

### 4. Airflow smoke

```bash
export AIRFLOW_HOME="$PWD/airflow-runtime"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
.venv/bin/airflow db migrate
.venv/bin/python scripts/configure_airflow_pools.py
.venv/bin/airflow dags test market_context_backfill_pipeline \
  -f "$PWD/dags/market_context_backfill_pipeline.py" \
  -c '{"event_types":["FOMC"],"release_from":"2026-07-29","release_to":"2026-07-29","symbols":["SPY","TLT"],"feed":"sip","data_cutoff":"2026-09-03T00:00:00Z"}'
```

### 5. 테스트

```bash
.venv/bin/python -m unittest discover -s tests -v
RUN_POSTGRES_INTEGRATION=1 \
  DATABASE_URL=postgresql://market:market@localhost:55432/market \
  .venv/bin/python -m unittest discover -s tests -p 'test_*integration.py' -v
```

## 저장 모델

| 테이블 | 한 행의 의미 | business key |
| --- | --- | --- |
| `economic_events` | 공식 발표 | deterministic event ID |
| `market_bars` | source/feed/timeframe별 가격 봉 | symbol·start·timeframe·source·feed |
| `macro_event_contexts` | 발표 당시 지표 값 | event·series |
| `pipeline_runs` | 파이프라인 실행 | run ID |
| `pipeline_work_items` | event·symbol·stage 작업 | run·event·symbol·stage |
| `pipeline_run_checks` | 검사와 alert 상태 | run·event·symbol·stage·check |
| `macro_event_impacts` | event·symbol·window 연구 결과 | source·feed·analysis version 포함 |
| `event_strategy_results` | 기준 전략 관측 | strategy name·version 포함 |
| `paper_order_intents` | Paper 주문 intent와 상태 | account scope·request ID |

## 다음 단계

- forecast·first-release actual·surprise의 신뢰 가능한 point-in-time 출처 추가
- 비발표일 비교군과 사건 통제, 통계 검정
- 실제 Paper fill 기반 position baseline·reconciliation과 위험 한도 검증
- stale `RUNNING` Airflow run의 terminal reconciliation
- OpenLineage/Marquez는 core 안정성을 바꾸지 않는 선택적 관측성 확장으로만 검토

## 구현·과제 증거

- [문서 허브](docs/README.md)
- [플랫폼 감사](docs/engineering/platform-audit.md) · [API 계약](docs/engineering/api-contracts.md)
- [7차시 서빙 과제](docs/serving-layer-assignment.md) · [최종 시연 증거](docs/evidence/serving-layer/README.md)
- [6차시 부하·복구](docs/load-recovery-assignment.md)
- [3차시 Kafka·Spark 과제](docs/kafka-spark-assignment.md)
- [Paper execution 계약과 증거](docs/paper-execution.md)
- [최종 테스트·통합·브라우저 검증](docs/evidence/final-portfolio/verification.md)
- [Archify 대화형 구성도](docs/diagrams/session7-architecture.html)
- [과정 발표 자료와 과거 실행 기록](docs/README.md#과정-아카이브)

현재 구현과 한계의 정본은 코드·migration·테스트와 최신 검증 증거입니다. 날짜가 붙은 과제 자료는 당시 실행을 보존하는 역사 기록이며 현재 계약을 덮어쓰지 않습니다.
