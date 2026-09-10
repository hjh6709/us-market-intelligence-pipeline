# Economic Event Intelligence & Strategy Validation Platform

공식 미국 경제 발표 시각과 당시 알 수 있었던 정보를 시장 데이터에 연결하는, 재현 가능한 데이터 플랫폼입니다.

> **Data engineering first.** 연구 결과는 과거 분석이며 투자 추천이 아닙니다. 연구 신호는 주문 입력으로 사용할 수 없고, 주문 기능은 사용자가 직접 검토하고 승인하는 Alpaca Paper 전용 경로로 격리돼 있습니다.

[Overview](http://127.0.0.1:8000/overview) · [Research](http://127.0.0.1:8000/) · [Pipelines](http://127.0.0.1:8000/pipelines) · [Paper Execution](http://127.0.0.1:8000/paper) · [API docs](http://127.0.0.1:8000/docs)

![실제 PostgreSQL 집계를 읽는 플랫폼 Overview](docs/images/portfolio/overview.jpg)

### Architecture

[현재 구현](docs/architecture/current-system.md) · [approved target contract](docs/architecture/platform-contract.md) · [현재와 target 차이](docs/engineering/current-vs-target.md) · [Airflow 실행 계약](docs/engineering/airflow-audit.md)

아래 이미지는 구현 완료 그림이 아니라 approved target입니다. 노드의 `CURRENT`, `EVOLVING`, `P1 FOUNDATION` tag와 [현재/target 표](docs/engineering/current-vs-target.md)를 함께 읽어야 합니다. 이어지는 Mermaid는 현재 연결만 설명합니다.

[![approved target architecture](docs/diagrams/target-platform.visual-check.1440x900.light.png)](docs/diagrams/target-platform.html)

아래 정적 이미지는 과정 과제에서 사용한 기존 구현 흐름을 보존한 것입니다. approved target이나 최신 완료 범위를 뜻하지 않으며, 판단에는 위 target diagram과 current/target 표를 사용합니다.

![전체 프로젝트 데이터 파이프라인 아키텍처](docs/diagrams/pipeline-architecture.png)

## 핵심 결과

아래 수치는 날짜가 있는 기존 실행 evidence의 결과입니다. 이번 architecture pass가 같은 대용량 실행을 다시 수행했다는 뜻이 아닙니다.

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
    K[Kafka · versioned routing]
    S[Spark event-time validation]
    PQ --> K --> S
  end

  subgraph Product[Product plane]
    PG[(PostgreSQL research/serving tables)]
    VPG[(PostgreSQL validation-only tables)]
    A[Versioned impact analysis]
    API[FastAPI serving layer]
    UI[Overview · Research · Pipelines]
    PAPER[Isolated Paper Execution]
    PG --> A --> API --> UI
    PAPER -. manual only .-> API
  end

  AF --> PG
  S --> VPG
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
- 시장 데이터 Airflow 실행·work item·품질 검사·alert를 durable telemetry로 기록
- Kafka/Spark 원시 체결 검증과 분석용 provider-bar 경로의 의미 분리
- legacy curated table은 business key/upsert로, normalized fact와 validation evidence는 append-only idempotency/conflict로 재실행 의미 보존
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
3. **연구:** event + bar → versioned impact → 기준 전략 결과; PIT macro context는 같은 발표의 설명 맥락으로 함께 조회
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

## Orchestration with Airflow

Airflow는 매개변수 기반 역사 데이터 재수집, 작업 의존성, 제한된 재시도와 API 동시성 제어를 담당합니다. 네 DAG 모두 `schedule=None`, `catchup=False`, `max_active_runs=1`입니다. 이는 통제된 backfill 설계이며 상시 실시간 수집이나 cron 운영을 구현했다고 주장하지 않습니다.

| 실제 DAG ID | 역할 | 결과 확인 경로 |
| --- | --- | --- |
| `market_context_backfill_pipeline` | 발표별 다종목 시장 봉 수집·검증 | PostgreSQL `pipeline_*`, Pipelines 상세 |
| `macro_context_backfill_pipeline` | 발표 시점 FRED/ALFRED 맥락 | `macro_event_contexts`, Airflow 작업 상태·로그 |
| `market_sip_replay_pipeline` | 제한된 원시 SIP Kafka→Spark→DB 재생 | Airflow 상태·로그, delivery/DB 검증 결과 |
| `market_context_backfill_orchestrator` | 연도별 market child run 생성·완료 대기 | Airflow trigger 상태, child run 기록 |

시장 DAG: `validate_run_config → register_run → build_work_items → collect_market_context[mapped] → verify_run → finish_run`.
전체 범위는 **202개 발표별 mapped task × 내부 10종목 = 2,020개 DB work item**입니다. 2,020개 Airflow mapped task라는 뜻이 아닙니다.

시장·거시 DAG는 **최대 2회, 고정 30초 간격 재시도**이며 지수 backoff가 아닙니다. 시장 수집은 `alpaca_api_pool`과 mapped task 최대 4개, 거시 수집은 `fred_api_pool`과 최대 1개로 제한합니다. 시장 run 등록·검증·종료는 `postgres_write_pool`을 사용합니다. 수집 task 내부 DB 쓰기에는 별도 PostgreSQL pool이 중첩 적용되지 않습니다.

수집 성공과 관측 품질은 분리합니다. `COMPLETE` 요청의 `PARTIAL` 봉 품질은 경고이지 task 실패가 아닙니다. 실제 예외는 DB가 사용 가능할 때 실패 work item·검사·알림·run을 기록하고 다시 예외를 던집니다. DB 자체 장애나 등록 전 실패까지 같은 DB에 반드시 기록된다는 보장은 없으며, 이때는 Airflow 상태·로그를 함께 확인합니다.

2026-09-03의 전체 Airflow 실행·소요 시간과 2026-09-08의 직접 재수집·재계산은 **서로 다른 실행 증거**입니다. 거시·재생 DAG의 모든 상태가 `pipeline_runs`에 저장되는 것은 아닙니다. [DAG별 감사와 한계](docs/engineering/airflow-audit.md), [과정 요구사항 대응](docs/engineering/course-acceptance.md)을 참고하세요.

### D. 이벤트 분석과 기준 전략

`macro_event_impacts` 8,080행과 `event_strategy_results` 2,020행을 versioned upsert로 재계산했습니다. 1,988개 계산 가능 관측의 비용 차감 평균은 -0.1565%, 양수 비율은 39.34%였습니다. 실패한 기준도 숨기지 않고 데이터·가정·한계와 함께 제공합니다.

## 실행 방법

### 1. 준비

```bash
cp .env.example .env
uv sync --extra airflow
docker compose up -d --wait postgres kafka kafka-init
```

`.env`에 데이터 수집용 `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `FRED_API_KEY`를 넣습니다. 웹 모의주문은 별도 `ALPACA_PAPER_KEY_ID`, `ALPACA_PAPER_SECRET_KEY`와 해당 모의계정의 고정 ID `ALPACA_PAPER_ACCOUNT_ID`를 사용합니다. 브라우저에서는 키나 계정 범위를 받지 않습니다. 비밀키·원본 응답·대용량 Parquet은 Git에 올리지 않습니다.

### 2. 웹 애플리케이션

```bash
uv run --with python-dotenv uvicorn src.serving_api:app --env-file .env --host 127.0.0.1 --port 8000
```

`http://127.0.0.1:8000/overview`에서 시작합니다. 조회 화면은 외부 API를 호출하지 않고 PostgreSQL에 저장된 결과만 읽습니다. Paper 제출을 활성화하려면 서버에서 명시적으로 `ENABLE_PAPER_WEB_ORDERS=true`를 설정해야 하며, 그렇지 않으면 review까지만 가능합니다.

위 명령은 `.env`를 명시적으로 로드합니다. Paper의 계정·장 시각·대사 조회는 별도로 브로커 GET을 호출하지만 로컬 주문 목록은 DB만 읽습니다. 쓰기 허용 Paper 화면은 로컬/운영자 통제 시연용이며, 인증·CSRF 방어 없는 공개 서버에 노출하지 않습니다.

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
| `canonical_economic_events` / `economic_event_lifecycle_versions` | canonical event identity와 append-only 상태 revision foundation | event / event·lifecycle version |
| `economic_observation_registry` | 정확한 event·observation·unit ontology | observation code |
| `economic_release_observations` | 공식 값의 append-only revision foundation | event·observation code·revision |
| `economic_consensus_snapshots` | provider-local point-in-time consensus foundation | event·observation code·provider·snapshot time |
| `economic_surprises` / `current_canonical_surprises` | marker·actual·consensus lineage를 보존하는 immutable history / 현재 marker와 provider별 최신 pre-release consensus에 다시 맞는 projection | marker·actual observation·consensus·algorithm version |
| `calendar_snapshots` / `trading_sessions` | immutable market-calendar generation과 session foundation | snapshot / snapshot·session date |
| `economic_event_markers` | append-only marker revisions와 current primary foundation | event·kind·revision |
| `validation_runs` | immutable raw-validation execution lineage | validation run ID |
| `validation_reconstructed_bars` | raw-derived append-only validation bars | run·symbol·start·timeframe·source·feed |

Normalized event/calendar 테이블은 현재 비어 있는 foundation이며 production adapter, backfill, v2 reaction 계산, UI 연결은 아직 구현하지 않았습니다. Foundation write functions는 같은 logical identity를 transaction 단위로 직렬화해 동일 내용은 같은 fact로 수렴하고 다른 내용은 domain conflict로 거부합니다. `validation_runs`와 `validation_reconstructed_bars`는 현재 raw-SIP Spark sink가 사용하지만 연구용 `market_bars` 및 serving과 물리적으로 분리됩니다. 자세한 현재/target 구분은 [current-vs-target](docs/engineering/current-vs-target.md)을 따릅니다.

## 다음 단계

- forecast·first-release actual·surprise의 신뢰 가능한 point-in-time 출처 추가
- 비발표일 비교군과 사건 통제, 통계 검정
- 실제 Paper fill 기반 position baseline·reconciliation과 위험 한도 검증
- stale `RUNNING` Airflow run의 terminal reconciliation
- OpenLineage/Marquez는 core 안정성을 바꾸지 않는 선택적 관측성 확장으로만 검토

## 구현·과제 증거

- [문서 허브](docs/README.md)
- [2026-09-10 baseline audit](docs/engineering/baseline-audit-2026-09-10.md) · [target architecture](docs/diagrams/target-platform.html)
- [플랫폼 감사](docs/engineering/platform-audit.md) · [API 계약](docs/engineering/api-contracts.md)
- [7차시 서빙 과제](docs/serving-layer-assignment.md) · [최종 시연 증거](docs/evidence/serving-layer/README.md)
- [6차시 부하·복구](docs/load-recovery-assignment.md)
- [3차시 Kafka·Spark 과제](docs/kafka-spark-assignment.md)
- [Paper execution 계약과 증거](docs/paper-execution.md)
- [최종 테스트·통합·브라우저 검증](docs/evidence/final-portfolio/verification.md)
- [Archify 대화형 구성도](docs/diagrams/session7-architecture.html)
- [과정 발표 자료와 과거 실행 기록](docs/README.md#과정-아카이브)

현재 구현과 한계의 정본은 코드·migration·테스트와 최신 검증 증거입니다. 날짜가 붙은 과제 자료는 당시 실행을 보존하는 역사 기록이며 현재 계약을 덮어쓰지 않습니다.
