# Final presentation truth — 2026-09-10

이 문서는 최종 발표를 만드는 사람이 가장 먼저 읽어야 하는 정본입니다. 기준은 `origin/main`의 merge commit `56334a89ceff48f63e93b5c260df28e16179b74e`이며, PR #36의 canonical foundation까지 포함합니다. 날짜가 붙은 과거 실행 evidence와 현재 구현, approved target을 서로 바꾸어 말하지 않습니다.

## 왜 시작했는가

프로젝트의 출발점은 평범한 개인 투자자로서 겪은 불편입니다. 증권 서비스의 뉴스·AI 요약이나 커뮤니티 설명이 실제 가격 움직임과 언제나 한 방향으로 이해되지는 않았고, CPI·고용·PCE·FOMC가 무엇을 뜻하는지 비전문가가 직접 판단하기 어려웠습니다. 특정 발표가 가격 움직임의 원인인지, 발표 뒤에 붙인 편리한 설명인지도 구분하기 어려웠습니다.

발표에서는 특정 서비스가 틀렸다고 주장하지 않습니다. 다음 문장을 사용합니다.

> 기존 요약이나 댓글을 그대로 받아들이기보다, 당시 이용 가능했던 데이터와 실제 가격 반응을 연결해 직접 판단할 수 있는 환경을 만들고 싶었습니다.

## 제품 가설

```text
경제 이벤트
→ 무엇을 예상했는가?
→ 실제로 무엇이 발표됐는가?
→ 그 시각에 알 수 있었던 정보는 무엇인가?
→ 당시 거시 환경은 어땠는가?
→ 시장은 어떻게 반응했는가?
→ 그 반응은 역사적으로 이례적인가?
→ 어떤 가설을 세울 수 있는가?
→ 과거 시뮬레이션을 견디는가?
→ 향후 Paper 검증에서도 견디는가?
```

현재 제품은 이 전체 목표 중 수집·저장·legacy event study·조회와 제한된 수동 Paper 경계까지 구현했습니다. consensus/official observation 기반 surprise, session-aware reaction v2, 비교사건, 정식 simulator와 Paper experiment lifecycle은 foundation 또는 target입니다.

## 현재 아키텍처

정적 재현 원본은 [CURRENT_ARCHITECTURE.mmd](CURRENT_ARCHITECTURE.mmd)입니다.

### Research plane — 현재 운영·실행 증거 있음

공식 BLS·BEA·Federal Reserve 발표 일정, Alpaca provider-aggregated SIP bar, FRED/ALFRED 시점별 거시 맥락을 Airflow historical backfill로 수집해 PostgreSQL research tables에 저장합니다. legacy `multi_event_sip_v1` 영향 분석과 `pre60_momentum_post60/v1` 탐색 전략을 계산하고 serving 계층이 읽습니다.

### Validation plane — 현재 구현·별도 실험 증거 있음

Git에 넣지 않은 archived Raw SIP trades를 제한된 범위로 Kafka에 재생하고 Spark가 검증·중복 제거·event-time 집계를 수행합니다. 현재 sink는 immutable `validation_runs`와 `validation_reconstructed_bars`에 기록합니다. 이 결과는 비교·검증용이며 research serving의 `market_bars`와 분리됩니다.

**중요:** 202회×10종목 연구 데이터는 Raw SIP Kafka/Spark 경로로 만든 데이터가 아닙니다.

### Product plane — 현재 로컬 제품

PostgreSQL → repository → service → FastAPI → browser로 이어집니다. 화면은 Overview, Research, Pipelines, Paper Execution입니다. Paper는 사용자가 직접 입력·검토·확인하는 Alpaca Paper 전용 경계이며 research 신호가 자동 주문 입력으로 바뀌지 않습니다.

## GCP의 실제 역할

GCP Compute Engine `e2-standard-4`는 2026-08-31 baseline replay, 736만 Raw SIP 부하, PostgreSQL 중단, 복구·재처리를 검증한 **실험용 compute 환경**입니다. 프로덕션 serving 배포가 아닙니다. evidence 복사 후 VM은 삭제됐습니다.

## 검증된 데이터 규모

| 항목 | 검증값 | 증거 시점·의미 |
| --- | ---: | --- |
| 공식 발표 | 202 | CPI 55 + Employment 55 + PCE 55 + FOMC 37 |
| 종목 | 10 | SPY, QQQ, IWM, TLT, XLF, SMH, GLD, NVDA, AAPL, JPM |
| event-symbol | 2,020 | 202×10 |
| 1m research bars | 308,512 | 2026-09-08, DB unique provider bars |
| derived 3m / 5m | 112,593 / 70,090 | 실제 1m만 집계, forward fill 없음 |
| event-selected daily rows | 30,270 | event별 선택 합계; DB unique 1d는 11,700 |
| PIT macro contexts | 2,020 | 202×10 series context rows |
| legacy impact rows | 8,080 | 202×10×4 legacy windows |
| legacy strategy rows | 2,020 | 계산 가능 1,988 |
| legacy mean after 10 bp | -0.15649% | 탐색 규칙 평균, 기대수익률 아님 |
| positive ratio | 39.336% | 미래 성과 아님 |

수집 완료와 봉 품질은 별도입니다. 2026-09-08 session collection 2,020건은 모두 정상 종료됐고, 실제 1m 관측 품질은 COMPLETE 581 / PARTIAL 1,409 / NO_MARKET_DATA 30입니다.

## 주요 엔지니어링 검증

### Raw SIP 부하와 식별자 교정

2026-08-31 GCP 대용량 실행은 7,360,804건을 Kafka 발행·소비·Spark 입력까지 전달하고 22,260개 bar를 저장했습니다. 당시 49건은 실제 시장 중복이 아니라 `exchange`를 빠뜨린 event identity 충돌이었습니다. 식별자에 exchange를 넣은 뒤 전체 archive 7,360,804건을 다시 검사·재실행했고 충돌·Spark duplicate·DB business-key duplicate가 모두 0이었습니다.

### 장애와 복구

PostgreSQL 컨테이너를 의도적으로 중단한 실행은 `OperationalError`로 실패하고 0행을 저장했습니다. DB를 복구한 뒤 같은 118,118건을 재처리해 472개 bar를 저장했고, 결과 hash와 최종 DB business-key duplicate 0을 확인했습니다. 이는 해당 실험 범위의 upsert/replay 복구 증거이지 Kafka 전역 exactly-once 보장은 아닙니다.

### 파티션 routing

과거 symbol-only GCP 실행의 최대 파티션 비중은 97.53%였습니다. 별도 로컬 118,118건 v2 routing 검증은 6개 파티션에서 최대 33.9229%였습니다. 서로 다른 환경·실행이므로 throughput 향상 비교로 쓰지 않고, bounded validation에서 skew가 감소했다는 말만 사용합니다.

### Airflow

네 DAG 모두 `schedule=None`, `catchup=False`, `max_active_runs=1`인 통제된 historical workflow입니다. 시장 DAG는 발표별 **202 mapped tasks**이며 각 task 내부 10종목을 처리해 **2,020 DB work items**를 남깁니다. 2,020 mapped tasks가 아닙니다. 2026-09-03 시장 실행은 522.660초, 거시 실행은 14.835초였습니다. 2026-09-08 재수집·재계산은 별도 실행입니다.

## Current / Foundation / Target

### Current

- 공식 일정, provider SIP bars, FRED/ALFRED context historical ingestion
- legacy 4-window event impact와 단순 탐색 전략
- pipeline telemetry의 시장 DAG 범위
- 분리된 Raw SIP Kafka/Spark validation sink
- FastAPI와 로컬 browser UI
- guarded manual Alpaca Paper submit/reconcile/cancel; accepted→canceled probe

### Foundation implemented, operational population 아님

- canonical four-event identity와 append-only lifecycle
- official observation ontology/revisions
- provider-local consensus와 PIT surprise semantics
- immutable calendar snapshots, trading sessions, marker revisions, S0/S+N planner
- quality/maturity/eligibility taxonomy와 Reaction-v2 typed contracts
- concurrency-safe immutable writes와 validation lineage

### Target only

- production official-observation·consensus adapters/backfill
- operational exchange-calendar ingestion과 plan persistence
- Reaction-v2 역사 재계산·서빙 전환
- macro regime, comparable-event engine
- registered hypothesis, proper time-split simulator, cost registry
- full Paper fills·positions·outcomes·risk controls와 experiment lineage
- automated research-to-order
- authenticated managed production deployment

## Capability matrix

| Capability | Current operational? | Foundation implemented? | Target only? | Runtime evidence | Presentation-safe wording |
| --- | --- | --- | --- | --- | --- |
| release schedule/events | Yes, legacy catalog | canonical identity/lifecycle | official change adapter | 202-event backfill | 공식 일정 202회를 사용했고 canonical lifecycle 기반을 추가했다 |
| official actual revisions | legacy scalar fields only | append-only observation ontology/revisions | production adapters/backfill | foundation DB tests only | revision-safe schema는 있지만 운영 수집은 다음 단계다 |
| consensus/surprise | No operational canonical path | provider-local PIT selector and immutable surprise lineage | historical provider ingestion/population | T01–T51 contract tests | 미래정보 누출을 막는 계약을 검증했다; 현재 전략 입력은 아니다 |
| research market bars | Yes, Alpaca provider SIP | source/feed/version isolation | stricter collection conflict policy | 308,512 1m rows | 분석용 provider bar 경로가 동작한다 |
| Raw SIP validation | Yes, bounded replay | immutable validation run/bar lineage | managed object storage/workers | 7.36M historical run; current sink tests | Raw SIP는 별도 검증 plane이다 |
| trading sessions/S0 | legacy release-relative only | snapshot/session/marker planner | operational calendar adapter/persistence | pure + PostgreSQL tests | session semantics 기반은 있으나 legacy 분석을 아직 대체하지 않았다 |
| reaction metrics | legacy four windows | typed Reaction-v2 contract | recomputation/backfill/serving | 8,080 legacy rows; v2 tests only | 현재 수치는 legacy v1, v2는 foundation이다 |
| quality/maturity | current collection/coverage fields | typed orthogonal taxonomy | metric-level persisted decisions | 2,020 collection + quality evidence | 수집 완료와 관측 품질을 분리했다 |
| macro context | Yes, FRED/ALFRED context | PIT rules | regime feature engine | 2,020 contexts | 설명 맥락으로 조회하며 예측 변수 모델은 아니다 |
| comparable events | No | deterministic outline/contract | computation and serving | no runtime output | target capability다 |
| strategy validation | one exploratory legacy rule | hypothesis/simulation contract | time split simulator/cost registry | 2,020 rows, negative mean | 약한 baseline을 기각한 탐색 결과다 |
| serving UI | local FastAPI/browser | resource boundary docs | authenticated managed deployment | current screenshots and API tests | 로컬 제품 화면에서 DB 결과를 읽는다 |
| Paper execution | guarded manual submit/reconcile/cancel | journal/idempotency boundary | fills/positions/outcomes/risk/automation | accepted→canceled, fill 0 | 수동 Paper 연결만 검증했으며 자동 주문은 없다 |
| operations | controlled historical DAGs | target contracts/audits | schedules, SLAs, full observability | dated Airflow runs | 수동 backfill workflow이며 상시 운영 스케줄은 아니다 |

## 발표에서 금지할 주장

- “수익성 있는/예측 가능한 투자 시스템을 완성했다.”
- “202×10 연구 데이터가 Kafka/Spark Raw SIP 경로를 통과했다.”
- “GCP에 프로덕션 배포했다.”
- “49개 실제 중복 거래를 제거했다.”
- “foundation schema가 production adapter로 채워져 운영 중이다.”
- “PIT consensus/surprise를 현재 legacy 전략이 사용한다.”
- “Paper 주문이 실제 체결됐거나 포지션 대사가 완료됐다.”
- “연구 신호가 자동으로 주문된다.”
- “2,020개의 Airflow mapped task를 실행했다.”
- “30,250개의 일봉을 최신 수치로 검증했다.”

## 결론 문장

> 이 프로젝트의 목표는 수익 전략을 포장하는 것이 아니라, 당시 알 수 있었던 정보와 실제 반응을 재현해 투자 가설을 확인하거나 기각할 수 있는 환경을 만드는 것입니다.
