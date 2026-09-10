# Presentation claims ledger

`CURRENT`는 현재 코드/제품, `HISTORICAL`은 날짜가 고정된 실행, `FOUNDATION`은 테스트된 기반이지만 운영 데이터 경로가 아닌 상태, `TARGET`은 미구현 목표입니다.

| Claim | Status | Evidence | Evidence date | Safe wording | Unsafe wording |
| --- | --- | --- | --- | --- | --- |
| 개인 투자 판단을 돕기 위해 시작 | OWNER MOTIVATION | [presentation truth](PRESENTATION_TRUTH.md) | 2026-09-10 | 당시 데이터와 가격 반응을 직접 검증하고 싶었다 | 특정 증권사 AI가 틀렸다 |
| 202 releases | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | CPI 55·고용 55·PCE 55·FOMC 37 | 모든 미래 발표까지 자동 운영 |
| 10 symbols, 2,020 pairs | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | 202×10 research scope | 2,020 Airflow mapped tasks |
| 308,512 1m bars | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | DB unique Alpaca SIP 1m research bars | 736만 raw trades와 합산한 총 데이터 |
| 112,593 / 70,090 derived bars | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | observed 1m만 집계, PARTIAL 보존 | 결측 가격을 채워 생성 |
| daily selected 30,270 / unique 11,700 | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | event별 선택 합계 / DB unique rows를 분리 | 최신 일봉 합계 30,250 |
| PIT macro contexts 2,020 | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | 발표 시점 이용 가능 거시 맥락 | 발표 자체의 consensus/actual/surprise |
| 8,080 impact rows | HISTORICAL CURRENT DATASET | [event analysis](../../evidence/multi-event-expansion/event-analysis.json) | 2026-09-08 | legacy 202×10×4 windows | Reaction-v2 결과 |
| strategy 2,020 / calculable 1,988 | HISTORICAL CURRENT DATASET | [event analysis](../../evidence/multi-event-expansion/event-analysis.json) | 2026-09-08 | 단순 pre60 momentum legacy observations | 검증된 거래 전략 |
| mean -0.15649%, positive 39.336% | HISTORICAL CURRENT DATASET | [event analysis](../../evidence/multi-event-expansion/event-analysis.json) | 2026-09-08 | 10 bp 비용 후 과거 관측 평균; 가설 기각 사례 | 미래 기대수익률 또는 portfolio return |
| collection complete 2,020 | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | provider request lifecycle 정상 종료 | 모든 분의 가격이 존재 |
| 581/1,409/30 quality | HISTORICAL CURRENT DATASET | [full expansion](../../evidence/multi-event-expansion/full-expansion-summary.json) | 2026-09-08 | COMPLETE/PARTIAL/NO_MARKET_DATA | 1,439건 수집 실패 |
| GCP Raw SIP 7,360,804 | HISTORICAL EXPERIMENT | [load results](../../evidence/load-recovery/results.json) | 2026-08-31 | GCP Compute Engine 부하 검증 | GCP production traffic |
| GCP large runtime 1,690.250158 sec | HISTORICAL EXPERIMENT | [load results](../../evidence/load-recovery/results.json) | 2026-08-31 | 해당 e2-standard-4 실험 소요 시간 | 로컬 v2보다 느리므로 개선됨 |
| 49 apparent duplicates | HISTORICAL INCIDENT | [identity correction](../../evidence/pipeline-review/event-identity-correction.json) | 2026-09-01 | exchange 누락 identity가 만든 false-positive collision | 실제 중복 시장 거래 49건 |
| corrected 7.36M duplicates 0 | HISTORICAL CORRECTION | [identity correction](../../evidence/pipeline-review/event-identity-correction.json) | 2026-09-01 | exchange 포함 후 전체 archive와 pipeline 재검증 0 | 모든 종류의 중복을 영구 보장 |
| DB failure and recovery | HISTORICAL EXPERIMENT | [load results](../../evidence/load-recovery/results.json) | 2026-08-31 | DB 중단 실패 뒤 동일 118,118건 복구·472 bars·duplicate 0 | 전체 시스템 exactly-once |
| partition max 97.53% → 33.9229% | TWO HISTORICAL EXPERIMENTS | [GCP v1](../../evidence/load-recovery/results.json), [local v2](../../evidence/load-recovery/v2-partition-routing.json) | 2026-08-31 / 09-03 | bounded validation에서 routing skew 감소 | 동일 환경 성능 비교 |
| Airflow 202 mapped tasks | HISTORICAL RUN + CURRENT CODE | [Airflow audit](../../engineering/airflow-audit.md), [run](../../evidence/multi-event-expansion/airflow-full-run.json) | 2026-09-03 / 09-09 | event task 202개, 내부 10 symbols | mapped task 2,020개 |
| market / macro 522.660s / 14.835s | HISTORICAL RUN | [Airflow run](../../evidence/multi-event-expansion/airflow-full-run.json) | 2026-09-03 | 서로 다른 DAG의 기록된 runtime | 상시 스케줄 SLA |
| browser serving exists | CURRENT | [current system](../../architecture/current-system.md), [current screenshot](../../evidence/corrective-pass/overview.png) | 2026-09-10 | local FastAPI가 PostgreSQL 결과를 조회 | managed public production service |
| manual Alpaca Paper exists | CURRENT + HISTORICAL PROBE | [contract/evidence](../../paper-execution.md), [probe](../../evidence/paper-execution/actual-paper-probe.json) | 2026-09-07 | 1 Paper order accepted→canceled, fill 0 | 체결·수익·포지션 대사 완료 |
| canonical lifecycle/PIT/session/reaction contracts | FOUNDATION | [current vs target](../../engineering/current-vs-target.md), [verification](../../evidence/architecture-pass-2026-09-10/verification.json) | 2026-09-10 | migration 009와 pure contracts/tests가 foundation 제공 | production adapters와 v2 backfill 운영 중 |
| 51 semantic counterexamples | FOUNDATION VERIFICATION | [verification](../../evidence/architecture-pass-2026-09-10/verification.json) | 2026-09-10 | T01–T51 green | target feature 51개 완료 |
| 318 Python / 46 PG / 6 UI | FOUNDATION VERIFICATION | [verification](../../evidence/architecture-pass-2026-09-10/verification.json) | 2026-09-10 | Python 263 pass+55 skip, PG 46 pass, UI 6 pass | skip 55까지 실행·통과 |
| merge commit CI success | CURRENT REPOSITORY | [GitHub run](https://github.com/hjh6709/us-market-intelligence-pipeline/actions/runs/34478926754) | 2026-09-10 | merge commit `56334a8` push CI success | 운영 환경 검증 완료 |
| target platform diagram | TARGET | [target diagram](../../diagrams/target-platform.html) | 2026-09-10 | approved target, tags와 matrix를 함께 읽음 | 전체 노드 현재 구현 완료 |
