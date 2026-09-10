# Presentation asset map

최종 deck은 16:9를 기준으로 하고, 긴 full-page 캡처는 그대로 축소하지 않습니다. `MAIN`은 본문 추천, `BACKUP`은 부록/대체, `HISTORICAL`은 날짜·범위를 화면에 표시할 때만 사용, `REJECT`는 최종 deck에서 사용하지 않습니다.

| Asset | What it proves | Freshness / conflict | Recommended slide | Crop guidance | Decision |
| --- | --- | --- | --- | --- | --- |
| [CURRENT_ARCHITECTURE.mmd](CURRENT_ARCHITECTURE.mmd) | 현재 세 plane과 storage isolation | 2026-09-10 current truth | 3 | 벡터로 재렌더, plane 제목·invariant 유지 | MAIN |
| [target-platform HTML](../../diagrams/target-platform.html) | approved destination과 foundation 위치 | target; current 증거 아님 | 8 또는 backup | viewer chrome 제외, TARGET 라벨 상시 표시 | MAIN AS TARGET |
| [target light render](../../diagrams/target-platform.visual-check.1440x900.light.png) | target diagram render | current로 쓰면 오해 | 8 | 상하 viewer UI를 잘라 diagram만 | BACKUP/TARGET |
| [corrective overview](../../evidence/corrective-pass/overview.png) | 현재 DB 기반 Overview와 음수 baseline | 2026-09-10 current | 9 | browser chrome 최소화, KPI+음수 결과 포함 | MAIN |
| [corrective research](../../evidence/corrective-pass/research.png) | marker, bars, research-only 신호 | 2026-09-10 current | 9 | chart·RESEARCH_ONLY/NO_TRADE 중심 | MAIN |
| [pipeline lineage](../../evidence/corrective-pass/pipeline-lineage.png) | research vs validation 경계 설명 | 2026-09-10 current, 화면 일부 | 3 또는 9 | lineage 설명과 Kafka/Spark 경계 중심 | MAIN/BACKUP |
| [pipeline detail](../../evidence/corrective-pass/pipeline-detail.png) | 202 tasks/2,020 work items historical run detail | current UI + historical run | 5 또는 backup | run header와 work item/quality 영역 | BACKUP |
| [paper disabled](../../evidence/corrective-pass/paper-disabled.png) | Paper write-disabled 안전 기본값 | 2026-09-10 current | 9 또는 backup | disabled banner와 검토 폼 중심 | BACKUP |
| [serving live 09-08](../../evidence/serving-layer/dashboard-cpi-nvda-live-20260908.png) | PostgreSQL 결과를 실제 browser에서 읽음 | 2026-09-08, current-enough evidence | 9 | 전체 16:9 사용 또는 chart+read-only badge | MAIN |
| [portfolio overview](../../images/portfolio/overview.jpg) | Overview 전체 페이지 | 오래된 영문·세로 중복 | backup | 상단 16:9만, 최신 캡처 우선 | REJECT |
| [portfolio research](../../images/portfolio/research.jpg) | Research 전체 페이지 | 세로 중복·혼합 언어 | backup | 사용 시 chart 한 구역만 | REJECT |
| [portfolio pipelines](../../images/portfolio/pipelines.jpg) | Pipelines UI | 영문 이전 UI | backup | run summary만 | BACKUP |
| [portfolio paper](../../images/portfolio/paper-execution.jpg) | Paper UI | 세로 중복·영문 이전 UI | backup | 사용하지 말고 current disabled 사용 | REJECT |
| [load failure/recovery](../../evidence/load-recovery/02-failure-and-recovery.png) | DB 중단→복구→integrity | 2026-08-31 historical GCP | 7 | 중앙 flow와 final duplicate 0을 16:9로 crop | MAIN/HISTORICAL |
| [load data scope](../../evidence/load-recovery/03-data-scope-and-integrity.png) | GCP 환경·데이터 범위·VM 삭제 | 2026-08-31 historical | 5 또는 backup | machine/dataset/VM-deleted 영역 | MAIN/HISTORICAL |
| [load baseline vs load](../../evidence/load-recovery/01-baseline-vs-load.png) | 118,118/7.36M 원 실행 | 49가 교정 전 결과라 단독 사용 시 오해 | 5/6 backup | 49 영역을 쓰려면 반드시 “false positive” 교정 overlay | BACKUP/HISTORICAL |
| [CPI Raw SIP diagram](../../diagrams/cpi-sip-kafka-spark-assignment.png) | 한 CPI/NVDA Raw SIP replay의 bounded flow | 2026-08 course evidence; current validation sink 이전 | backup | 제목·58,036→121 flow만; storage 명칭은 historical 표시 | HISTORICAL/BACKUP |
| [Airflow run A](../../evidence/airflow-market-replay/airflow-run-a-four-symbols.png) | raw replay DAG 성공 화면 | 2026-08-27 historical | backup | DAG ID, success, mapped rows | BACKUP/HISTORICAL |
| [Airflow run B](../../evidence/airflow-market-replay/airflow-run-b-changed-input.png) | parameterized changed input | 2026-08-27 historical | backup | config JSON과 success만 | BACKUP/HISTORICAL |
| [services/raw storage](../../evidence/presentation-captures/01_services_and_raw_storage.png) | Kafka/PG healthy와 offsets | 2026-08-20 historical, sanitized | backup | 그대로 16:9 | BACKUP/HISTORICAL |
| [live websocket](../../evidence/presentation-captures/02_live_websocket_kafka.png) | IEX WebSocket→Kafka smoke 10건 | 별도 작은 historical route | backup | 그대로, research 202×10과 분리 라벨 | BACKUP/HISTORICAL |
| [actual ingestion](../../evidence/presentation-captures/03_actual_ingestion_e2e.png) | 427 trades→3 finalized bars | 작은 historical smoke | backup | 그대로 | BACKUP/HISTORICAL |
| [PostgreSQL storage](../../evidence/presentation-captures/04_postgres_storage.png) | historical Spark bar SQL read | 현재 validation sink 이전 shared market_bars | backup에서만 historical | 제목에 “당시 구현” 표시 | REJECT FOR MAIN |
| [automated tests old](../../evidence/presentation-captures/05_automated_tests.png) | 45+2 tests 당시 상태 | PR36 318/46/6보다 오래됨 | none | 사용하지 않음 | REJECT |
| [database recovery old](../../evidence/presentation-captures/06_database_recovery.png) | 작은 local DB rollback/retry | newer GCP evidence 존재 | backup | newer recovery asset 우선 | REJECT |
| [dashboard.png](../../evidence/serving-layer/dashboard.png) | 초기 Macro Pulse serving | obsolete product name·old readiness | none | 사용하지 않음 | REJECT |
| [dashboard 09-07](../../evidence/serving-layer/dashboard-cpi-nvda-20260907.png) | 초기 long-page dashboard | obsolete Macro Pulse·세로 | none | 사용하지 않음 | REJECT |
| [session7 architecture](../../diagrams/session7-architecture.html) | 7차시 당시 설명 | Research와 validation PG 경계가 현재보다 느슨함 | archive only | current diagram으로 사용 금지 | REJECT/HISTORICAL |
| [pipeline architecture](../../diagrams/pipeline-architecture.png) | 과거 전체 과정 개요 | daily 30,250, shared market_bars 등 stale | archive only | 사용하지 않음 | REJECT |
| [session7 demo video](../../evidence/session7-demo/session7-submission-demo.mp4) | 당시 serving demo | 7차시 historical, current architecture 전부 증명하지 않음 | backup/video appendix | 70초 원본 그대로, historical label | BACKUP/HISTORICAL |

## 다음 deck 제작 턴에서 새로 만들어야 할 시각물

- `CURRENT_ARCHITECTURE.mmd`를 16:9 slide-safe SVG로 렌더한 current-only 구성도
- 49 false-positive 사건의 `관측 → 원인 → 수정 → 7.36M 재검증` 단일 흐름 그림
- 202/10/2,020/308,512를 표현하는 단순 scale chart
- Current / Foundation / Target 3열 capability strip
- PR #36 verification(51, 318, 46, 6, CI green)을 과장 없이 보여주는 작은 evidence footer
