# Airflow 실행 계약 감사 — 2026-09-09

아래는 현재 코드에서 확인한 동작입니다. 이번 교정에서는 전체 Airflow backfill을 다시 실행하지 않았습니다. 과거 실행 증거와 현재 직접 재검증은 별도로 보존합니다.

## 공통 운영 경계

네 DAG 모두 수동 매개변수 기반 역사 데이터 처리입니다. `schedule=None`, `catchup=False`, `max_active_runs=1`은 의도된 설정입니다. 상시 실시간 스케줄이나 자동 증분 수집은 별도 향후 과제입니다. 재시도 간격은 고정 30초이며 지수 backoff가 아닙니다.

## 시장 데이터

정본: [market_context_backfill_pipeline.py](../../dags/market_context_backfill_pipeline.py).

- 입력: 발표 유형, 시작·종료 날짜, 종목, feed, provider cutoff. 설정을 검증하고 해시·코드 버전·cutoff를 run에 기록합니다.
- 의존성: `validate_run_config → register_run → build_work_items → collect_market_context[mapped] → verify_run → finish_run`.
- 매핑: 발표 한 건에 종목들을 묶습니다. 전체 범위는 202개 event task이며 DB work item은 202×10=2,020개입니다.
- 동시성: 수집은 `alpaca_api_pool`, `max_active_tis_per_dag=4`; 등록·검증·종료는 `postgres_write_pool`. 수집 내부 저장은 수집 task의 동시성 한도를 따르며 두 pool을 동시에 점유하지 않습니다.
- 재시도: 최초 시도 이후 최대 2회, 각 30초 간격.
- 저장: `economic_events`, `market_bars`와 `pipeline_runs`, `pipeline_work_items`, `pipeline_run_checks`. 데이터의 business key에 따른 Upsert를 사용합니다.
- 작업 상태: 먼저 `RUNNING`, 수집 정상 종료 후 `SUCCEEDED` 또는 `DATA_NOT_AVAILABLE`. 요청 완료와 관측 품질은 별도입니다. 정상 요청의 성긴 봉은 성공+품질 경고로 남깁니다.
- 예외: 수집 try 블록 안에서 발생한 오류는 DB가 동작할 때 해당 발표의 work item을 `FAILED`, collection 검사를 `FAIL/OPEN`, run을 `FAILED`로 저장한 뒤 다시 예외를 던집니다.
- 종료 검증: mapped 결과의 예상 작업 수와 DB accepted 상태 수를 비교하고 실패 작업·미해결 실패 알림을 검사합니다. 통과하면 `finish_run`이 성공으로 종료합니다.

### 보장하지 않는 부분

등록 전 오류, `RUNNING` 기록 자체의 실패, DB 접속 장애는 같은 DB에 실패 기록을 남길 수 없습니다. 모든 종료 경로를 처리하는 DAG-wide `all_done` finalizer는 없습니다. 따라서 중단된 실행이 DB에서 `RUNNING`으로 남을 수 있으며 Airflow task 상태·로그를 함께 봐야 합니다. 재시도 중에는 이전 시도의 run 실패가 최종 성공까지 표시될 수도 있습니다. DB 기록만으로 모든 Airflow scheduler 상태를 추론하지 않습니다.

## 발표 시점 거시 환경

정본: [macro_context_backfill_pipeline.py](../../dags/macro_context_backfill_pipeline.py).

- 입력: 발표 범위, series, cutoff, force refresh.
- 의존성: `build_event_work_items → collect_event_macro_context[mapped] → verify_macro_context`.
- 매핑: 발표별 task. 기존 context가 요청 series 수와 일치하고 강제 갱신이 아니면 재사용합니다.
- 동시성: `fred_api_pool`, mapped task 최대 1개, 활성 run 1개.
- 재시도: 최대 2회, 고정 30초.
- 저장: `economic_events`, `macro_event_contexts`; PIT cutoff와 provider vintage를 보존합니다.
- 최종 검증: 신규 저장+재사용 수가 발표 수×series 수와 일치하는지 검사합니다.
- 실패·종료: Airflow task 상태와 로그. 시장 DAG와 같은 `pipeline_*` telemetry를 생성하지 않습니다. 캐시 재사용은 건수 기반이며 선택 알고리즘의 독립 버전 key는 향후 개선 항목입니다.

## 원시 SIP 재생 검증

정본 파일: [market_replay_pipeline.py](../../dags/market_replay_pipeline.py). 실제 DAG ID는 **`market_sip_replay_pipeline`**입니다.

- 입력: 제한된 archive/replay 범위와 symbol별 설정. 공식 연구 전체 202×10을 raw trade로 재수집하는 경로가 아닙니다.
- 의존성: `validate_run_config → replay_trades_to_kafka → verify_kafka_delivery → build_minute_bars_with_spark → verify_stored_result`.
- 매핑: 검증된 symbol별 설정과 그 단계 결과를 후속 task로 전달합니다.
- 재시도: 최대 1회, 고정 30초. 별도 provider pool 또는 mapped task 한도는 선언되지 않았으며 활성 run 1개와 Airflow 환경 한도를 따릅니다.
- 저장·검증: Kafka 발행·수신 건수/trace, Spark 유효성·중복·집계, PostgreSQL 결과를 확인합니다. 재생 중복은 event identity 및 sink business-key Upsert로 제어합니다. Kafka의 전역 exactly-once를 주장하지 않습니다.
- 실패·종료: Airflow 상태, XCom 및 `pipeline_summary` 로그. 별도 시장 `pipeline_*` 레코드는 생성하지 않습니다.

## 다년 실행 조정

정본: [market_context_backfill_orchestrator.py](../../dags/market_context_backfill_orchestrator.py).

- 입력: 다년 날짜 범위와 시장 수집 설정.
- 의존성: `build_yearly_runs → run_market_context_year[mapped TriggerDagRunOperator]`.
- 각 연도 설정을 검증하고 market child run을 생성합니다. `wait_for_completion=True`, 30초 polling으로 child 성공/실패를 기다립니다.
- 별도 retry/pool 선언은 없습니다. parent와 child 각각 `max_active_runs=1`; child의 수집 task가 위 pool·동시성 한도를 적용합니다.
- 자체 시장 테이블은 쓰지 않습니다. 결과·추적은 child run과 Airflow metadata에 남습니다.

## 증거 구분

- [과거 전체 Airflow 실행](../airflow-assignment.md): 당시 DAG 실행·소요 시간 기록.
- [확장·재검증 증거](../evidence/multi-event-expansion/README.md): 데이터 규모와 별도 재수집 근거.
- [부하·장애·복구](../evidence/load-recovery/README.md): raw SIP 별도 실험.
- [이번 교정 감사](corrective-pass.md): 현재 테스트, 재현한 결함, 재실행하지 않은 범위.

Pipelines는 PostgreSQL에 실제 저장된 run만 조회합니다. logical lineage는 프로젝트 수준 연결도이며 실시간 task tracing이나 행별 lineage가 아닙니다.
