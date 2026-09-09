# 과정 요구사항 → 구현·검증 근거

정체성은 데이터 엔지니어링 플랫폼입니다. 서빙은 저장 결과의 소비 경로이고 Paper는 별도 수동 sandbox입니다. 이 표는 과거 증거와 현재 코드의 위치를 연결하며 모든 대용량 실험을 이번에 재실행했다는 뜻이 아닙니다.

| 차시 | 확인할 구현 | 검증·발표 근거 |
| --- | --- | --- |
| 2 데이터·설계 | [공식 일정](../../src/economic_event_schedule.py), [시장 구간](../../src/market_event_context.py), [PIT 수집](../../src/macro_context_ingestion.py), [데이터 모델](../data-model.md) | 공식 UTC 발표 시각, source/feed, vintage, business key와 결측 의미 |
| 3 Kafka | [event envelope](../../src/market_event.py), [producer](../../src/kafka_publisher.py), [archive replay](../../src/archive_kafka_replay.py) | [routing-v2 결과](../evidence/load-recovery/v2-partition-routing.json), [producer 통합 테스트](../../tests/integration/test_kafka_market_producer.py) |
| 4 Spark | [입력 스키마](../../src/spark_schemas.py), [stream processor](../../src/spark_market_processor.py), [SIP batch](../../src/spark_sip_trade_batch.py) | 검증·중복 제거·거래 조건 적용·1분 집계, [checkpoint/restart 테스트](../../tests/integration/test_spark_market_processor.py) |
| 5 Airflow | [네 DAG별 감사](airflow-audit.md), [시장 DAG](../../dags/market_context_backfill_pipeline.py) | 발표별 매핑, 고정 30초 재시도, pool·동시성, DB 상태와 최종 검증 |
| 6 부하·장애 | [원본 실험 기록](../evidence/load-recovery/README.md) | raw 7,360,804건, 식별 충돌 49→0, DISK_ONLY, DB 중단 후 Upsert 복구; routing 118,118건은 별도 |
| 7 서빙 | [API](../../src/serving_api.py), [repository](../../src/serving_repository.py), [실행 demo](../../scripts/run_serving_demo.py) | [저장 후 읽기 증거](../evidence/serving-layer/README.md), Overview·Research·Pipelines, 격리된 Paper |
| 8 최종 시연 | [현재 시스템](../architecture/current-system.md), [API 계약](api-contracts.md) | 구성도 → Airflow run 상세 → 작업·품질 → 저장된 발표 → 공식 시각·시장 반응 → 과거 비교 → 별도 Paper 경계 |

## 심사 질문에 대한 확인 위치

1. Kafka 입력·키: `market_event.py`가 정규화한 envelope, `kafka_publisher.py`가 topic·routing·acks/idempotence를 다룹니다. 거래소가 다른 동일 번호 체결을 분리합니다. 6-partition v2 개선은 118,118건 시험 범위입니다.
2. Spark 입출력: raw event schema를 검증하고 유효 체결을 event-time 1분 OHLCV로 집계합니다. 스키마와 sink 매핑은 위 Spark 코드 및 [수직 통합 테스트](../../tests/integration/test_kafka_spark_postgres.py)에서 함께 확인합니다.
3. 저장: [migration](../../db/migrations/)과 [데이터 모델](../data-model.md)이 PK/UK/FK의 정본입니다. 시장 봉의 source/feed/symbol/timeframe/window/session identity와 Upsert가 재실행 중복을 막습니다. 분석은 canonical source/feed/version으로 읽습니다.
4. Airflow: 시장 데이터 DAG가 주요 historical workflow를 조정합니다. 202개 mapped event batch 내부에서 10종목을 처리하며 2,020개 DB work item을 남깁니다. 실패 시 DB 기록은 DB 접근이 가능한 경우에만 보장됩니다.
5. 검증: [이번 감사](corrective-pass.md)에 격리 PostgreSQL·Kafka·Spark 테스트 결과를 기록합니다. 기존 연구 DB를 truncate하거나 부하 실험을 새 결과로 포장하지 않습니다.
6. 소비: 저장한 결과는 FastAPI repository/service를 통해 읽습니다. Pipelines의 run 상세는 코드 버전·cutoff·설정 해시와 전체 수/반환 페이지를 구분합니다. 없는 값은 0이 아니라 미기록입니다.

## 발표의 안전한 범위

실시간 시연은 저장 결과 조회와 run 상세 탐색을 1–2분 안에 수행합니다. 대용량 replay·외부 API 재수집·장애 재현은 기존 캡처와 원본 로그를 사용합니다. 단일 명령 demo는 저장된 한 발표·한 종목을 입력으로 재계산·Upsert·재조회하는 범위이며 외부 원천 수집 전체를 실행하는 명령은 아닙니다.

음수 연구 baseline을 개선된 수익 전략으로 소개하지 않습니다. Paper 접수·취소 증거는 분석 성과나 실제 모의체결 후 포지션 복구의 증거가 아닙니다.
