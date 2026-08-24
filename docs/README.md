# 프로젝트 문서 안내

README에는 과제 설명과 실행에 필요한 핵심만 둔다. 세부 계약, 검증 결과와 장기 아이디어는 목적별로 아래 문서에서 관리한다.

## 현재 구현과 설계

| 문서 | 역할 |
| --- | --- |
| [architecture.md](architecture.md) | 현재 파이프라인 경계, 데이터 흐름과 장애 처리 |
| [diagrams/](diagrams/README.md) | 아키텍처 SVG 정본과 README용 PNG |
| [data-source-catalog.md](data-source-catalog.md) | API별 제공 값과 프로젝트 사용 범위 |
| [data-model.md](data-model.md) | Kafka event envelope와 PostgreSQL 논리 모델 |
| [data-lifecycle.md](data-lifecycle.md) | 수집량, 저장 위치, 활용과 보존 기간 |
| [design-decisions.md](design-decisions.md) | Kafka·Spark·PostgreSQL·Airflow 선택 근거 |
| [course-alignment.md](course-alignment.md) | 과정 기술과 구현·증거의 연결 |
| [api-selection.md](api-selection.md) | 무료 API 비교와 Alpaca 선택 근거 |
| [kafka-spark-assignment.md](kafka-spark-assignment.md) | CPI 발표 구간을 재현한 Kafka·Spark 과제 실행법·증거 |

4주 일정과 회차별 완료 기준은 [PROJECT_PLAN.md](../PROJECT_PLAN.md)에 있다.

## 실행 증거

| 위치 | 내용 |
| --- | --- |
| [test-results/](test-results/) | 실행 날짜별 smoke·통합 테스트 보고서 |
| [3차시 Kafka·Spark 결과](test-results/2026-08-21-kafka-spark-assignment.md) | Producer·Consumer 427건 대조와 Spark 전처리·저장 결과 |
| [4차시 CPI 구간 Kafka·Spark 결과](test-results/2026-08-24-cpi-kafka-spark.md) | CPI 발표 구간 NVDA SIP 원시 체결 58,036건의 전송·전처리·저장 결과 |
| [100x replay 파일럿](test-results/2026-08-24-replay-load-100x.md) | 실제 거래 1,523건의 배속 replay와 Kafka·Spark·DB 결과 |
| [CPI SIP backfill](test-results/2026-08-24-cpi-sip-backfill.md) | CPI 12회 × 4종목의 Historical SIP 적재·coverage 결과 |
| [CPI event impact](test-results/2026-08-24-cpi-event-impact.md) | 발표 전후 4개 window 계산·benchmark·한계 |
| [CPI matched baseline](test-results/2026-08-24-cpi-matched-baseline.md) | 발표 1·2·3주 전 동일 요일·동부시각 비교 결과 |
| [evidence/actual-ingestion/](evidence/actual-ingestion/README.md) | 실제 Historical 거래 수집·저장 수치와 재현 절차 |
| [evidence/postgres-market-bars/](evidence/postgres-market-bars/README.md) | DB 중복 방지와 장애 복구 증거 |
| [evidence/presentation-captures/](evidence/presentation-captures/README.md) | 비밀정보를 제거한 발표용 캡처 6종 |
| [submission-checklist.md](submission-checklist.md) | 과제 요구사항별 완료 상태와 발표 직전 확인 순서 |
| [../data/README.md](../data/README.md) | 공개 합성 샘플과 로컬 실제 데이터의 구분 |

## 발표 자료

- [발표 자료와 사용 순서](presentation/README.md)
- [4분 발표 대본](presentation-script.md)
- [예상 질문과 짧은 답변](presentation-qa.md)

## 장기 방향

[final-vision.md](final-vision.md)는 현재 과제의 완료 약속이 아니라, 경제지표 영향 검증 이후 Agent·MCP·RAG·위험 관리·자동매매로 확장할 때 참고하는 장기 목표다.

## 문서 유지 기준

- 구현 계약은 한 문서만 정본으로 두고 다른 문서에서는 링크한다.
- 실행하지 않은 기능을 완료된 것처럼 적지 않는다.
- 날짜별 실행 결과는 `test-results/`, 공개 가능한 집계 증거는 `evidence/`에 둔다.
- 임시 구현 계획, 프롬프트 산출물, 중복 초안과 렌더 중간 파일은 Git에 남기지 않는다.
- `.env`, API key, connection URL, 원본 응답과 DB dump는 문서나 이미지에 포함하지 않는다.
