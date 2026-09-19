# Documentation hub

현재 구현, CPI W1 목표 계약, 과거 설계와 실행 증거를 분리합니다. 먼저
[Architecture Authority Index](architecture/AUTHORITY.md)에서 관심사별 권위를
확인합니다. 날짜가 붙은 문서는 해당 시점의 사실을 보존할 뿐 현재 구현이나
새 CPI W1 의미를 자동으로 정의하지 않습니다.

## 권위 라우팅

- `CURRENT_IMPLEMENTATION`: 현재 checkout의 코드·migration·테스트
- `TARGET_CANONICAL`: [2026-09-19 CPI W1 Data & Governance 설계](superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md)
- `HISTORICAL_SUPERSEDED`: migration 009 및 2026-09-10 target 문서
- `STATUS_LEDGER`: [현재/목표 진행표](engineering/current-vs-target.md)

## 현재 구현과 호환성 문서

### Architecture

- [과거 플랫폼 target 계약](architecture/platform-contract.md) (`HISTORICAL_SUPERSEDED`)
- [검증된 현재 시스템 snapshot](architecture/current-system.md)
- [migration 009 데이터·품질·lineage 계약](architecture/data-contracts.md) (`HISTORICAL_SUPERSEDED`)
- [연구·검증 계약](architecture/research-contracts.md)
- [과거 운영·저장·배포 target](architecture/operations-and-deployment.md) (`HISTORICAL_SUPERSEDED`)
- [현재와 target 상태 ledger](engineering/current-vs-target.md)
- [데이터 출처 catalog](data-source-catalog.md)
- [Archify 구성도와 검증 receipt](diagrams/README.md)
- [설정 경계와 current/target 구분](configuration/README.md)
- [ADR 0001: additive event/session foundation](adr/0001-additive-event-session-foundation.md)

### Engineering

- [2026-09-10 baseline audit](engineering/baseline-audit-2026-09-10.md)
- [2026-09-10 contradiction self-review](engineering/contradiction-review-2026-09-10.md)
- [2026-09-10 selected P0/P1 scope](engineering/implementation-scope-2026-09-10.md)
- [2026-09-10 architecture pass verification](evidence/architecture-pass-2026-09-10/README.md)
- [2026-09-10 presentation-ready facts](engineering/presentation-ready-summary-2026-09-10.md)
- [최종 교정 진행 기록](engineering/corrective-pass.md)
- [Airflow DAG별 계약·실패 경계](engineering/airflow-audit.md)
- [과정 요구사항별 구현·검증 위치](engineering/course-acceptance.md)
- [플랫폼 감사와 위험](engineering/platform-audit.md)
- [API·source·version 계약](engineering/api-contracts.md)

### Research

- [연구 방법·coverage·baseline 해석](research/methodology.md)
- [다중 이벤트 확장 증거](evidence/multi-event-expansion/README.md)

### Execution

- [웹 Paper sandbox와 안전 경계](execution/paper-sandbox.md)
- [Paper 주문 구현·drill 증거](paper-execution.md)

### Serving and demonstration

- [서빙 레이어 과제·실행 결과](serving-layer-assignment.md)
- [입력 → 처리 → 저장 → 읽기 증거](evidence/serving-layer/README.md)
- [세션 7 제출 영상·캡처](evidence/session7-demo/README.md)
- [최종 자동·통합 검증](evidence/final-portfolio/verification.md) · [브라우저 검증](evidence/final-portfolio/browser-validation.md)

## 과정 아카이브

아래 문서는 삭제하지 않습니다. 당시 실행 범위·숫자·판단을 재현하기 위한 역사 기록이며, 현재 제품 계약으로 읽으면 안 됩니다.

- [08.31 발표 대본](archive/presentations/08.31_대본.md)
- [09.03 발표 대본](archive/presentations/09.03_대본.md)
- [당시 4주 실행 계획](archive/project-history/PROJECT_PLAN.md)
- [09.07 발표 대본](09.07_대본.md)
- [3차시 Kafka·Spark](kafka-spark-assignment.md)
- [Airflow 과제](airflow-assignment.md)
- [6차시 부하·복구](load-recovery-assignment.md)
- [7차시 서빙](session7-submission.md)
- [날짜별 test results](test-results/)
- [발표 자료](presentation/README.md)
- [이전 MVP architecture](architecture.md)
- [이전 final vision](final-vision.md)
- [이전 혼합 data model](data-model.md)
- [이전 lifecycle policy](data-lifecycle.md)
- [이전 MVP 설계 결정](design-decisions.md)
- [4차시 발표 대본](presentation-script.md)

## 문서 유지 규칙

- 현재 기능은 코드와 테스트가 있을 때만 현재형으로 표현합니다.
- target contract는 구현 완료의 증거가 아니며, 현재 상태는 `architecture/current-system.md`와 `engineering/current-vs-target.md`로 확인합니다.
- 집계에는 source table, unit, source/feed/version을 함께 적습니다.
- `collection integrity`, `observed coverage`, `analysis eligibility`를 합치지 않습니다.
- 연구 결과와 Paper 주문 결과를 같은 성과로 표현하지 않습니다.
- 비밀키·account ID·원본 provider payload·DB dump는 커밋하지 않습니다.
