# Documentation hub

현재 제품 계약과 과거 과제 증거를 분리합니다. 코드·migration·테스트와 아래 **현재 정본**이 우선이며, 날짜가 붙은 문서는 실행 당시 사실을 보존하는 archive입니다.

## 현재 정본

### Architecture

- [현재 시스템과 세 개 plane](architecture/current-system.md)
- [데이터 모델](data-model.md)
- [데이터 lifecycle](data-lifecycle.md)
- [데이터 출처 catalog](data-source-catalog.md)
- [Archify 구성도와 검증 receipt](diagrams/README.md)

### Engineering

- [플랫폼 감사와 위험](engineering/platform-audit.md)
- [API·source·version 계약](engineering/api-contracts.md)
- [설계 결정](design-decisions.md)

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

- [08.31 발표 대본](08.31_대본.md)
- [09.03 발표 대본](09.03_대본.md)
- [09.07 발표 대본](09.07_대본.md)
- [3차시 Kafka·Spark](kafka-spark-assignment.md)
- [Airflow 과제](airflow-assignment.md)
- [6차시 부하·복구](load-recovery-assignment.md)
- [7차시 서빙](session7-submission.md)
- [날짜별 test results](test-results/)
- [발표 자료](presentation/README.md)

## 문서 유지 규칙

- 현재 기능은 코드와 테스트가 있을 때만 현재형으로 표현합니다.
- 집계에는 source table, unit, source/feed/version을 함께 적습니다.
- `collection integrity`, `observed coverage`, `analysis eligibility`를 합치지 않습니다.
- 연구 결과와 Paper 주문 결과를 같은 성과로 표현하지 않습니다.
- 비밀키·account ID·원본 provider payload·DB dump는 커밋하지 않습니다.
