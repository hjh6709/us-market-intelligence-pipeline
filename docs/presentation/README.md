# 과제 발표 자료

- 발표 자료: `us-market-pipeline-assignment.pptx`
- 발표 대본: [4분 발표 대본](../presentation-script.md)
- 예상 질문: [발표 Q&A](../presentation-qa.md)
- 실행 증거: [발표용 캡처 6종](../evidence/presentation-captures/README.md)
- 아키텍처 원본: [pipeline-architecture.svg](../diagrams/pipeline-architecture.svg)

슬라이드는 다음 순서로 구성했다.

1. 프로젝트 목표
2. 실제 사용하는 데이터와 역할
3. Source → Ingestion → Raw → Processing → Storage → Analysis 전체 흐름
4. Kafka·Spark·PostgreSQL 선택 이유
5. 실제 ingestion과 저장 결과
6. 자동 테스트와 장애 복구 결과
7. 현재 완료 범위, 다음 단계, 멘토 피드백 질문

발표할 때는 아키텍처를 전부 읽지 말고 데이터가 왼쪽에서 오른쪽으로 흐르는 과정만 설명한다. 구현 결과는 `실시간 WebSocket → Kafka`와 `Historical 실제 데이터 → PostgreSQL`을 구분해 말한다.
