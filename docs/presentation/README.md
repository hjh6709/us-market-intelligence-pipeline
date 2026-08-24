# 과제 발표 자료

- 발표 자료: `us-market-pipeline-assignment.pptx`
- 예상 질문: [발표 Q&A](../presentation-qa.md)
- 실행 증거: [CPI Kafka·Spark 실행 증거](../evidence/cpi-kafka-spark/README.md)
- 과제 아키텍처 원본: [cpi-sip-kafka-spark-assignment.svg](../diagrams/cpi-sip-kafka-spark-assignment.svg)
- 전체 프로젝트 아키텍처: [pipeline-architecture.svg](../diagrams/pipeline-architecture.svg)

슬라이드는 다음 순서로 구성했다.

1. 프로젝트 목표
2. 실제 사용하는 데이터와 역할
3. CPI 발표 시각 → SIP raw trade → Kafka → Spark → PostgreSQL → 증거 흐름
4. Kafka·Spark·PostgreSQL 선택 이유
5. 실제 58,036건 전송과 121개 1분봉 저장 결과
6. Producer·Consumer·Spark·DB 교차 검증 결과
7. 현재 완료 범위, 다음 단계, 멘토 피드백 질문

발표할 때는 아키텍처를 전부 읽지 말고 데이터가 왼쪽에서 오른쪽으로 흐르는 과정만 설명한다. `실시간 IEX WebSocket → Kafka`는 선행 검증이고, 이번 제출 결과는 `Historical SIP raw trade → Kafka → Spark batch → PostgreSQL`임을 구분해 말한다. 발표 대본은 공개 저장소에 포함하지 않고 PPT speaker notes와 로컬 전용 파일로 관리한다.
