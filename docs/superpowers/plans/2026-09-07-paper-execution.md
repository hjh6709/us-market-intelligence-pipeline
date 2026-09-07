# 모의주문 연결 실행 계획

1. 실패 테스트: endpoint 고정, 재전송 금지, 요청 동일성, 잘못된 응답.
2. 주문 모델·Alpaca paper 어댑터·PostgreSQL journal·실행 서비스 구현.
3. 별도 CLI: submit / reconcile / cancel. 자동 전략 주문 경로는 연결하지 않는다.
4. 격리된 PostgreSQL schema와 mock HTTP로 실제 journal 복구·동시 실행 검증.
5. 외부 paper 인증 준비 여부 확인, 실행한 것과 못한 것을 기록.
6. README·후속 설계·증거 문서 갱신, 회귀 테스트·PR·CI·병합.
