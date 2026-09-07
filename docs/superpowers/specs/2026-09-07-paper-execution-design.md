# 모의주문 접수·복구 연결

자동매매 확장 계획의 첫 주문 모듈이다. 연구 화면은 RESEARCH_ONLY를 유지한다.
과거 이벤트의 LONG/SHORT를 주문으로 변환하지 않는다. 사용자가 지정한 모의투자
연결 시험만 별도 CLI에서 실행한다.

## 이번 범위

- Alpaca paper endpoint 고정: 매수 지정가 DAY 주문, 1~10주, 주문당 1,000달러 이하.
- PostgreSQL paper_order_intents에 요청과 상태를 먼저 기록한다.
- account scope + request ID가 재실행의 기준이다. 같은 ID로 가격·수량 변경 시 거절한다.
- 최초 전송 전에 UNKNOWN 상태를 commit한다. timeout·5xx·프로세스 중단 뒤에는
  client_order_id로 조회만 한다. 404여도 자동 재주문하지 않는다.
- 요청별 PostgreSQL advisory lock으로 동시 실행을 직렬화한다.
- 접수·부분체결·체결·취소 상태와 누적 체결량을 브로커 조회로 갱신한다.
- 취소 요청 응답만으로 취소 완료라고 기록하지 않는다. 이후 조회로 확인한다.
- 모의투자와 로컬 mock 실행은 별도 scope를 사용한다.

## 검증과 한계

로컬 mock은 접수 후 응답 유실 → 재시작 조회 → 부분체결 → 체결을 재현한다.
같은 DB에서 두 서비스 인스턴스를 사용해 POST 1회, journal 1행을 검증한다.
동시 실행, 요청 내용 충돌, 미확인 주문, 잘못된 응답도 시험한다.
실제 paper 접속은 별도 인증 여부를 확인한 다음 진행한다. mock 통과를 실제
Alpaca 체결이나 전략 수익 검증으로 표현하지 않는다.

일일 손실 한도, 전체 계좌 포지션 대사, 실시간 전략, 매도·공매도, Slack 승인,
실계좌 연결은 다음 단계다. 주문당 한도는 전체 위험관리 구현을 뜻하지 않는다.

공식 계약: https://docs.alpaca.markets/us/docs/working-with-orders
주문 조회: https://docs.alpaca.markets/us/reference/getorderbyclientorderid
