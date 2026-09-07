# 모의주문 접수·복구 검증

`actual-paper-probe.json`은 기존 키로 실제 Alpaca paper 계좌에 접속해 실행한
접수·조회·취소 결과다. NVDA 1주·1달러 지정가 주문 1개를 취소했고 체결은 0주다.
같은 요청 재실행 후에도 canceled 상태와 DB 1행을 확인했다. 터미널 응답과
PostgreSQL 조회를 요약했으며 계좌 ID, 키, 브로커 주문 ID는 제외했다.

`local-drill.json`은 2026-09-07 `run_paper_execution_drill.py` 실행 결과다.
HTTP 응답은 httpx MockTransport, 주문 기록은 로컬 PostgreSQL을 사용했다.
Alpaca 서버·실계좌 호출은 0건이다. 결과에는 키나 계좌 식별자를 포함하지 않았다.

입력 3개 → mock POST 3회 → journal 3행. 응답 유실 후 조회 복구,
동시 전송 직렬화, 부분체결·취소 상태와 변경된 요청 차단을 검증했다.
mock 증거 자체로 실제 체결 및 전체 포지션 복구를 보장하지 않는다.
외부 접수·취소 증거와 로컬 부분체결·장애 시험은 별개의 실행이다.

## 재시작 복구 추가 증거

[actual-restart-recovery.json](actual-restart-recovery.json)은 2026-09-07 새 CLI 프로세스로
실제 모의계좌의 기존 주문을 조회한 결과다. DB 1개 주문을 복원해 canceled·체결 0주,
NVDA 보유량 0주를 확인했다. 이 실행에서는 신규 주문을 보내지 않았다.
기준 보유량을 수집하기 전이므로 포지션 대사는 완료 처리하지 않았다.
