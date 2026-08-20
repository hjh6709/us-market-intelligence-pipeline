# Historical 실제 Alpaca 데이터 수집·저장 결과

- 실행일: 2026-08-20 KST
- 데이터: Alpaca Historical Trades API의 실제 IEX `SMH` 거래
- 조회 구간: 2026-08-19 19:50:00Z 이상, 19:56:00Z 미만
- 실행 경로: Alpaca REST → Python ingestion → Kafka `raw.market.v1` → Spark Structured Streaming → PostgreSQL `market_bars`

이 보고서의 1분 봉은 WebSocket 실시간 10건으로 만든 결과가 아니다. 장 종료 후 Historical Trades REST API로 조회한 실제 거래를 동일한 처리 경로에 넣어 저장 단계를 검증한 결과다. WebSocket 실제 거래 10건은 별도 실행에서 Kafka 발행·재소비까지 검증했으며, WebSocket부터 PostgreSQL까지의 전체 실시간 실행은 다음 미국 정규장 검증 항목이다.

## 결과

| 확인 항목 | 결과 |
| --- | ---: |
| API page | 1 |
| 가져온 실제 거래 | 427건 |
| Kafka 발행 성공 | 427건 |
| PostgreSQL final 1분 봉 | 3건 |
| final business key | 3개 |
| 중복 business key | 0개 |
| final 봉 시간 범위 | 19:50Z ~ 19:52Z |
| 집계 거래량 | 6,914주 |
| 집계 trade count | 174건 |

Spark는 2분 event-time watermark를 사용합니다. 조회 구간의 끝부분은 이후 event가 충분히 진행되기 전까지 미확정 상태이므로, 이 실행에서는 19:50~19:52의 3개 봉만 final row로 저장됐습니다. 이는 손실이 아니라 늦게 도착하는 거래를 기다리는 설계입니다.

## 합격 기준 확인

- [x] 합성 fixture가 아닌 외부 API 실제 데이터 사용
- [x] 조회 건수와 Kafka 발행 건수 일치: 427 = 427
- [x] PostgreSQL final row 1건 이상: 3건
- [x] 저장된 종목·시간 범위·source·feed 일치
- [x] 중복 business key 0개
- [x] API key, secret, 원본 응답을 Git 증빙에 저장하지 않음

재현 명령과 SQL은 [증빙 절차](../evidence/actual-ingestion/README.md), 기계 판독용 수치는 [result.json](../evidence/actual-ingestion/result.json)에 있습니다.
