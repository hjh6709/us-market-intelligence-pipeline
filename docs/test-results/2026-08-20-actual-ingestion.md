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
| 실제 저장 행 SHA-256 | `098f3ce57033680c6d4f3ef24b2668ec58d4f3c65804b7908cea1d9ea1206c51` |

Spark는 2분 event-time watermark를 사용합니다. 실제 거래 427건을 Kafka에 발행했으며, 이 실행 종료 시점에는 그중 174건이 19:50~19:52의 확정 봉 3개에 반영됐습니다. 나머지 구간은 watermark를 통과하지 않아 PostgreSQL final row로 저장되지 않았습니다. checkpoint를 유지하고 이후 event-time이 진행되면 확정될 수 있습니다.

## 합격 기준 확인

- [x] 합성 fixture가 아닌 외부 API 실제 데이터 사용
- [x] 조회 건수와 Kafka 발행 건수 일치: 427 = 427
- [x] PostgreSQL final row 1건 이상: 3건
- [x] 저장된 종목·시간 범위·source·feed 일치
- [x] 중복 business key 0개
- [x] API key, secret, 원본 응답을 Git 증빙에 저장하지 않음

재현 명령과 SQL은 [증빙 절차](../evidence/actual-ingestion/README.md), 기계 판독용 수치는 [result.json](../evidence/actual-ingestion/result.json)에 있습니다.

## 실제 저장 행 확인

`scripts/evidence/actual_ingestion_evidence.sql`은 로컬 PostgreSQL의 OHLCV·VWAP 실제 행과 중복 여부를 조회한다. 같은 행을 CSV로 확인할 때는 다음 명령을 사용한다.

```bash
.venv/bin/python -m scripts.evidence.export_actual_market_bars
```

CSV는 `data/local/actual_market_bars.csv`에 생성되며 Git에서 제외된다. 다른 출력 경로는 스크립트가 거부한다. Alpaca의 시장 데이터 재배포 제한 때문에 공개 저장소에는 정확한 가격 행이나 원본 payload를 올리지 않는다. 공개 증빙은 행 수, UTC 범위, 집계값, 중복 검사, 재현 코드와 행 해시로 구성한다. 해시는 로컬 행의 일관성을 확인하며 데이터 출처 자체를 독립 증명하지는 않는다. 자세한 공개·로컬 구분은 [데이터 파일 안내](../../data/README.md)에 있다.
