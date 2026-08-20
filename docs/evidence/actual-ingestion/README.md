# 실제 Alpaca 데이터 수집·저장 증빙

이 증빙은 합성 fixture가 아니라 Alpaca Historical Trades API의 실제 IEX 거래를 기존 ingestion 경로로 재생해 PostgreSQL 저장까지 확인한다.

2026-08-20 검증 결과는 [테스트 보고서](../../test-results/2026-08-20-actual-ingestion.md)와 [result.json](result.json)에 저장했다. 공개 파일에는 집계 수치만 남기고 API key, secret, 원본 응답은 남기지 않았다.

```text
Alpaca Historical Trades
→ historical_market_replay
→ Kafka raw.market.v1
→ Spark Structured Streaming
→ PostgreSQL market_bars
```

## 고정 검증 구간

- symbol: `SMH`
- feed: `iex`
- start: `2026-08-19T19:50:00Z`
- end: `2026-08-19T19:56:00Z`
- API endpoint: `/v2/stocks/SMH/trades`

원본 API 응답, request header, API key와 전체 인증 URL은 파일이나 Git에 저장하지 않는다. 커밋 가능한 결과는 수집 건수, page 수, 저장된 final bar 수, UTC 범위와 중복 수뿐이다.

## 실행 순서

1. Kafka와 PostgreSQL을 시작한다.
2. 새 checkpoint로 Spark를 먼저 실행한다.
3. 고정 구간의 실제 거래를 Kafka에 발행한다.
4. watermark가 지나 final bar가 저장될 때까지 Spark를 실행한다.
5. SQL로 저장 건수와 중복을 확인한다.

명령은 README의 `2-B. 장 종료 후 실제 historical trade 재생`을 따른다. 저장 확인:

```bash
docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/actual_ingestion_evidence.sql
```

## 합격 기준

- `fetched_trades > 0`
- `published_trades = fetched_trades`
- `final_bar_rows > 0`
- `final_bar_rows = business_keys`
- duplicate query 결과 0행
- 저장된 `symbol=SMH`, `source=alpaca`, `feed=iex`, `is_final=true`
- `bar_start`가 지정한 실제 거래 구간 안의 UTC 시각

## 발표 캡처

캡처는 `docs/evidence/actual-ingestion/captures/`에 로컬 보관하며 Git에는 올리지 않는다.

1. Kafka·PostgreSQL healthy 상태
2. historical replay의 sanitized summary
3. Spark 실행 완료 로그에서 오류가 없는 상태
4. PostgreSQL final bar count와 UTC 범위
5. duplicate query 0행

캡처 전에 terminal history, `.env`, API key, connection URL과 원본 payload가 화면에 없는지 확인한다.
