# Kafka 427건 전송과 Spark 전처리·저장 결과

- 실행일: 2026-08-21 KST
- 데이터: Alpaca Historical Trades API의 실제 IEX `SMH` 거래
- 조회 구간: `2026-08-19T19:50:00Z` 이상, `19:56:00Z` 미만
- Topic: `raw.market.v1`
- 검증 trace: `assignment-20260821-smh-001`
- 실행 경로: Alpaca REST → Producer → Kafka → Spark Structured Streaming → PostgreSQL

API key, 원본 payload, 정확한 시장 가격 행과 Spark checkpoint는 Git에 포함하지 않는다. 이 문서에는 건수, offset 범위, 저장 범위와 중복 검사만 기록한다.

## 1. Producer와 Consumer 건수

고정 trace ID로 실제 거래를 발행했다.

```bash
.venv/bin/python -m src.historical_market_replay \
  --symbol SMH --start 2026-08-19T19:50:00Z \
  --end 2026-08-19T19:56:00Z --feed iex \
  --trace-id assignment-20260821-smh-001
```

Producer summary는 API 조회 427건과 Kafka 발행 427건을 보고했고, delivery flush까지 오류 없이 완료됐다. 검증 Consumer는 원본 값을 출력하지 않고 같은 trace의 메시지만 계산했다.

```bash
.venv/bin/python -m src.kafka_trace_consumer \
  --trace-id assignment-20260821-smh-001 \
  --expected-count 427 --timeout 60
```

| 확인 항목 | 결과 |
| --- | ---: |
| API 조회 | 427건 |
| Producer 발행 | 427건 |
| Consumer 수신 | 427건 |
| 발행·수신 차이 | 0건 |

## 2. Spark 처리 전·후 건수

Spark는 새 checkpoint와 `startingOffsets=latest`로 Producer보다 먼저 시작했다. `raw.market.v1`의 source offset은 partition 2에서 `427 → 854`로 증가했고 다른 partition은 변하지 않았다. 따라서 이 실행에서 Spark가 읽은 입력은 427건이다.

invalid reason 집계는 빈 결과여서 schema, 필수값, symbol, 가격·수량과 timestamp validation 오류는 0건이었다.

| 처리 단계 | 건수 | 의미 |
| --- | ---: | --- |
| Spark Kafka source 입력 | 427건 | source offset 증가량 |
| validation 오류 | 0건 | invalid reason 없음 |
| 확정 봉 반영 거래 | 174건 | `SUM(trade_count)` |
| 확정 봉 미반영 차이 | 253건 | 입력 427 - 확정 봉 반영 174 |
| 최종 1분봉 | 3행 | 19:50Z~19:52Z |

Spark의 전처리는 JSON schema parsing, 필수 필드 검증, symbol allowlist, 양수 가격·0 이상 수량 검증, UTC event-time 변환, `event_id` 중복 제거, 2분 watermark와 1분 window 집계를 수행한다. 최종 집계는 OHLCV, 거래 건수와 VWAP이다.

`427 - 174 = 253`이다. 이 값은 Spark 상태 저장소에서 직접 센 건수가 아니라 입력과 확정 봉 반영 건수의 차이다. validation 오류가 0건이고 append mode는 watermark를 통과한 final window만 저장하므로, 종료 시 미확정 window에 남은 건수로 추정한다.

## 3. PostgreSQL 저장 결과

저장 대상은 `market.market_bars`이고 business key는 `(symbol, bar_start, timeframe, source, feed)`다.

| 확인 항목 | 결과 |
| --- | ---: |
| final 1분봉 | 3행 |
| 집계 trade count | 174건 |
| 집계 volume | 6,914주 |
| 중복 business key | 0개 |

최종 컬럼은 `symbol`, `bar_start`, `timeframe`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`, `source`, `feed`, `is_final`, `condition_policy`, `spark_batch_id`, `updated_at`이다.

실제 저장 행은 다음 읽기 전용 SQL과 로컬 전용 CSV 내보내기로 확인한다.

```bash
docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/actual_ingestion_evidence.sql

.venv/bin/python -m scripts.evidence.export_actual_market_bars
```

기계 판독용 집계 결과는 [result.json](../evidence/actual-ingestion/result.json), JSON 메시지 정본은 [데이터 모델](../data-model.md)에 있다.

## 4. 추가 검증

- 전체 테스트: 70개 통과
- 외부 서비스 실행 flag가 필요한 통합 테스트: 6개 건너뜀
- 실제 Kafka·Spark·PostgreSQL 경로: 이 문서의 427건 실행으로 별도 검증
- 로컬 실제 행 내보내기: 3행
- 공개 증빙 SHA-256과 로컬 내보내기 해시: 일치

통합 테스트가 건너뛰어진 사실을 숨기지 않는다. 단위 테스트와 로컬 Spark 테스트는 전체 통과했고, 서비스가 필요한 실제 수직 경로는 위의 고정 trace 실행 결과로 확인했다.
