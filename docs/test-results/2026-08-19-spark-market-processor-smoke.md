# Spark Market Processor Smoke Test — 2026-08-19

## 검증 대상

- Python `3.14.6`
- Java `21.0.12`
- PySpark `4.2.0`
- Spark Kafka connector `spark-sql-kafka-0-10_2.13:4.2.0`
- Apache Kafka `4.3.1`, single broker
- Kafka `raw.market.v1` 형식 → Spark schema/validation → dedup/watermark → final 1분 bar

Spark 4.2 공식 요구사항인 Python 3.10 이상과 Java 17 이상을 충족하며, local DataFrame action과 실제 Kafka source query를 모두 실행했다.

## 확인한 처리 흐름

```text
Kafka value JSON
→ canonical envelope / Alpaca payload explicit schema
→ normalized MarketTrade
→ valid / invalid reason split
→ event_timestamp 2-minute watermark
→ event_id dropDuplicatesWithinWatermark
→ symbol/source/feed + 1-minute window
→ append-mode final OHLCV/VWAP/trade_count
```

Alpaca raw payload에는 `T`와 `t`가 동시에 존재하므로 Spark session을 `spark.sql.caseSensitive=true`로 고정했다. 그렇지 않으면 두 field가 ambiguous reference로 충돌한다.

## 결정적 fixture 결과

13:30 window에 out-of-order trade 4건과 duplicate 1건을 넣었다. 중간 micro-batch 뒤에 도착했지만 watermark 안에 있던 13:30:20 trade도 final 출력 전에 반영했다.

```text
open: 100.000000
high: 105.000000
low: 99.000000
close: 102.000000
volume: 11
trade_count: 4
vwap: 101.727273
condition_policy: all_valid_trades_v1
```

같은 event를 두 번 발행했지만 `event_id` deduplication 뒤 `trade_count`는 4다. 13:33:30 event로 watermark를 진행시켜 13:30 bar를 append output으로 확정했다.

그 뒤 13:30:40 event를 추가로 보냈을 때 final bar는 바뀌지 않았고 `stateOperators.numRowsDroppedByWatermark >= 1`을 확인했다.

## Checkpoint 재시작

동일 Kafka topic, output path와 checkpoint로 query를 중단·재시작했다. 재시작 전후 output row 수가 같아 이미 처리한 Kafka offset이 다시 final bar로 출력되지 않았다. 새 checkpoint를 사용하는 full replay와 이 정상 재시작을 같은 동작으로 취급하지 않는다.

## Invalid 처리

다음 bounded reason을 fixture로 검증했다.

```text
MALFORMED_JSON
INVALID_EVENT_TYPE
UNSUPPORTED_SCHEMA_VERSION
MISSING_EVENT_ID
MISSING_SOURCE_EVENT_ID
SYMBOL_NOT_ALLOWED
INVALID_PRICE
INVALID_SIZE
INVALID_TIMESTAMP
TIMESTAMP_MISMATCH
```

runner는 invalid raw JSON을 출력하지 않고 `foreachBatch`에서 reason별 count만 보여준다. 운영용 `dead-letter.v1` sink는 PostgreSQL sink와 함께 다음 PR에서 구현한다.

## 실행 명령

```bash
RUN_SPARK_KAFKA_INTEGRATION=1 \
  .venv/bin/python -m unittest tests/integration/test_spark_market_processor.py -v
```

## 현재 한계

- bar sink는 현재 console/test Parquet이며 PostgreSQL 저장은 아직 없다.
- `all_valid_trades_v1`은 schema-valid trade를 모두 포함한다. Alpaca 제공 bar와 condition별 완전 일치를 주장하지 않는다.
- 두 output query가 같은 Kafka source를 각각 실행하는 local MVP 구조다. 실제 처리 비용은 부하 테스트에서 측정한다.
- Kafka는 single broker/replication factor 1이므로 broker 고가용성을 보장하지 않는다.
