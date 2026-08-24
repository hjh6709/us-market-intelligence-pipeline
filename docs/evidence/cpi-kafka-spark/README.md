# CPI SIP Kafka·Spark 실행 증거

공개 저장소에는 API key나 58,036개의 원시 payload를 넣지 않고, 재현 코드와 비식별 집계 결과만 보관한다.

## 58,036건의 정확한 의미

- 기준 이벤트: 2026년 7월 미국 CPI, 2026-08-12 08:30 ET 발표
- API 조건: `NVDA`, `feed=sip`, `[2026-08-12T11:30:00Z, 2026-08-12T13:31:00Z)`
- 행 단위: Historical Trades API가 반환한 개별 체결 레코드 한 건
- 범위: 07:30~09:31 ET, 장전 120분 중 CPI 발표 전후 구간과 정규장 첫 1분 포함
- 제외되는 해석: CPI 지표 58,036개, 하루치 데이터, 미국 전체 종목 거래, 체결 수량 합계

체결 수량은 각 원본 행의 `s` 필드이며, 거래량은 `SUM(s)`로 계산한다. 58,036은 `COUNT(record)`다.

## 공개된 증거

- [result.json](result.json): Producer·Consumer·Spark 처리 건수와 저장 결과
- [실행 보고서](../../test-results/2026-08-24-cpi-kafka-spark.md): 실행 조건, 처리 단계와 해석
- [Kafka·Spark 과제 문서](../../kafka-spark-assignment.md): 메시지 명세, 명령과 최종 schema
- [`cpi_sip_kafka_spark_evidence.sql`](../../../scripts/evidence/cpi_sip_kafka_spark_evidence.sql): 로컬 DB의 실제 행·건수·중복을 조회하는 SQL

## 멘토 앞에서 보여주는 순서

### 1. 서비스 상태

```bash
docker compose ps
```

Kafka와 PostgreSQL이 `healthy`인지 보여준다.

### 2. Topic 설정

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe --topic raw.market-sip.v1
```

Topic 이름, partition 3개와 24시간 retention을 보여준다.

### 3. Producer·Consumer 건수

이미 검증한 집계는 `result.json`에서 먼저 보여준다. 재확인을 요청받으면 다음 Consumer를 실행한다.

```bash
KAFKA_TOPIC=raw.market-sip.v1 .venv/bin/python -m src.kafka_trace_consumer \
  --trace-id cpi-20260812-nvda-sip-001 \
  --expected-count 58036 --timeout 120
```

정상 결과는 `consumer_received=58036`이다. Kafka retention이 지나 Topic 데이터가 삭제된 경우에는 과거 숫자가 남아 있다고 가장하지 말고, replay 명령부터 다시 실행해야 한다.

### 4. Spark 코드와 실행 결과

[`src/spark_sip_trade_batch.py`](../../../src/spark_sip_trade_batch.py)에서 Kafka read, trace filter, schema validation, `event_id` 중복 제거, 1분 집계와 PostgreSQL upsert 순서만 설명한다. 실제 실행 결과는 다음이다.

```text
input 58,036
→ invalid 0
→ unique 58,036
→ 1-minute bars 121
→ PostgreSQL upsert 121
```

### 5. PostgreSQL 실제 저장 결과

```bash
docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/cpi_sip_kafka_spark_evidence.sql
```

확인할 내용은 다음과 같다.

- `reconstructed_bar_rows=121`
- `business_keys=121`
- `reconstructed_trade_count=58036`
- 시간 범위 `11:30–13:30 UTC`
- duplicate query `0 rows`
- 실제 OHLCV 처음·마지막 행

## 화면에 노출하면 안 되는 것

- `.env`와 API key
- Alpaca 요청 header
- 원시 payload 전체
- terminal history와 PostgreSQL 접속 비밀번호

발표 중에는 위의 고정 명령만 실행하고 `.env`, `printenv`, shell history는 열지 않는다.
