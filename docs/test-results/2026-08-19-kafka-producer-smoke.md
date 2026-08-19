# Kafka Producer Smoke Test — 2026-08-19

## 검증 대상

- Apache Kafka `4.3.1`, KRaft combined mode, single broker
- `confluent-kafka 2.15.0`
- topic `raw.market.v1`
- Alpaca raw trade → canonical envelope → Kafka record

## 토픽 설정

```text
PartitionCount: 3
ReplicationFactor: 1
retention.ms=86400000
```

종목코드를 record key로 사용하므로 같은 종목의 이벤트는 같은 partition에 기록된다. 원본 이벤트는 학습용 로컬 환경에서 24시간 보관한다.

## 실제 브로커 통합 테스트

`KafkaPublisher`로 NVDA fixture 1건을 발행하고 새 consumer group으로 같은 레코드를 다시 읽었다.

```text
produced: 1
consumed: 1
key: NVDA
value: canonical envelope와 일치
delivery: 성공
```

확인한 envelope 필드는 `event_id`, `event_type`, `schema_version`, `source`, `feed`, `source_event_id`, `event_timestamp`, `ingested_at`, `trace_id`, `payload`다. `payload`는 Alpaca 필드명을 삭제하거나 바꾸지 않는다.

## Alpaca IEX → Kafka 실시간 검증

병합된 `main`에서 `SPY`, `QQQ`, `NVDA`를 구독해 실제 IEX trade 10건을 `raw.market.v1`에 발행했다. 별도 consumer group으로 다시 읽어 다음 결과를 확인했다.

```text
published: 10
consumed: 10
symbols: NVDA, QQQ, SPY
canonical envelope: 10/10 유효
원본 routing/identity 필드 T·S·i·t: 10/10 유지
```

API key와 secret은 ignored `.env`에서만 읽었으며 테스트 출력과 이 문서에는 기록하지 않았다.

## 신뢰성 설정과 한계

- Producer: `enable.idempotence=true`, `acks=all`
- 로컬 queue full: 최대 3회 poll 후 명시적 실패
- flush timeout 또는 delivery callback 오류: 예외로 처리
- 단일 브로커와 복제 계수 1이므로 브로커 장애 중 데이터 보존을 보장하지 않는다.
- 이번 검증은 수집·전송 경로의 동작을 확인한 smoke test다. 장시간 안정성, 처리량, consumer lag와 장애 복구는 별도 부하 테스트에서 측정한다.
