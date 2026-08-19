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

## 신뢰성 설정과 한계

- Producer: `enable.idempotence=true`, `acks=all`
- 로컬 queue full: 최대 3회 poll 후 명시적 실패
- flush timeout 또는 delivery callback 오류: 예외로 처리
- 단일 브로커와 복제 계수 1이므로 브로커 장애 중 데이터 보존을 보장하지 않는다.
- live Alpaca → Kafka 10건 검증 결과는 feature를 main에 병합한 뒤 로컬 `.env`를 사용해 이 문서에 추가한다.
