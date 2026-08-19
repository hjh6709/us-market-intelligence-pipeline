# PostgreSQL market bar 저장 테스트 결과

- 실행일: 2026-08-20 KST
- PostgreSQL: `postgres:17.6-alpine`, host port `55432`
- 저장 방식: Spark `foreachBatch` → psycopg transaction → business-key upsert
- business key: `(symbol, bar_start, timeframe, source, feed)`

## 확인 결과

| 검증 | 입력 | 기대 | 실제 |
| --- | ---: | ---: | ---: |
| 동일 2개 bar replay | 2행 × 2회 | DB 2행 | DB 2행 |
| 같은 key 값 보정 | NVDA close 102 → 103 | DB 2행, close 103 | 일치 |
| 비정상 OHLC transaction | 정상 1행 + `close > high` 1행 | batch 전체 rollback | DB 0행 |
| 연결 실패 후 재시도 | 같은 batch id 30 | 복구 후 1행 | DB 1행 |
| Kafka→Spark→PostgreSQL | publish 5건, duplicate 1건 | final bar 1행 | DB 1행 |
| checkpoint 재시작 | 동일 topic/checkpoint | 행 증가 없음 | 1행 → 1행 |
| UTC 시각 보존 | `13:30:00Z` trade | `13:30:00+00:00` 저장 | 일치 |
| 실제 DB stop/restart | batch id 9001 | 중단 중 실패, 복구 후 1행 | 일치 |

## 수직 슬라이스 고정 결과

```json
{
  "published_events": 5,
  "unique_final_bar_trades": 3,
  "postgres_rows_before_restart": 1,
  "postgres_rows_after_restart": 1,
  "final_bar": {
    "bar_start": "2026-08-19T13:30:00+00:00",
    "open": "100.000000",
    "high": "105.000000",
    "low": "100.000000",
    "close": "102.000000",
    "volume": 10,
    "trade_count": 3
  }
}
```

## 실제 PostgreSQL 중단·복구 결과

```text
before stop:  1|9001|9001
during stop:  database_unavailable|OperationalError|9001 (exit 2)
after retry:  upsert_succeeded|1|9001
final query:  1|9001|9001|102.000000|11|4
```

DB 연결 실패는 Spark micro-batch 성공으로 처리하지 않는다. 복구 후 같은 batch와 business key를 다시 처리하며, `ON CONFLICT DO UPDATE` 때문에 중복 행은 생성되지 않는다. Spark checkpoint는 offset/state 복구를 담당하고 PostgreSQL unique key는 sink의 at-least-once 재실행을 방어한다.

## 범위와 한계

- 현재 규모는 분당 최대 약 22개 final bar이므로 driver-side psycopg batch upsert를 사용한다.
- raw tick은 PostgreSQL에 장기 저장하지 않는다.
- PostgreSQL HA나 자동 failover를 검증한 결과가 아니다. 단일 컨테이너의 연결 실패·재기동·재처리 검증이다.
- 캡처 파일은 저장소에 가짜로 만들지 않는다. 실제 발표 실행 시 [증빙 체크리스트](../evidence/postgres-market-bars/README.md)에 따라 생성한다.
