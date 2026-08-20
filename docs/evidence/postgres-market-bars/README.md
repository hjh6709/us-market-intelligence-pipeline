# PostgreSQL market bar 실행 증빙

코드 존재 여부가 아니라 입력·처리·저장 수치가 서로 맞는지를 보여주기 위한 증빙 묶음이다. 비밀정보가 포함된 `.env`, 전체 connection URL, Alpaca key는 캡처하지 않는다.

## 고정 검증 시나리오

```text
Kafka publish 5건
= 정상 trade 4건 + duplicate 1건

13:30 final bar에 포함되는 unique trade 3건
→ volume 10, trade_count 3
→ PostgreSQL market_bars 1행

동일 checkpoint 재시작
→ PostgreSQL market_bars 여전히 1행
```

측정값 정본은 [result.json](result.json)이다.

## 실행 명령

```bash
docker compose up -d --wait postgres

RUN_POSTGRES_INTEGRATION=1 \
  .venv/bin/python -m unittest \
  tests/integration/test_postgres_market_bars.py -v

RUN_KAFKA_SPARK_POSTGRES_INTEGRATION=1 \
  .venv/bin/python -m unittest \
  tests/integration/test_kafka_spark_postgres.py -v

docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/market_bar_evidence.sql
```

## 장애·복구 재현

```bash
.venv/bin/python -m scripts.evidence.postgres_sink_probe
docker compose stop postgres
.venv/bin/python -m scripts.evidence.postgres_sink_probe
docker compose up -d --wait postgres
.venv/bin/python -m scripts.evidence.postgres_sink_probe
```

중단 중 probe는 `database_unavailable`과 종료 코드 `2`를 반환해야 한다. 복구 후 같은 `spark_batch_id=9001`을 다시 실행하면 성공하고 business row 수는 1로 유지돼야 한다.

## 캡처 체크리스트

발표나 과제 제출 직전에 다음 화면을 같은 실행 시각 흐름으로 캡처한다.

1. `01_postgres_healthy.png` — `docker compose ps`의 PostgreSQL healthy 상태
2. `02_vertical_slice.png` — Kafka→Spark→PostgreSQL 통합 테스트의 입력 수와 final bar JSON
3. `03_market_bars_query.png` — 전체 행 수와 business key 수가 모두 1인 SQL 결과
4. `04_database_unavailable.png` — PostgreSQL 중단 상태에서 batch 9001 실패 결과
5. `05_database_recovered.png` — 복구 후 동일 batch 성공과 최종 행 수 1

캡처는 실행 결과를 보조한다. 코드, migration, 자동 테스트와 [테스트 보고서](../../test-results/2026-08-20-postgres-market-bars.md)를 함께 제출해야 재현 가능하다.

공개 발표에서는 비밀정보가 제거된 `04_postgres_storage.png`와 `06_database_recovery.png`를 포함한 [발표용 캡처 묶음](../presentation-captures/README.md)을 사용한다.
