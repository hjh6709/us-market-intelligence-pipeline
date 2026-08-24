# 4차시 Kafka·Spark 과제 제출 점검표

과제의 필수 범위는 Kafka 이벤트 100건 이상 전송, Producer·Consumer 건수 대조, Spark 전처리와 최종 저장이다. 분석·BI는 다음 단계다.

## 제출 상태

| 요구사항 | 저장소 증거 | 상태 |
| --- | --- | --- |
| 필드명·타입·의미 | README 요약과 `docs/kafka-spark-assignment.md` 전체 표 | 완료 |
| Kafka JSON 예시와 Topic | `raw.market.v1`, 상세 과제 문서의 합성 JSON | 완료 |
| 실제 데이터 Producer | `src/historical_market_replay.py` | 완료 |
| Kafka 100건 이상 | CPI 발표 구간 실제 NVDA 거래 1,576건 발행 | 완료 |
| Producer·Consumer 건수 대조 | `1,576 = 1,576`, `src/kafka_trace_consumer.py` | 완료 |
| Spark 처리 코드 | `src/spark_market_processor.py` | 완료 |
| 처리 전·후 건수 | 입력 1,576, 오류 0, 확정 반영 509, 최종 18행 | 완료 |
| PostgreSQL 저장 코드·스키마 | `src/postgres.py`, `db/migrations/001_market_bars.sql` | 완료 |
| 최종 컬럼·저장 형식 | README의 PostgreSQL `market_bars` 표 | 완료 |
| 실제 API → DB 저장 결과 | [CPI 구간 실행 보고서](test-results/2026-08-24-cpi-kafka-spark.md) | 완료 |
| 저장 결과 재현 | `scripts/evidence/actual_ingestion_evidence.sql` | 완료 |
| 실제 행 로컬 내보내기 | `scripts/evidence/export_actual_market_bars.py` | 완료 |
| 공개 샘플 데이터 | `data/sample/market_bars.synthetic.csv` | 완료, 합성임을 표시 |
| WebSocket → DB 실시간 전체 실행 | 다음 미국 정규장 검증 | 미완료·명시 |

## 발표 직전 확인 순서

1. `docker compose ps`에서 Kafka와 PostgreSQL이 healthy인지 확인한다.
2. README의 아키텍처에서 현재 실선 경로만 설명한다.
3. 실행 보고서에서 `Producer 1,576건 = Consumer 1,576건 = Spark 입력 1,576건`을 보여준다.
4. `509건 반영 → 18개 final 봉 → 중복 0개`를 보여주고, 입력과 반영 건수의 차이 1,067건은 watermark 미통과 상태로 추정한 값이라고 구분한다.
5. 로컬 PostgreSQL에 아래 SQL을 실행해 OHLCV 실제 행을 보여준다.
6. 필요하면 로컬 CSV를 내보내 실제 데이터가 파일로 생성되는 것까지 확인한다.
7. WebSocket은 Kafka까지만 확인한 선행 실험이고, 제출 결과는 CPI 발표 구간 Historical Trades API의 실제 체결을 동일 Kafka·Spark 경로로 재생한 것이라고 구분한다.

```bash
docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/actual_ingestion_evidence.sql

.venv/bin/python -m scripts.evidence.export_actual_market_bars
```

## 공개 저장소에 포함하지 않는 것

- `.env`, Alpaca key·secret, 인증 URL
- 실제 원본 거래 payload와 정확한 시장 가격 CSV
- PostgreSQL dump, Spark checkpoint, 실행 로그 원본
- 터미널 history나 비밀정보가 보이는 캡처

실제 가격 행을 공개하지 않는 것은 데이터가 없어서가 아니라 Alpaca 재배포 조건을 지키기 위한 선택이다. 공개 저장소에는 집계 결과, 중복 검사, 재현 코드와 실제 행의 해시를 증거로 남긴다.

## 멘토에게 받을 피드백

- 2분 watermark 때문에 마지막 구간의 봉이 미확정으로 남는 현재 기준이 적절한가?
- 현재 검증 규모에서 Spark를 유지할 실익과 Python processor 비교 기준은 무엇이 좋은가?
- 다음 단계에서 IEX 실시간 예비 신호와 historical SIP 사후 검증을 어떻게 분리하면 좋은가?
