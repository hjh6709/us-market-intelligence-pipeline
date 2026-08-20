# 2차시 과제 제출 점검표

과제의 필수 범위는 실제 데이터의 `Data Source → Ingestion → Data Storage` 구현이다. 분석·BI는 전체 아키텍처에는 표시하지만 이번 완료 범위로 주장하지 않는다.

## 제출 상태

| 요구사항 | 저장소 증거 | 상태 |
| --- | --- | --- |
| 프로젝트 목표와 데이터셋 | [README](../README.md) | 완료 |
| 전체 데이터 흐름 아키텍처 | [pipeline-architecture.png](diagrams/pipeline-architecture.png) | 완료 |
| 실제 데이터 수집 코드 | `src/market_producer.py`, `src/historical_market_replay.py` | 완료 |
| Kafka 원본 수집 | `raw.market.v1`, WebSocket 실제 거래 10건 재소비 증거 | 완료 |
| Spark 처리 코드 | `src/spark_market_processor.py` | 완료 |
| PostgreSQL 저장 코드·스키마 | `src/postgres.py`, `db/migrations/001_market_bars.sql` | 완료 |
| 실제 API → DB 저장 결과 | 실제 거래 427건 발행, 174건이 final 1분 봉 3건에 반영 | 완료 |
| 저장 결과 재현 | `scripts/evidence/actual_ingestion_evidence.sql` | 완료 |
| 실제 행 로컬 내보내기 | `scripts/evidence/export_actual_market_bars.py` | 완료 |
| 공개 샘플 데이터 | `data/sample/market_bars.synthetic.csv` | 완료, 합성임을 표시 |
| WebSocket → DB 실시간 전체 실행 | 다음 미국 정규장 검증 | 미완료·명시 |

## 발표 직전 확인 순서

1. `docker compose ps`에서 Kafka와 PostgreSQL이 healthy인지 확인한다.
2. README의 아키텍처에서 현재 실선 경로만 설명한다.
3. 실제 수집 결과 보고서에서 `427건 발행 → 174건 반영 → 3개 final 봉 → 중복 0개`를 보여준다.
4. 로컬 PostgreSQL에 아래 SQL을 실행해 OHLCV 실제 행을 보여준다.
5. 필요하면 로컬 CSV를 내보내 실제 데이터가 파일로 생성되는 것까지 확인한다.
6. WebSocket은 Kafka까지만, PostgreSQL 결과는 Historical API 경로임을 구분해서 말한다.

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
- 22종목 규모에서 Spark를 유지할 실익과 Python processor 비교 기준은 무엇이 좋은가?
- 다음 단계에서 IEX 실시간 예비 신호와 historical SIP 사후 검증을 어떻게 분리하면 좋은가?
