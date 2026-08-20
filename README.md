# U.S. Macro Impact & Market Data Pipeline

> 실제 미국 주식 거래를 수집·가공·저장하고, 이후 경제지표 발표 전후의 시장 반응을 반복 검증하기 위한 데이터 파이프라인입니다.

- 기간: 2026-08-13 ~ 2026-09-12
- 이번 과제 범위: 실제 데이터 소스 → Ingestion → Data Storage
- 기술: Alpaca Market Data, Kafka, Spark Structured Streaming, PostgreSQL, Docker Compose

## 이번 과제 목표

Alpaca의 실제 미국 주식 거래를 Kafka에 원본 이벤트로 보관하고, Spark로 1분 OHLCV를 만든 뒤 PostgreSQL에 중복 없이 저장합니다.

```text
Alpaca actual trade
→ Python ingestion
→ Kafka raw.market.v1
→ Spark Structured Streaming
→ PostgreSQL market_bars
```

장기적으로는 경제지표 발표 당시 공개된 값과 같은 시각의 시장 반응을 연결해 자동매매 전략 연구에 활용합니다. 이번 단계에서는 예측이나 주문보다 신뢰할 수 있는 수집·저장 기반을 먼저 구현합니다.

## 이번 과제 데이터셋

| 데이터 | 출처 | 사용 목적 |
| --- | --- | --- |
| 실시간 IEX 거래 | [Alpaca WebSocket](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data) | 장 운영 중 실시간 수집과 Kafka 경로 검증 |
| 과거 IEX 거래 | [Alpaca Historical Trades](https://docs.alpaca.markets/us/reference/stocktradesingle-1) | 장 종료 후에도 실제 거래로 전체 저장 경로 재현 |
| 테스트 replay | Alpaca 형식의 고정 fixture | 중복·지연·오류·장애 복구 자동 테스트 |

수집 필드는 종목, 거래 ID, 거래소, 가격, 수량, 조건과 실제 거래 시각입니다. 무료 IEX는 미국 전체 거래소 데이터가 아니므로 전체 시장의 확정 신호로 사용하지 않습니다. 다음 단계에서 historical SIP와 공식 경제지표·FRED 데이터를 별도로 연결합니다.

## 이번 과제 아키텍처

![전체 데이터 파이프라인 아키텍처](docs/diagrams/pipeline-architecture.png)

- 실선: 현재 구현된 데이터 경로. 실행 증거 범위는 화살표의 설명으로 구분
- 점선: Airflow, 경제지표 영향 분석과 BI 등 다음 단계
- Kafka: 원본 거래 이벤트를 24시간 보관
- PostgreSQL: 확정된 1분 봉을 business key 기준으로 저장

| 기술 | 사용하는 이유 |
| --- | --- |
| Kafka | 수집기와 처리기를 분리하고 처리기 중단 후 원본 이벤트를 다시 읽습니다. |
| Spark Structured Streaming | event-time 1분 집계, 중복 제거, watermark와 checkpoint를 처리합니다. |
| PostgreSQL | 최종 결과를 SQL로 확인하고 upsert로 재처리 중복을 막습니다. |
| Docker Compose | Kafka와 PostgreSQL 실행 환경을 로컬에서 재현합니다. |

편집 가능한 원본은 [pipeline-architecture.svg](docs/diagrams/pipeline-architecture.svg), 상세 설계는 [architecture.md](docs/architecture.md)에 있습니다.

## 이번 과제 실행 방법

### 1. 환경 준비

```bash
cp .env.example .env
uv sync
docker compose up -d --wait kafka kafka-init postgres
```

`.env`에 Alpaca key와 secret을 입력합니다. `.env`, 원본 응답, 실행 로그와 DB 파일은 Git에 포함하지 않습니다.

### 2. 실제 거래 수집

미국 장 운영 중에는 Spark와 WebSocket Producer를 각각 실행합니다.

```bash
.venv/bin/python -m src.spark_market_processor \
  --starting-offsets latest --symbols SPY QQQ NVDA --watermark "2 minutes"

.venv/bin/python -m src.market_producer \
  --feed iex --symbols SPY QQQ NVDA --max-trades 1000 --timeout 180
```

장 종료 후에는 실제 historical trade를 같은 Kafka·Spark 경로로 재생할 수 있습니다.

```bash
.venv/bin/python -m src.spark_market_processor \
  --starting-offsets latest --symbols SMH --watermark "2 minutes" \
  --checkpoint-root .spark-checkpoints/assignment-historical --timeout 120

.venv/bin/python -m src.historical_market_replay \
  --symbol SMH --start 2026-08-19T19:50:00Z \
  --end 2026-08-19T19:56:00Z --feed iex
```

### 3. 저장 결과와 테스트 확인

```bash
docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/actual_ingestion_evidence.sql

.venv/bin/python -m unittest discover -s tests -v
```

실제 저장된 OHLCV 행을 로컬 CSV로 확인하려면 다음 명령을 실행합니다.

```bash
.venv/bin/python -m scripts.evidence.export_actual_market_bars
```

생성되는 `data/local/actual_market_bars.csv`는 실제 Alpaca 시장 데이터이므로 Git에서 제외합니다. 공개 저장소에는 [합성 스키마 예시](data/sample/market_bars.synthetic.csv), 실제 처리 건수·시간 범위·중복 검사와 재현 코드만 둡니다. 구분 이유는 [데이터 파일 안내](data/README.md)에 적었습니다.

통합 테스트와 증빙 재현 명령은 [문서 안내](docs/README.md)에서 확인할 수 있습니다.

## 현재 구현 범위

- [x] Alpaca WebSocket 실제 거래 수신과 Kafka 발행·재소비
- [x] Historical Trades API 실제 데이터 replay
- [x] Kafka symbol key와 24시간 retention
- [x] Spark schema 검증, 중복 제거, watermark와 1분 OHLCV/VWAP
- [x] PostgreSQL migration, transaction upsert와 장애 복구 테스트
- [x] 실제 Historical 거래 → Kafka → Spark → PostgreSQL 통합 실행
- [ ] WebSocket → Kafka → Spark → PostgreSQL 실시간 전체 경로 증빙

| 검증 경로 | 실제 결과 | 상태 |
| --- | --- | --- |
| WebSocket → Kafka | 2026-08-19 실제 IEX 거래 10건 수신·발행·재소비 | 완료 |
| Historical REST → Kafka → Spark → PostgreSQL | 실제 IEX 거래 427건 발행, 그중 174건이 final 1분 봉 3건에 반영, 중복 key 0건 | 완료 |
| WebSocket → Kafka → Spark → PostgreSQL | Spark와 Producer를 동시에 실행해 end-to-end로 확인 | 다음 미국 정규장에 검증 |

PostgreSQL의 3개 봉은 WebSocket 10건으로 만든 것이 아닙니다. Historical API의 실제 거래 427건을 Kafka에 발행한 실행에서, watermark를 통과한 174건이 반영된 결과입니다. 자세한 수치는 [실제 수집·저장 결과](docs/test-results/2026-08-20-actual-ingestion.md)에 있습니다.

## 다음 단계

1. 미국 정규장에 WebSocket → PostgreSQL 전체 경로 검증
2. Airflow로 공식 발표 시각과 FRED/ALFRED 당시 값 수집
3. Historical SIP로 발표 전후 반응과 평소·시장·섹터 기준 비교
4. 5분·30분·60분 반응을 조회하는 SQL·BI 결과 작성

자동 주문은 반복 사례, point-in-time 백테스트와 위험 관리 검증 이후의 별도 단계입니다.

## 고민한 부분과 현재 이슈

- 현재 22종목 규모에는 Python 처리기도 가능하지만 과정 필수 기술과 event-time 처리를 검증하기 위해 Spark를 사용합니다.
- IEX는 전체 시장이 아니므로 실시간 결과는 예비 신호로만 보고 historical SIP로 사후 확인합니다.
- 로컬 Kafka는 single broker이므로 복제 기반 고가용성을 제공하지 않습니다.
- 원본 trade는 Kafka에서 24시간 보관하고 PostgreSQL에는 최종 1분 봉만 저장합니다.

## 문서와 발표 자료

- [문서 전체 안내](docs/README.md)
- [과제 제출 점검표](docs/submission-checklist.md)
- [데이터 파일 안내](data/README.md)
- [4주·8회차 실행 계획](PROJECT_PLAN.md)
- [실제 실행 증거](docs/evidence/actual-ingestion/README.md)
- [발표 자료·대본·예상 질문](docs/presentation/README.md)

## 면책 및 출처 고지

이 프로젝트는 교육·연구 목적이며 투자 조언이 아닙니다. 현재 구현은 계좌·주문 API를 호출하지 않습니다.

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.
