# U.S. CPI Market Reaction Pipeline

> 과거 미국 CPI 발표 당시 공개된 값과 발표 전후 주식시장 반응을 같은 시간축으로 연결하고, 같은 발표 구간의 실제 거래를 Kafka·Spark로 재현하는 데이터 파이프라인입니다.

현재 목표는 자동매매나 가격 예측이 아닙니다. “CPI 때문에 주가가 올랐다”를 단정하기 전에 공식 발표 시각, 당시 이용 가능했던 경제지표 값, 전체 시장 분봉과 데이터 coverage를 재현하는 것이 우선입니다.

## 이번 실행 데이터가 정확히 무엇인가

`58,036건`은 경제지표 건수나 하루치 주가 데이터가 아닙니다. 2026년 7월 미국 CPI가 발표된 시각을 기준으로, NVDA 한 종목의 지정 구간에서 조회한 **개별 체결 레코드 수**입니다.

| 데이터 계층 | 한 행의 의미 | 이번 실행의 실제 범위 | 건수·값 |
| --- | --- | --- | --- |
| 경제 이벤트 | BLS의 CPI 발표 한 번 | 2026년 7월 CPI, 2026-08-12 08:30 ET(12:30 UTC) 발표 | 1건 |
| 거시 관측 | 발표일에 알 수 있었던 계절조정 CPI 지수 한 개 | ALFRED vintage `2026-08-12`, `CPIAUCSL`·`CPILFESL`, 기준월 `2026-07-01` | `332.813`, `336.789` |
| 시장 원본 | NVDA의 개별 체결 한 건 | Alpaca Historical Trades, `feed=sip`, 1분 버킷 `T-60`부터 `T+60`까지 포함하는 `[07:30, 09:31) ET` | 58,036 레코드, 6 API pages |
| 처리 결과 | NVDA의 event-time 1분 OHLCV 한 행 | `11:30`부터 `13:30 UTC`까지 Spark 집계 후 PostgreSQL 저장 | 121행 |

시장 원본 한 행에는 거래 ID, 종목, 거래소, 체결 가격, 체결 수량, 거래 조건, 체결 시각과 테이프 코드가 들어 있습니다. `58,036`은 이 행의 개수이며 거래량은 각 행의 체결 수량을 합산한 별도 값입니다. SIP는 여러 미국 거래소의 체결을 통합한 feed지만, 이번 숫자는 **미국 전체 종목**이 아니라 해당 시간 범위의 **NVDA 한 종목**만 뜻합니다.

## 현재 분석 범위

- CPI 발표: 최근 실제 발표 12회
- 경제지표: `CPIAUCSL`, `CPILFESL`의 ALFRED 당시 vintage
- 시장 데이터: Alpaca Historical SIP `1Min` bar
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 분석 window: 발표 전 60분, 발표 후 5·30·60분
- 저장소: PostgreSQL

2025년 10월 CPI는 실제 발표되지 않아 분석 목록에서 제외했습니다. 전망치 출처는 아직 연결하지 않았으므로 `forecast`와 `surprise`를 임의로 만들지 않습니다.

## 데이터 흐름

```text
BLS 공식 CPI 발표 시각
        +
ALFRED 당시 CPI·근원 CPI vintage
        +
Alpaca Historical SIP 1분봉
        ↓
검증·UTC 정규화·멱등 upsert
        ↓
PostgreSQL
  ├─ economic_events
  ├─ macro_observations
  ├─ market_bars
  └─ macro_event_impacts
        ↓
발표 전후 수익률·거래량·변동성·SPY 상대수익률

같은 BLS CPI 발표 시각
        +
Alpaca SIP 원시 체결
        ↓
Kafka raw.market-sip.v1
        ↓
Spark batch
        ↓
PostgreSQL market_bars
```

같은 CPI 발표 구간의 SIP 원시 체결 전체를 Kafka·Spark로 재생합니다. 이미 만들어진 1분봉을 Kafka에 넣는 것이 아니라, Spark가 원시 체결을 직접 검증·중복 제거·1분 집계합니다.

![CPI 발표 구간 SIP Kafka Spark 처리 경로](docs/diagrams/cpi-sip-kafka-spark-assignment.png)

## 데이터 출처

| 데이터 | 공식 출처 | 역할 |
| --- | --- | --- |
| CPI 발표 날짜·시각 | [BLS CPI release schedule·archive](https://www.bls.gov/schedule/news_release/cpi.htm) | 이벤트 기준 시각과 대상 월 |
| 당시 CPI 값과 revision | [FRED/ALFRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | 미래 수정값이 섞이지 않는 point-in-time 값 |
| 발표 구간 실제 체결 | [Alpaca Historical Stock Trades](https://docs.alpaca.markets/reference/stocktradesingle) | Kafka·Spark 실시간 경로의 결정적 replay |
| 발표 전후 주식시장 | [Alpaca Historical Stock Bars](https://docs.alpaca.markets/us/v1.4.2/reference/stockbars) | SIP 1분 OHLCV·거래 수·VWAP |

## 실제 구현 결과

| 단계 | 실제 결과 | 품질 확인 |
| --- | ---: | --- |
| BLS CPI 이벤트 | 12건 | 공식 시각·미국 동부시간·UTC 보존 |
| ALFRED 관측값 | 24건 | 결측 0, 중복 0 |
| Historical SIP 1분봉 | 5,320건 | 재실행 후 동일 건수, 중복 0 |
| Event impact | 192건 | SPY benchmark 누락 0, 중복 0 |
| Matched baseline | 576건 | 36개 비교 시간창, 재실행 후 동일 건수 |
| 2026-08-12 CPI 구간 NVDA Historical SIP 체결 레코드 | 58,036건 | 지정 시간 범위의 Trades API 반환·Producer·Consumer·Spark 입력 일치, 오류 0 |
| CPI 구간 Spark 재구성 1분봉 | 121건 | 전체 원시 체결 반영, 121분 coverage, 중복 key 0 |

`macro_event_impacts`는 `12회 × 4종목 × 4개 window`입니다. 데이터가 충분한 결과는 163건, 장전 거래가 희소한 partial coverage 결과는 29건입니다. 특히 SMH는 거래가 없는 분을 임의로 채우지 않았으므로 complete와 partial 결과를 분리해서 해석해야 합니다.

상세 결과:

- [Historical SIP backfill 결과](docs/test-results/2026-08-24-cpi-sip-backfill.md)
- [CPI event impact 초기 결과](docs/test-results/2026-08-24-cpi-event-impact.md)
- [같은 요일·시간 matched baseline 결과](docs/test-results/2026-08-24-cpi-matched-baseline.md)

현재 평균 수익률은 선택한 12개 발표 구간의 관측값입니다. 비발표일 비교군과 통계 검정이 아직 없으므로 CPI의 인과 효과나 미래 수익률로 해석하지 않습니다.

## 실행 방법

### 1. 환경 준비

```bash
cp .env.example .env
uv sync
docker compose up -d --wait postgres
```

`.env`에는 `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, `FRED_API_KEY`를 입력합니다. 실제 key, 원본 API 응답과 PostgreSQL dump는 Git에 포함하지 않습니다.

### 2. CPI 데이터 파이프라인 실행

```bash
# BLS 발표 목록 + ALFRED 당시 값
.venv/bin/python -m src.cpi_ingestion

# 발표 전후 Historical SIP 1분봉
.venv/bin/python -m src.cpi_market_backfill

# 발표 전후 시장 반응 계산
.venv/bin/python -m src.macro_event_impact

# 발표 1·2·3주 전 같은 요일·동부시각 비교군
.venv/bin/python -m src.cpi_matched_baseline
```

모든 단계는 같은 입력으로 다시 실행해도 business key 기준 row 수가 증가하지 않도록 upsert합니다.

### 3. 검증

```bash
.venv/bin/python -m unittest discover -s tests -v

docker compose exec -T postgres \
  psql -U market -d market \
  -f /dev/stdin < scripts/evidence/cpi_event_impact_summary.sql
```

## 저장 모델

| 테이블 | 저장 내용 | 멱등 key |
| --- | --- | --- |
| `economic_events` | CPI 대상 월과 공식 발표 시각 | event type·reference period·release |
| `macro_observations` | ALFRED 값과 realtime/vintage 기간 | series·observation date·realtime start |
| `market_bars` | Alpaca SIP 1분봉 | symbol·bar start·timeframe·source·feed |
| `macro_event_impacts` | 종목별 window 반응과 SPY 비교 | event·symbol·feed·window·analysis version |
| `macro_event_baseline_impacts` | 동일 요일·시각의 비교 window | event·week offset·symbol·window·version |

상세 schema와 계산 계약은 [데이터 모델](docs/data-model.md), 시스템 경계는 [아키텍처](docs/architecture.md)에 있습니다.

## CPI 발표 구간 Kafka·Spark 경로

2026년 7월 CPI의 2026-08-12 발표 시각 `08:30 ET(12:30 UTC)`을 기준으로 1분 버킷 `T-60`부터 `T+60`까지 NVDA의 실제 SIP 원시 체결 전체를 replay했습니다. 원시 timestamp 조회 범위는 `[07:30, 09:31) ET`이며 장전 구간과 정규장 첫 1분을 포함합니다.

```text
NVDA Historical SIP 원시 체결 레코드 58,036건
→ Kafka raw.market-sip.v1
→ Spark batch validation·deduplication·aggregation
→ 1분 OHLCV 121건
→ PostgreSQL market_bars
```

여기서 58,036건은 SIP 전체 시장의 거래량이 아니라, `NVDA`, `feed=sip`, `[11:30:00, 13:31:00) UTC` 조건으로 Historical Trades API가 반환한 개별 체결 레코드 수입니다. 첫 체결은 `11:30:02 UTC`, 마지막 체결은 `13:30:59 UTC`이며 121개 분 구간이 모두 존재합니다. 기존 Alpaca provider SIP bar는 `source=alpaca`, Spark 재구성 bar는 `source=alpaca_replay`로 저장해 서로 덮어쓰지 않습니다. provider bar의 `trade_count` 합계는 58,034건이고 원시 Trades API 행은 58,036건이므로, 두 건의 차이를 임의 보정하지 않고 거래 조건 정책의 후속 검증 대상으로 남겼습니다.

### 4차시 과제 제출 요약

| 필수 항목 | 실제 구현 결과 |
| --- | --- |
| Kafka Topic | `raw.market-sip.v1`, key=`symbol`, 3 partitions |
| 메시지 명세 | 공통 envelope와 Alpaca payload의 필드·타입·의미·합성 JSON 예시 |
| 프로젝트 연결 | 2026년 7월 CPI 발표 전후 지정 구간의 NVDA Historical SIP 개별 체결 레코드 |
| 전송 건수 | Producer 58,036건 = Consumer 58,036건 |
| Spark 처리 전·후 | 입력 58,036건, 오류·중복 0건 → 1분봉 121건 |
| 최종 저장 | PostgreSQL `market_bars`, business key 기반 upsert, 중복 0건 |

#### 데이터·메시지 명세

- Topic: `raw.market-sip.v1`
- Kafka key: `symbol` (`NVDA`)
- partitions: `3`
- retention: `24시간`
- event type: `market.trade.raw`

| 구간 | 필드 | 타입 | 의미 |
| --- | --- | --- | --- |
| envelope | `event_id` | string | source·feed·종목·원본 거래 ID·거래 시각으로 만든 결정적 중복 제거 키 |
| envelope | `event_type` | string | 이벤트 종류. 현재 `market.trade.raw` |
| envelope | `schema_version` | integer | 메시지 구조 버전. 현재 `1` |
| envelope | `source` | string | 데이터 제공처. 현재 `alpaca` |
| envelope | `feed` | string | 시장 데이터 feed. 이번 실행은 `sip` |
| envelope | `source_event_id` | string | Alpaca가 제공한 원본 거래 ID |
| envelope | `event_timestamp` | UTC datetime string | 실제 거래 발생 시각 |
| envelope | `ingested_at` | UTC datetime string | Producer 수집 시각 |
| envelope | `trace_id` | string 또는 null | 한 번의 replay 실행을 추적하는 ID |
| envelope | `payload` | object | 변경하지 않고 보존한 Alpaca 원본 거래 필드 |
| payload | `T` | string | 메시지 종류. 거래는 `t` |
| payload | `S` | string | 종목 코드 |
| payload | `i` | integer | Alpaca 거래 ID |
| payload | `x` | string | 거래소 코드 |
| payload | `p` | decimal(18,6) | 체결 가격 |
| payload | `s` | integer | 체결 수량 |
| payload | `c` | array(string) | 거래 조건 코드 목록 |
| payload | `t` | UTC datetime string | 원본 체결 시각 |
| payload | `z` | string | 테이프 코드 |

아래 메시지는 스키마 설명을 위한 **합성 예시**이며 실제 시장 가격이나 API key가 아닙니다.

```json
{
  "event_id": "sha256:example-only",
  "event_type": "market.trade.raw",
  "schema_version": 1,
  "source": "alpaca",
  "feed": "sip",
  "source_event_id": "12345",
  "event_timestamp": "2026-08-12T11:30:02.123456Z",
  "ingested_at": "2026-08-24T07:05:00.000000Z",
  "trace_id": "assignment-example",
  "payload": {
    "T": "t",
    "S": "NVDA",
    "i": 12345,
    "x": "V",
    "p": 100.25,
    "s": 10,
    "c": ["@"],
    "t": "2026-08-12T11:30:02.123456Z",
    "z": "C"
  }
}
```

상세 계약과 Spark validation 규칙은 [4차시 Kafka·Spark 제출 문서](docs/kafka-spark-assignment.md)와 [데이터 모델](docs/data-model.md)에 있습니다.

```bash
docker compose up -d --wait kafka kafka-init postgres

KAFKA_TOPIC=raw.market-sip.v1 .venv/bin/python -m src.historical_market_replay \
  --symbol NVDA --start 2026-08-12T11:30:00Z \
  --end 2026-08-12T13:31:00Z --feed sip --max-pages 20 \
  --trace-id cpi-20260812-nvda-sip-001

KAFKA_TOPIC=raw.market-sip.v1 .venv/bin/python -m src.kafka_trace_consumer \
  --trace-id cpi-20260812-nvda-sip-001 \
  --expected-count 58036 --timeout 120

.venv/bin/python -m src.spark_sip_trade_batch \
  --trace-id cpi-20260812-nvda-sip-001 \
  --topic raw.market-sip.v1 --symbols NVDA
```

전체 메시지 표, JSON 예시, Spark 전처리 내용, 최종 컬럼과 실행 증거는 [4차시 Kafka·Spark 제출 문서](docs/kafka-spark-assignment.md)와 [CPI 구간 실행 보고서](docs/test-results/2026-08-24-cpi-kafka-spark.md)에서 확인할 수 있습니다. 현재 구현은 PostgreSQL 저장까지이며 Airflow, API·대시보드와 주문 실행은 다음 단계입니다.

## 다음 단계

1. 미국 거래일과 주요 경제 발표 calendar로 matched baseline 정제
2. 장전 반응과 첫 정규장 반응 분리
3. BLS 발표문의 월간·연간 actual 구조화
4. 검증 가능한 전망치 출처가 확보되면 surprise 분석 추가
5. Airflow로 수집·재실행·품질 검사를 자동화

## 문서

- [문서 전체 안내](docs/README.md)
- [데이터 소스](docs/data-source-catalog.md)
- [데이터 수명주기](docs/data-lifecycle.md)
- [설계 결정](docs/design-decisions.md)
- [4주 실행 계획](PROJECT_PLAN.md)

## 면책 및 출처 고지

이 프로젝트는 교육·연구 목적이며 투자 조언이 아닙니다. 계좌·주문 API를 호출하지 않습니다.

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.
