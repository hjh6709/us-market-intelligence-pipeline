# U.S. CPI Market Reaction Pipeline

> 과거 미국 CPI 발표 당시 공개된 값과 발표 전후 주식시장 반응을 같은 시간축으로 연결하고, 같은 발표 구간의 실제 거래를 Kafka·Spark로 재현하는 데이터 파이프라인입니다.

장기 목표는 검증 가능한 데이터에 기반한 자동매매 시스템입니다. 다만 현재 단계에서는 주문이나 가격 예측보다, “CPI 때문에 주가가 올랐다”를 단정하기 전에 공식 발표 시각, 당시 이용 가능했던 경제지표 값, 전체 시장 분봉과 데이터 coverage를 재현하는 데 집중합니다.

## 프로젝트 목표

이 프로젝트는 경제지표 발표와 시장 데이터를 수집·처리·저장하고, 같은 입력으로 결과를 다시 계산할 수 있는 데이터 기반을 만드는 것이 목적입니다.

- BLS의 공식 발표 일정과 ALFRED의 당시 공개값을 point-in-time 형태로 보존합니다.
- Alpaca SIP 시장 데이터를 같은 발표 시각에 맞춰 수집합니다.
- Kafka와 Spark를 통해 원시 거래의 전달·검증·중복 제거·1분 집계를 재현합니다.
- PostgreSQL에 경제 이벤트, 시장 데이터와 분석 결과를 멱등 저장합니다.
- 관측 결과는 인과관계나 주문 신호로 단정하지 않고 후속 백테스트 입력으로 제공합니다.

## 현재 분석 범위

- CPI 발표: 최근 실제 발표 12회
- 경제지표: `CPIAUCSL`, `CPILFESL`의 ALFRED 당시 vintage
- 시장 데이터: Alpaca Historical SIP `1Min` bar
- 종목: `SPY`, `QQQ`, `SMH`, `NVDA`
- 분석 window: 발표 전 60분, 발표 후 5·30·60분
- 저장소: PostgreSQL

2025년 10월 CPI는 실제 발표되지 않아 분석 목록에서 제외했습니다. 전망치 출처는 아직 연결하지 않았으므로 `forecast`와 `surprise`를 임의로 만들지 않습니다.

## 데이터 흐름

![전체 프로젝트 데이터 파이프라인 아키텍처](docs/diagrams/pipeline-architecture.png)

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

## 데이터 출처

| 데이터 | 공식 출처 | 역할 |
| --- | --- | --- |
| CPI 발표 날짜·시각 | [BLS CPI release schedule·archive](https://www.bls.gov/schedule/news_release/cpi.htm) | 이벤트 기준 시각과 대상 월 |
| 당시 CPI 값과 revision | [FRED/ALFRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | 미래 수정값이 섞이지 않는 point-in-time 값 |
| 발표 구간 실제 체결 | [Alpaca Historical Stock Trades](https://docs.alpaca.markets/reference/stocktradesingle) | Kafka·Spark 실시간 경로의 결정적 replay |
| 발표 전후 주식시장 | [Alpaca Historical Stock Bars](https://docs.alpaca.markets/us/v1.4.2/reference/stockbars) | SIP 1분 OHLCV·거래 수·VWAP |

## 실제 구현 결과

### A. 경제지표 발표 영향 분석 데이터

이 경로는 Alpaca가 이미 1분 단위로 집계한 Historical Bars API 데이터를 사용합니다.

| 데이터 | 한 행의 의미 | 수집·계산 범위 | 결과 |
| --- | --- | --- | ---: |
| BLS CPI 이벤트 | CPI 공식 발표 한 번 | 최근 실제 발표 12회 | 12행 |
| ALFRED 관측값 | 발표 당시 알려진 지수 한 개 | 12회 × CPI·근원 CPI 2개 | 24행 |
| Historical SIP 1분봉 | 종목 한 개의 1분 OHLCV | 12회 발표 구간 × `SPY`, `QQQ`, `SMH`, `NVDA` | 5,320행 |
| Event impact | 발표 한 번·종목·분석 window의 반응 | 12회 × 4종목 × 4개 window | 192행 |
| Matched baseline | 발표일과 비교할 과거 동일 요일·시각 반응 | 12회 × 4종목 × 4개 window × 3주 | 576행 |

Historical SIP 1분봉 5,320행은 **여러 발표일과 4개 종목을 합한, 이미 집계된 1분봉 전체**입니다. 거래가 없던 분은 임의로 생성하지 않았기 때문에 이론상 최대 행 수보다 적습니다.

### B. Kafka·Spark 원시 거래 처리 데이터

이 경로는 Historical Trades API의 개별 체결을 Kafka로 보내고 Spark가 직접 1분봉으로 집계합니다. 위의 5,320행과 행 단위가 다릅니다.

| 데이터 | 한 행의 의미 | 수집·계산 범위 | 결과 |
| --- | --- | --- | ---: |
| Historical SIP 원시 체결 | NVDA에서 실제로 발생한 개별 체결 한 건 | 2026-08-12 CPI 한 번·NVDA 한 종목·`[07:30, 09:31) ET` | 58,036행 |
| volume·trade_count 반영 체결 | provider 조건상 거래량·거래 건수에 포함되는 체결 | 위와 동일한 58,036행을 전처리 | 58,034행 |
| OHLC·VWAP 가격 형성 체결 | provider 조건상 대표 가격 형성에 사용하는 체결 | 위와 동일한 58,036행을 전처리 | 8,752행 |
| Spark 재구성 1분봉 | NVDA의 1분 OHLCV 한 개 | 위 원시 체결을 121개 event-time 분으로 집계 | 121행 |

```text
한 CPI 발표일의 NVDA 원시 체결 58,036행
→ 거래 조건 적용: trade_count 58,034행 / 가격 형성 8,752행
→ Spark 1분 집계 121행

이 121행은 12회 발표·4종목의 Historical SIP 1분봉 5,320행 중
같은 날짜·종목·시간 범위의 provider 121행과 비교해 정확성을 검증합니다.
```

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

## 다음 단계

1. 미국 거래일과 주요 경제 발표 calendar로 matched baseline 정제
2. 장전 반응과 첫 정규장 반응 분리
3. BLS 발표문의 월간·연간 actual 구조화
4. 검증 가능한 전망치 출처가 확보되면 surprise 분석 추가
5. Airflow로 수집·재실행·품질 검사를 자동화

## 구현·과제 증거

README는 프로젝트 전체 구조와 실행 진입점만 설명합니다. 회차별 요구사항, 메시지 명세, 상세 실행 명령과 검증 숫자는 아래 문서에서 관리합니다.

- [4차시 Kafka·Spark 과제 문서](docs/kafka-spark-assignment.md)
- [CPI 구간 Kafka·Spark 실행 결과](docs/test-results/2026-08-24-cpi-kafka-spark.md)
- [재현 명령과 PostgreSQL 검증 SQL 안내](docs/evidence/cpi-kafka-spark/README.md)
- [과제 제출 체크리스트](docs/submission-checklist.md)

## 문서

- [문서 전체 안내](docs/README.md)
- [데이터 소스](docs/data-source-catalog.md)
- [데이터 수명주기](docs/data-lifecycle.md)
- [설계 결정](docs/design-decisions.md)
- [4주 실행 계획](PROJECT_PLAN.md)

## 면책 및 출처 고지

이 프로젝트는 교육·연구 목적이며 투자 조언이 아닙니다. 계좌·주문 API를 호출하지 않습니다.

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.
