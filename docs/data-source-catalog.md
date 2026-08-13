# API Data Source Catalog

상태: initial contract, smoke test required

검증일: 2026-08-13

이 문서는 각 API가 **무엇을 제공하는지**, 그중 MVP가 **무엇을 가져오고 무엇을 제외하는지**, 받은 raw field를 **어디에 사용하는지**를 정의한다. 요금제·권한·schema는 구현 직전과 발표 전에 공식 문서와 실제 계정으로 다시 검증한다.

## 1. 한눈에 보는 선택

| API/Source | 제공 범위 | MVP에서 가져오는 것 | 제외 또는 후속 |
| --- | --- | --- | --- |
| Alpaca Real-time Stock WebSocket | trade, quote, minute/updated/daily bar, trading status, LULD 등 | IEX feed의 raw `trade` event | quote와 provider bar는 비교용 smoke test만, 나머지 channel은 제외 |
| Alpaca Historical Stock REST | trade, quote, bar와 IEX/SIP 등 feed 선택 | IEX/SIP `1Min` bar, 과거 20거래일 warm-up과 지연 검증 | historical raw trade/quote 장기 backfill 제외 |
| Alpaca Asset/Calendar/Clock | symbol metadata, 거래일, open/close, 현재 개장 상태 | active/tradable symbol 확인, 정규장·휴장·조기종료 판정 | 주문 가능성·margin 정보는 사용하지 않음 |
| FRED API | series metadata, observations, revisions/vintage, releases | 9개 series metadata와 observations | 서버측 임의 변환과 forecast 추정 제외 |
| Alpaca News REST/WebSocket — 선택 | 기사 metadata, symbol, headline, summary, content, URL | 구현 시 metadata와 summary, URL, symbols | 전체 본문 저장·재배포 제외 |
| Groq — 선택 | 입력 text에 대한 LLM output | 뉴스의 제한된 structured classification | 시장 원천 데이터 공급자가 아니며 가격/거시 데이터를 제공하지 않음 |
| Replay fixture | 우리가 만든 raw event | 정상·급등·중복·지연·오류 payload | 외부 API가 아님 |

## 2. Alpaca Real-time Stock WebSocket

공식 문서: [Real-time Stock Data](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)

MVP endpoint:

```text
wss://stream.data.alpaca.markets/v2/iex
```

### API가 제공하는 주요 channel

| Channel | 제공 데이터 | MVP 결정 |
| --- | --- | --- |
| `trades` | 개별 체결의 symbol, trade id, exchange, price, size, condition, timestamp, tape | **사용** |
| `quotes` | bid/ask exchange, price, size, quote condition, timestamp, tape | P0 제외 |
| `bars` | 직전 1분 OHLCV, VWAP, trade count | 자체 Spark 집계 검증용 smoke test만 |
| `updatedBars` | 늦게 도착한 trade로 수정된 이전 minute bar | late-event 비교 실험에만 사용 가능 |
| `dailyBars` | 당일 누적 daily bar | P0 제외 |
| trading status/LULD 등 | 거래 정지·상태·가격 밴드 event | 후속 위험 상태 기능 |

### P0 raw trade schema

| Raw field | 의미 | 내부 사용 |
| --- | --- | --- |
| `T` | message type, trade는 `t` | event type 검증 |
| `S` | symbol | Kafka key와 `symbol` |
| `i` | provider trade ID | `source_event_id`, deterministic dedup |
| `x` | 체결 exchange code | provenance와 condition 검증 |
| `p` | 체결 가격 | OHLC와 VWAP |
| `s` | 체결 수량 | volume과 VWAP |
| `c` | trade condition 배열 | bar 포함·제외 정책 |
| `t` | nanosecond RFC-3339 event timestamp | event-time window와 순서 |
| `z` | tape | provenance와 품질 분석 |

Kafka `raw.market.v1`은 이 provider payload를 수정하지 않고 common envelope의 `payload`에 넣는다. Collector는 인증·구독·재연결, 수신 시각과 source metadata, 결정적 `event_id`만 추가한다. 실제 field rename, type validation, condition filter는 Spark가 담당한다.

```json
{
  "event_id": "alpaca:trade:NVDA:12345",
  "event_type": "market.trade.raw",
  "schema_version": 1,
  "source": "alpaca",
  "feed": "iex",
  "ingested_at": "2026-08-13T14:00:03.100Z",
  "payload": {
    "T": "t",
    "S": "NVDA",
    "i": 12345,
    "x": "V",
    "p": 182.10,
    "s": 100,
    "c": ["@"],
    "t": "2026-08-13T14:00:03Z",
    "z": "C"
  }
}
```

## 3. Alpaca Historical Stock REST

공식 문서: [Historical Stock Bars](https://docs.alpaca.markets/us/v1.4.2/reference/stockbars), [Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)

MVP endpoint:

```text
GET https://data.alpaca.markets/v2/stocks/bars
```

### 요청에서 지정할 수 있는 값

| Parameter | 제공 기능 | MVP 값 |
| --- | --- | --- |
| `symbols` | comma-separated 종목 | 최종 allowlist 약 22개 |
| `timeframe` | minute/hour/day/week/month 집계 | `1Min` |
| `start`, `end` | RFC-3339/날짜 범위 | warm-up 20거래일 또는 reconciliation window |
| `feed` | `iex`, `sip` 등 data feed | 요청 목적에 따라 명시적으로 `iex` 또는 `sip` |
| `adjustment` | raw/split/dividend 등 조정 | P0 `raw`; corporate action 영향은 품질 이슈로 기록 |
| `asof` | symbol rename 기준일 | 필요 시 명시, 기본 current |
| `limit`, `page_token` | 페이지당 결과와 다음 페이지 | `next_page_token`이 없을 때까지 반복 |
| `sort` | 시간 정렬 | `asc` |

### bar에서 가져오는 값

| Raw field | 의미 | 저장 필드 |
| --- | --- | --- |
| `t` | bar 시작 timestamp | `bar_start` |
| `o`, `h`, `l`, `c` | open/high/low/close | 동일 이름 |
| `v` | volume | `volume` |
| `n` | trade count | `trade_count` |
| `vw` | VWAP | `vwap` |

Historical IEX bar는 IEX feature warm-up, SIP bar는 SIP feature warm-up과 실시간 경고의 지연 검증에 사용한다. 무료 SIP 계약은 `end <= now-15m`이며 실제 DAG는 5분 safety margin을 둬 `window_end <= now-20m`을 조회한다. 서로 다른 feed의 row와 baseline을 합치거나 덮어쓰지 않는다.

## 4. Alpaca Asset, Calendar, Clock

공식 문서: [Asset by Symbol](https://docs.alpaca.markets/us/reference/get-v2-assets-symbol_or_asset_id), [Market Calendar](https://docs.alpaca.markets/us/reference/calendar-1), [Market Clock](https://docs.alpaca.markets/us/reference/clock-1)

| API | 가져올 값 | 사용 목적 |
| --- | --- | --- |
| Asset | symbol/id, asset class, exchange, status, tradable 여부 | 잘못되거나 비활성인 symbol을 allowlist에서 제외 |
| Calendar | trading date, market open/close, 조기 종료 정보 | 정규장 window, 휴장일, early close 판정 |
| Clock | current timestamp, `is_open`, next open/close | collector와 dashboard의 현재 session 상태 |

주문·계좌 기능은 사용하지 않는다. Calendar 결과가 세션 판단의 기준이고, 고정된 평일 시간만으로 거래일을 추정하지 않는다.

## 5. FRED API

공식 문서: [FRED API Overview](https://fred.stlouisfed.org/docs/api/fred/overview.html), [Series Observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [Real-Time Periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)

MVP endpoint:

```text
GET https://api.stlouisfed.org/fred/series
GET https://api.stlouisfed.org/fred/series/observations
```

### API가 제공하는 값

Series metadata:

```text
id, title, observation_start/end,
frequency, units, seasonal_adjustment,
last_updated, notes, realtime_start/end
```

Observation:

```text
realtime_start, realtime_end, date, value
```

`value`는 문자열이며 결측은 `.`으로 올 수 있으므로 `.`을 `null`로 바꾸고 나머지를 Decimal 계열 숫자로 검증한다. `date`는 관측 대상일이지 반드시 실제 발표 timestamp는 아니다. revision을 추적하기 위해 realtime/vintage 정보를 버리지 않는다. 서버측 `units` 변환은 사용하지 않고 `units=lin` 원값을 저장한 뒤 필요한 변화량을 애플리케이션에서 계산한다.

### 선택한 9개 series

| Series | 공식 의미 | 빈도·단위 | 프로젝트 활용 |
| --- | --- | --- | --- |
| [`CPIAUCSL`](https://fred.stlouisfed.org/series/CPIAUCSL) | 전체 도시 소비자물가지수 | 월간, SA index | 최근 값과 직전 발표 대비 변화 |
| [`CPILFESL`](https://fred.stlouisfed.org/series/CPILFESL) | 식품·에너지 제외 CPI | 월간, SA index | 근원 물가 환경 |
| [`PCEPI`](https://fred.stlouisfed.org/series/PCEPI) | PCE 물가지수 | 월간, SA index | PCE 물가 환경 |
| [`PCEPILFE`](https://fred.stlouisfed.org/series/PCEPILFE) | 식품·에너지 제외 PCE | 월간, SA index | 근원 PCE 환경 |
| [`UNRATE`](https://fred.stlouisfed.org/series/UNRATE) | 실업률 | 월간, SA percent | 고용 환경 |
| [`DFF`](https://fred.stlouisfed.org/series/DFF) | Effective Federal Funds Rate | 일간, percent | 정책금리 환경 |
| [`DGS2`](https://fred.stlouisfed.org/series/DGS2) | 2년 미 국채 constant maturity 금리 | 일간, percent | 단기 금리 환경 |
| [`DGS10`](https://fred.stlouisfed.org/series/DGS10) | 10년 미 국채 constant maturity 금리 | 일간, percent | 장기 금리와 `DGS10-DGS2` |
| [`VIXCLS`](https://fred.stlouisfed.org/series/VIXCLS) | CBOE VIX 종가 | 일간 close, index | 시장 변동성 환경 |

FRED는 실시간 주가 feed가 아니다. Alert 시각 이전에 발표·수집되어 이용 가능했던 최신 observation만 배경 정보로 붙이며, 인과관계나 forecast consensus를 만들지 않는다.

## 6. Alpaca News — Optional

공식 문서: [News REST](https://docs.alpaca.markets/us/reference/news-3), [Real-time News](https://docs.alpaca.markets/us/docs/streaming-real-time-news)

REST endpoint와 WebSocket:

```text
GET  https://data.alpaca.markets/v1beta1/news
WSS  wss://stream.data.alpaca.markets/v1beta1/news
```

제공 field:

```text
id, headline, summary, author,
created_at, updated_at,
content, url, symbols, source
```

REST는 `start`, `end`, `symbols`, `sort`, `limit`, `include_content`, `page_token` 등을 지원한다. 구현한다면 `symbols=allowlist`, `include_content=false`로 시작하고 id/headline/summary/URL/symbol/source/timestamp metadata만 저장한다. provider 이용 조건을 확인하기 전에는 HTML 본문을 장기 저장하거나 재배포하지 않는다.

## 7. Groq LLM — Optional Processor

Groq는 데이터셋이나 시장 데이터 API가 아니다. News metadata/summary를 입력받아 다음처럼 제한된 JSON으로 구조화하는 처리 후보다.

```text
event_type, category, sector, sentiment,
importance, affected_assets, expected_horizon,
summary, model_confidence
```

LLM 출력은 원천 사실이 아니며 schema validation을 통과한 파생 데이터다. 시장 가격, FRED observation 또는 alert 값을 생성·수정하지 않는다.

## 8. Replay Fixture — Internal Source

Replay는 외부 API가 아니라 Alpaca raw trade envelope와 같은 계약으로 만든 입력이다.

```text
normal trades
duplicate trade id
out-of-order timestamp
late within watermark
too-late event
invalid field/type
price and volume spike
```

각 fixture에는 입력 event뿐 아니라 기대 OHLCV, DLQ code와 alert 결과가 함께 있어야 한다. live credential이나 장 운영 시간과 무관하게 Kafka→Spark→PostgreSQL을 검증하는 데 사용한다.

## 9. 구현 전 Contract Smoke Test

| Source | 확인할 것 |
| --- | --- |
| Alpaca IEX WebSocket | 22종목 subscription, raw field/type, trade ID 안정성, condition/tape, reconnect |
| Alpaca Historical | IEX/SIP 권한, `end` 제한, pagination, bar field, `adjustment=raw` |
| Asset/Calendar/Clock | inactive symbol, holiday, early close, DST, current session |
| FRED | API key, 9개 series metadata/frequency/units, `.` 결측, revision field, 429 |
| News — 선택 | entitlement, symbol filter, content 제외, pagination, storage terms |

실제 응답 fixture의 hash와 smoke-test 날짜를 남기고 문서 예시와 다르면 provider adapter contract를 먼저 갱신한다.
