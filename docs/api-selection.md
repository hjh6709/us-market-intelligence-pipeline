# Data API and Processing Platform Selection

검증일: **2026-08-13**

원칙: 구현 직전과 발표 전 다시 공식 문서를 확인한다. 정책·가격·모델·quota는 config나 문서의 숫자를 영구 진실로 취급하지 않는다.

## 1. Decision summary

| 역할 | MVP 선택 | 대체 방식 | 결정 |
| --- | --- | --- | --- |
| Realtime market data | Alpaca Trading API Basic / IEX | replay fixture | 채택 |
| Market reconciliation | Alpaca historical SIP, `end <= now - 15m` | stored SIP fixture | 채택 |
| News | Alpaca News | stored fixture | 선택 구현, account entitlement smoke test 필요 |
| Macro | FRED API | stored response fixture | 채택 |
| LLM | Groq API | deterministic stub/cache | 선택 구현 1차 후보, account quota smoke test 필요 |
| Market calendar | Alpaca Calendar/Clock | calendar fixture | 채택 |

한 기능에 상용 provider 여러 개를 동시에 구현하지 않는다. adapter 계약과 fixture가 교체 가능성을 제공하며, 실제 대체 공급자는 필요가 생겼을 때 추가한다. 무료 조건에서 22개 종목의 전체 미국시장 SIP raw trade를 실시간 제공하는 검증된 공급자는 없으므로, 전체시장 실시간 경고를 MVP 요구사항으로 두지 않는다.

## 2. Alpaca Market Data

공식 문서상 Trading API Basic은 무료이며 미국 주식/ETF를 지원한다. 다만 실시간 주식 범위는 IEX이고 WebSocket subscription은 30 symbols, historical API는 200 calls/min이며 최근 15분 historical data에 제한이 있다. 구독하지 않은 계정도 historical SIP query의 `end`가 최소 15분 이전이면 조회할 수 있으므로, 이를 실시간 IEX 결과의 지연 정합성 검증에 사용한다.

MVP 결정:

- 22 symbols만 subscribe한다.
- feed는 명시적으로 `iex`로 저장·표시한다.
- trade channel만 P0로 사용한다. quote는 실제 signal requirement가 생길 때 추가한다.
- IEX feature와 threshold baseline은 historical IEX만 사용한다.
- SIP feature와 threshold baseline은 historical SIP만 사용하며 IEX baseline과 혼합하지 않는다.
- 실시간 alert는 `PRELIMINARY_IEX`, SIP 정합성 검사 후 `CONFIRMED_SIP` 또는 `REJECTED_AFTER_RECONCILIATION`으로 기록한다.
- startup warm-up은 feed가 일치하는 historical bar/replay를 사용하고 historical 최신 15분 제한을 가정한다.
- 한 계정의 active stream connection 제한을 고려해 collector 하나가 구독을 소유한다.
- SIP 수준 coverage나 전체시장 VWAP이라고 표현하지 않는다.
- historical SIP 수집은 latest endpoint를 사용하지 않고 `end <= now - 15m`인 닫힌 1분 window만 요청한다.
- 실제 Airflow DAG는 5분 safety margin을 더해 초기에는 `window_end <= now - 20m`인 미수집 구간을 15분마다 조회한다.

공식 출처:

- [About Market Data API — subscription plans](https://docs.alpaca.markets/us/docs/about-market-data-api)
- [Real-time Stock Data — feeds, channels and schemas](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)
- [WebSocket Stream — connection and subscription behavior](https://docs.alpaca.markets/us/docs/streaming-market-data)
- [Market Data FAQ — IEX/SIP 차이와 무료 historical SIP 제한](https://docs.alpaca.markets/us/docs/market-data-faq)
- [Market Data FAQ — trade condition별 bar 집계 규칙](https://docs.alpaca.markets/us/docs/market-data-faq#how-are-bars-aggregated)

실행 전 smoke check:

1. 계정 생성/지역 eligibility 및 API key 발급
2. `v2/iex` 인증
3. 22 symbols trade subscription acknowledgement
4. 장전/장후 trade 수신 여부와 실제 coverage
5. 406 connection limit, 429, reconnect 동작
6. `feed=sip`, `end <= now - 16m` historical 1분 bar 조회 권한
7. IEX raw trade 직접 집계와 Alpaca IEX 1분 bar의 condition 적용 결과
8. 동일 window의 IEX/SIP close·volume 차이와 reconciliation upsert

### 무료 대안 재검토

| 후보 | 무료 범위에서 확인된 사실 | 미선택 이유 |
| --- | --- | --- |
| KIS Open API | 미국 0분 지연 가격, WebSocket 전체 상품 합산 41건 | 계좌·서비스 신청이 필요하고 `HDFSCNT0`에 고유 trade id, tape, trade condition이 없으며 미국 feed의 전체 SIP coverage가 공개 계약으로 확인되지 않음 |
| Finnhub Free | 60 REST calls/min, WebSocket 50 symbols, price/time/volume/condition | 무료 표에 historical OHLC/tick data가 없고 WebSocket event에 고유 trade id, exchange, tape가 없으며 전체 SIP coverage가 명시되지 않음 |
| yfinance | 동기/비동기 Yahoo price WebSocket과 historical download | Yahoo가 승인한 공식 market-data SDK가 아니고 개인 용도로 안내되며 raw trade id/condition/tape 계약이 없음 |
| Twelve Data Basic | 실시간 US equity와 8 trial WebSocket credits | 22 symbols 동시 구독 불가 |
| Massive Basic | EOD와 제한된 historical data | 무료 WebSocket 없음 |

무료 후보의 종목 수만 비교하지 않는다. P0는 raw event identity, source/feed 투명성, 22종목 재현성, feed가 일치하는 historical baseline을 더 높은 우선순위로 평가한다. 이 기준에서는 Alpaca Basic IEX + 지연 SIP reconciliation이 최선이다.

대안 공식 출처:

- [KIS 공식 Open API 샘플 — 해외주식 WebSocket schema](https://github.com/koreainvestment/open-trading-api/blob/main/examples_user/overseas_stock/overseas_stock_functions_ws.py)
- [KIS WebSocket 합산 41건 안내](https://apiportal.koreainvestment.com/community/10000000-0000-0011-0000-000000000001/post/d0d1a83f-6f8d-4437-9700-6d26702fd989)
- [Finnhub Free plan](https://finnhub.io/pricing)
- [Finnhub trade WebSocket schema](https://finnhub.io/docs/api/websocket-trades)
- [yfinance repository and usage notice](https://github.com/ranaroussi/yfinance)
- [Twelve Data pricing](https://twelvedata.com/pricing)
- [Massive stocks pricing](https://massive.com/stocks)

## 3. Alpaca News

Alpaca 공식 문서는 historical news가 2015년부터 제공되고 평균 130개 이상의 기사가 있으며 현재 Benzinga가 데이터를 제공한다고 설명한다. 실시간 news WebSocket endpoint와 article schema도 제공한다.

MVP 결정:

- 별도 news vendor를 추가하지 않고 기존 Alpaca credentials/adapter를 재사용한다.
- 22 symbol 관련 기사만 받아 dedup/relevance filter한다.
- provider news id, headline, summary, URL, symbol, timestamps를 우선 저장한다.
- article full content 저장·재표시는 실제 entitlement와 terms를 확인한 후 결정한다.
- news access가 계정에서 허용되지 않으면 live 기능을 억지로 다른 무료 API로 대체하지 않고 fixture demo + risk 기록으로 진행한다.

공식 출처:

- [Historical News Data](https://docs.alpaca.markets/us/docs/historical-news-data)
- [Real-time News](https://docs.alpaca.markets/us/docs/streaming-real-time-news)

실행 전 smoke check:

1. Basic 계정의 historical/realtime news entitlement
2. symbol filter 결과와 pagination/cursor
3. 원문 저장·표시·캐시 관련 terms
4. rate/connection limit과 재연결

## 4. FRED

FRED API는 HTTPS REST로 FRED/ALFRED series와 observations를 JSON/XML 등으로 조회할 수 있다. 모든 web service request에는 계정에서 발급받은 API key가 필요하다. 공식 오류 문서는 rate limit 초과 시 429를 반환할 수 있다고 안내하지만 고정 숫자를 보장하지 않는다.

MVP 결정:

- daily Airflow DAG로 필요한 series만 증분 조회한다.
- response의 observation/vintage 정보를 보존한다.
- 429에서는 bounded retry하며 호출량을 보수적으로 유지한다.
- 앱에는 FRED required notice를 표시한다.
- forecast consensus provider로 간주하지 않는다.

공식 출처:

- [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)
- [FRED API key requirements](https://fred.stlouisfed.org/docs/api/api_key.html)
- [Series observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
- [API errors and 429 behavior](https://fred.stlouisfed.org/docs/api/fred/errors.html)
- [FRED API Terms of Use](https://fred.stlouisfed.org/docs/api/terms_of_use.html)

필수 notice:

> This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.

## 5. Groq LLM — Optional

Groq 공식 문서는 Free Plan limits를 모델별 RPM/RPD/TPM/TPD로 공개하며, 정확한 조직 한도는 account limits page에서 확인하라고 안내한다. Structured Outputs는 지원 모델에서 JSON Schema를 사용할 수 있고 strict mode는 현재 일부 모델로 제한된다.

MVP 결정:

- `LLMProvider` 뒤에 Groq adapter 하나만 구현한다.
- 구현 시점에 Free Plan에서 사용 가능한 structured-output model을 선택해 config로 고정한다.
- Pydantic schema validation은 provider 보장과 별개로 항상 수행한다.
- article length, daily calls, retries를 애플리케이션에서 더 낮게 제한한다.
- quota 초과 시 자동으로 paid tier나 다른 paid provider로 전환하지 않는다.
- free model이 사라지거나 계정 quota가 불충분하면 cached/stub demo를 사용하고 event component confidence를 낮춘다.

2026-08-13 공식 표에는 예를 들어 `openai/gpt-oss-20b`/`120b`의 Free Plan limit가 게시되어 있고 strict JSON Schema 지원 모델로도 기재되어 있다. 숫자는 변경될 수 있으므로 코드에는 복사하지 않고 account limits에서 재검증한다.

공식 출처:

- [Groq rate limits](https://console.groq.com/docs/rate-limits)
- [Groq Structured Outputs](https://console.groq.com/docs/structured-outputs)
- [Groq API reference](https://console.groq.com/docs/api-reference)

실행 전 smoke check:

1. Free Plan 및 결제수단 미연결 상태 확인
2. 선택 model id와 strict structured output 지원
3. account-specific RPM/RPD/TPM/TPD
4. 한국에서 사용 가능 여부와 data retention/privacy 조건
5. 429 headers와 bounded retry

## 6. Market Calendar and Clock

Alpaca Calendar는 거래일과 조기 종료를 포함한 open/close 정보를, Clock은 현재 시장 상태와 다음 open/close 정보를 제공한다. 2026년에는 market code 범위가 확장되고 endpoint version 문서도 변경되었으므로 SDK가 사용하는 Trading API 버전을 smoke test로 확인한다.

MVP 결정:

- 거래일/open/close는 calendar adapter가 제공한다.
- `OPENING` 같은 전략 window만 애플리케이션에서 파생한다.
- 응답을 일 단위 cache하고 DST, holiday, early close fixture를 테스트한다.

공식 출처:

- [Trading API market calendar](https://docs.alpaca.markets/us/reference/calendar-1)
- [Trading API market clock](https://docs.alpaca.markets/us/reference/clock-1)
- [2026 calendar/clock market code update](https://docs.alpaca.markets/us/changelog/2026-06-04-market-codes-e8e76b9)

## 7. Spark Structured Streaming — Required Processing Engine

Spark는 외부 API가 아니지만 데이터·플랫폼 선택 근거를 한곳에서 추적하기 위해 이 문서에 포함한다.

현재 22개 IEX 종목은 Python consumer로도 처리 가능할 가능성이 높다. 그럼에도 과정의 필수 기술과 산출물에 Spark가 명시되어 있으므로 local Structured Streaming을 P0 처리 엔진으로 선택한다.

P0 역할:

```text
Kafka raw.market.v1
→ explicit JSON schema parsing
→ validation and event-id deduplication
→ event-time watermark
→ symbol + 1-minute window OHLCV/VWAP/count
→ foreachBatch PostgreSQL upsert
```

선택 근거:

- Structured Streaming은 DataFrame/Dataset API로 streaming aggregation과 event-time window를 지원한다.
- Kafka source integration과 checkpoint를 사용해 consumer offset과 stateful query를 복구할 수 있다.
- `foreachBatch`는 micro-batch별 custom sink를 구현할 수 있지만 기본 write guarantee가 at-least-once이므로 PostgreSQL unique key/upsert가 필요하다.
- local mode로만 사용하고 별도 cluster는 load-test 결과가 필요성을 보일 때까지 만들지 않는다.

공식 출처:

- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/4.2.0/streaming/index.html)
- [Structured Streaming DataFrame/Dataset APIs — watermark and foreachBatch](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html)
- [Structured Streaming Kafka Integration](https://spark.apache.org/docs/4.2.0/streaming/structured-streaming-kafka-integration.html)

구현 전 smoke check:

1. Java, Python, PySpark, Kafka broker와 connector artifact 호환 버전
2. local mode에서 Kafka read와 checkpoint restart
3. PostgreSQL JDBC driver와 `foreachBatch` upsert 방식
4. watermark/output mode의 late-event fixture 결과
5. Docker/host memory와 shuffle partition 기본값

Spark 버전은 위 문서의 최신 숫자를 그대로 선택하지 않고 local runtime·connector 호환을 확인한 뒤 고정한다.

## 8. Reverification checklist

다음 세 시점에 이 문서 상단의 검증일을 갱신한다.

- 구현 시작 전 (Gate 0)
- live integration 시작 전 (각 provider milestone)
- 발표 리허설 전 (2026-09-10)

각 검증에서 기록할 것:

```text
provider / account tier / billing attached?
endpoint and API version
coverage/feed and symbol limit
historical availability
RPM/RPD/TPM or response headers
websocket connection limit
extended-hours/calendar behavior
data storage/display terms
selected model id and schema support
smoke-test timestamp and result
```

정책이 바뀌면 무료 범위 안에서 symbol/call volume을 줄인다. 사용자 승인 없이 유료 API로 전환하는 것은 허용하지 않는다.
