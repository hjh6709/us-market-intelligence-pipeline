# MVP Data Model and Event Contracts

상태: proposed

기준일: 2026-08-13

이 문서는 provider와 내부 처리 사이의 안정된 계약 및 PostgreSQL의 Stage A 논리 모델을 정의한다. 이 모델은 후속 자동매매 전략과 point-in-time backtest가 당시 이용 가능했던 입력을 재현할 수 있게 설계하지만, 주문·체결·포지션 schema는 이번 MVP 범위에 포함하지 않는다. 실제 migration의 이름·타입은 구현 중 테스트와 함께 확정한다.

P0 query pattern과 최소 index 후보는 [MVP 설계 결정](design-decisions.md#7-조회-패턴과-인덱스)을 기준으로 migration과 함께 검증한다.

## 1. 공통 원칙

- 모든 timestamp는 timezone-aware UTC다.
- source event time과 ingestion time을 분리한다.
- 숫자 누락을 0으로 대체하지 않는다.
- event/schema는 명시적으로 versioning한다.
- 원천 payload 전체를 애플리케이션 테이블에 복사하지 않는다.
- at-least-once delivery를 가정하고 deterministic id/unique key를 둔다.
- signal 계산에 사용한 값은 `as_of` 이후에 알려진 정보를 포함하지 않는다.
- `source`와 `feed`가 다른 bar·feature·baseline은 결합하거나 서로 덮어쓰지 않는다.
- 경제지표 event는 공식 발표 출처의 `released_at`과 FRED/ALFRED의 당시 vintage를 분리해 추적한다.
- 분석 결과에는 표본 수, market coverage와 비교 기준 version을 포함하며 시간적 동시성을 인과관계로 저장하지 않는다.

## 2. Common event envelope

```json
{
  "event_id": "sha256:canonical-source-feed-type-symbol-id-timestamp",
  "event_type": "market.trade.raw",
  "schema_version": 1,
  "source": "alpaca",
  "feed": "iex",
  "source_event_id": "12345",
  "event_timestamp": "2026-08-13T13:30:00.123456Z",
  "ingested_at": "2026-08-13T13:30:00.200000Z",
  "trace_id": "collector-connection-or-replay-run-id",
  "payload": {
    "T": "t",
    "S": "NVDA",
    "i": 12345,
    "x": "V",
    "p": 182.42,
    "s": 100,
    "c": ["@"],
    "t": "2026-08-13T13:30:00.123456Z",
    "z": "C"
  }
}
```

이 절이 `raw.market.v1` envelope의 **정본(canonical contract)** 이다. 필수 field는 `event_id`, `event_type`, `schema_version`, `source`, `feed`, `source_event_id`, `event_timestamp`, `ingested_at`, `payload`이며, `trace_id`는 collector 연결 또는 replay 실행 단위의 선택적 상관관계 ID다.

`event_id`는 `source`, `feed`, event type, symbol, provider trade ID와 원본 event timestamp의 canonical serialization을 hash해 만든다. 2026-08-19 smoke test에서 test stream이 다른 timestamp에 같은 trade ID를 반복했으므로 provider ID 하나의 전역 유일성을 가정하지 않는다. 무작위 UUID만으로 중복 제거하지 않는다. Collector는 Kafka routing과 결정적 ID를 만들기 위해 raw `T`, `S`, `i`, `t`만 읽을 수 있다. 이 단계에서는 provider field를 rename하거나 거래 조건을 해석하지 않으며 원본 JSON을 `payload`에 그대로 보존한다. 전체 type/schema 검증, condition filter와 normalized field mapping은 Spark가 담당한다. API별 raw field의 의미는 [API 데이터 소스 카탈로그](data-source-catalog.md)를 따른다.

## 3. Market trade

```json
{
  "event_id": "sha256:canonical-source-feed-type-symbol-id-timestamp",
  "source_event_id": "12345",
  "event_timestamp": "2026-08-13T13:30:00.123456Z",
  "symbol": "NVDA",
  "price": 182.42,
  "size": 100,
  "exchange": "V",
  "source": "alpaca",
  "feed": "iex",
  "conditions": ["@"]
}
```

Validation:

- symbol은 활성 allowlist에 있어야 한다.
- price > 0, size > 0.
- timestamp는 parse 가능한 aware timestamp여야 한다.
- 미래 허용 오차 또는 지나치게 오래된 event는 reason code와 함께 격리한다.

## 4. One-minute bar

```json
{
  "symbol": "NVDA",
  "bar_start": "2026-08-13T13:30:00Z",
  "timeframe": "1m",
  "open": 182.10,
  "high": 182.60,
  "low": 182.05,
  "close": 182.42,
  "volume": 12500,
  "trade_count": 240,
  "vwap": 182.31,
  "source": "alpaca_replay",
  "feed": "sip",
  "is_final": true,
  "condition_policy": "alpaca_sip_minute_v1",
  "spark_batch_id": 42,
  "updated_at": "2026-08-13T13:32:00Z"
}
```

Unique key: `(symbol, bar_start, timeframe, source, feed)`.

`alpaca_sip_minute_v1`은 Alpaca의 CTA/UTP sale-condition 표에 따라 각 raw trade가 OHLC 가격 형성 및 volume·trade_count에 반영되는지를 따로 결정한다. 여러 조건이 있으면 가장 엄격한 조건을 사용하며, 알려지지 않은 condition/tape 조합은 조용히 포함하지 않고 집계에서 제외해 별도 건수로 기록한다. VWAP는 가격과 거래량 모두 갱신 가능한 체결만 사용한다.

`is_final`은 configured watermark가 해당 window를 통과해 더 이상 정상 update 대상이 아님을 뜻한다. P0는 append output mode로 watermark를 통과한 final bar만 PostgreSQL에 저장한다. Watermark 안의 late event는 DB에 쓰기 전에 Spark state의 집계에 포함되고, 너무 늦은 event는 별도 metric/DLQ 정책을 따른다. 정상 재시작은 Spark checkpoint에서 복구하며, full historical rebuild는 Kafka retention 안의 raw event 또는 deterministic replay dataset을 사용한다. PostgreSQL bar만으로 raw trades를 완전히 재구성할 수 있다고 가정하지 않는다.

## 5. Macro observation and economic event

FRED 관측값:

```json
{
  "series_id": "DGS10",
  "observation_date": "2026-08-12",
  "value": 4.21,
  "unit": "Percent",
  "realtime_start": "2026-08-13",
  "realtime_end": "9999-12-31",
  "released_at": null,
  "source": "fred",
  "ingested_at": "2026-08-13T01:00:00Z"
}
```

Economic event:

```json
{
  "event_type": "CPI",
  "reference_period": "2026-07",
  "scheduled_at": "2026-08-12T12:30:00Z",
  "released_at": "2026-08-12T12:30:00Z",
  "previous": 2.8,
  "forecast": null,
  "actual": 3.0,
  "surprise": null,
  "unit": "percent_yoy",
  "release_source": "bls",
  "release_source_url": "https://www.bls.gov/schedule/news_release/cpi.htm",
  "value_source": "fred",
  "vintage_as_of": "2026-08-12",
  "ingested_at": "2026-08-12T12:35:00Z"
}
```

Rules:

- `surprise`는 forecast와 actual이 둘 다 있을 때만 `actual - forecast`다.
- previous를 forecast로 사용하지 않는다.
- observation date를 가짜 release timestamp로 변환하지 않는다.
- revision 추적을 위해 FRED/ALFRED realtime/vintage field를 보존한다.
- `released_at`은 BLS·BEA·Federal Reserve 등 공식 발표 출처에서 확인한다. FRED release date만으로 장중 공개 시각을 만들지 않는다.
- `actual`과 `previous`는 해당 시점에 이용 가능했던 값과 vintage를 연결한다.
- 공식 시각이나 당시 값이 확인되지 않으면 event study 대상에서 제외하고 reason code를 남긴다.

### Macro event impact

```json
{
  "impact_id": "sha256:...",
  "economic_event_id": "CPI|2026-07|2026-08-12T12:30:00Z",
  "symbol": "QQQ",
  "source": "alpaca",
  "feed": "sip",
  "session_scope": "EXTENDED_HOURS",
  "window_name": "POST_30M",
  "window_start": "2026-08-12T12:30:00Z",
  "window_end": "2026-08-12T13:00:00Z",
  "open_price": 100.0,
  "close_price": 101.2,
  "return_pct": 1.2,
  "volume": 24000,
  "realized_volatility": 0.18,
  "benchmark_symbol": "SPY",
  "benchmark_return_pct": 0.8,
  "market_relative_return_pct": 0.4,
  "coverage_status": "COMPLETE",
  "coverage_reason": "bars=30/30;endpoint_lag_minutes=0",
  "analysis_version": "cpi_sip_v1"
}
```

Unique key: `(economic_event_id, symbol, feed, post_window_minutes, analysis_version)`.

`return_post`, 거래량 비율과 실현 변동성은 같은 feed의 bar에서 계산한다. `market_excess_return`은 시장 ETF, `sector_excess_return`은 사전에 정한 섹터 ETF 대비 값이다. 한 event row는 인과관계 결론이 아니며 동일 event type의 여러 날짜를 집계한 report에 표본 수, 분포와 한계를 함께 기록한다.

## 6. News article

```json
{
  "news_id": "alpaca:987654",
  "source": "alpaca_benzinga",
  "source_article_id": "987654",
  "headline": "...",
  "summary": "...",
  "url": "https://example.com/article",
  "symbols": ["NVDA", "AMD"],
  "published_at": "2026-08-13T12:10:00Z",
  "updated_at": "2026-08-13T12:12:00Z",
  "ingested_at": "2026-08-13T12:12:05Z",
  "news_hash": "sha256:...",
  "processing_status": "PENDING"
}
```

`processing_status`:

`PENDING | FILTERED | CLASSIFIED | UNCLASSIFIED | BUDGET_EXHAUSTED`

원문 저장 여부는 provider terms를 확인해 구현한다. 필요하지 않으면 headline, summary, URL, symbol, hash만 저장한다.

## 7. LLM market event — Optional

```json
{
  "event_id": "uuid-or-deterministic-hash",
  "news_id": "alpaca:987654",
  "event_type": "EXPORT_RESTRICTION",
  "category": "REGULATION",
  "sector": "SEMICONDUCTOR",
  "sentiment": "NEGATIVE",
  "importance": 0.85,
  "affected_assets": ["NVDA", "AMD", "SMH"],
  "expected_horizon": "SHORT_TERM",
  "summary": "...",
  "model_confidence": 0.78,
  "provider": "groq",
  "model": "configured-model-id",
  "prompt_version": 1,
  "schema_version": 1,
  "classified_at": "2026-08-13T12:12:07Z"
}
```

Closed enums:

- sentiment: `POSITIVE | NEUTRAL | NEGATIVE | MIXED | UNKNOWN`
- horizon: `INTRADAY | SHORT_TERM | MEDIUM_TERM | UNKNOWN`
- category MVP set: `EARNINGS | PRODUCT | CONTRACT | REGULATION | M_AND_A | MACRO | GEOPOLITICAL | INDUSTRY | OTHER`

`importance`와 `model_confidence`는 `[0, 1]`. affected assets는 allowlist와 sector ETF에 교차 검증하며 LLM이 만든 임의 ticker는 별도 rejected list/metric으로 남긴다.

## 8. Technical feature snapshot

Logical fields:

```text
symbol, as_of, timeframe, source, feed,
return_1m, return_5m,
ema_20, ema_50, rsi_14, session_vwap, atr_14,
volume_change, volume_zscore,
sample_count, is_ready, calculated_at, feature_version
```

Unique key: `(symbol, as_of, timeframe, feature_version, source, feed)`.

IEX는 Spark가 `market_bars`를 확정한 뒤 Feature/Anomaly Engine이 snapshot을 만들고, SIP는 reconciliation batch가 같은 feature contract를 feed별로 계산한다. `is_ready=false`인 feature는 score 계산에서 0으로 처리하지 않는다. ATR은 OHLC bar, VWAP은 같은 market session의 누적 가격×거래량으로 계산한다.

## 9. Market reconciliation

```json
{
  "reconciliation_id": "sha256:...",
  "symbol": "NVDA",
  "bar_start": "2026-08-13T14:00:00Z",
  "timeframe": "1m",
  "iex_bar_key": "NVDA|2026-08-13T14:00:00Z|1m|alpaca|iex",
  "sip_bar_key": "NVDA|2026-08-13T14:00:00Z|1m|alpaca|sip",
  "iex_close": 182.42,
  "sip_close": 182.39,
  "close_diff_bps": 1.64,
  "iex_volume": 12500,
  "sip_volume": 48120,
  "iex_volume_ratio": 0.2598,
  "bar_comparison_status": "DIVERGED",
  "rule_version": 1,
  "reconciled_at": "2026-08-13T14:16:05Z"
}
```

Unique key: `(symbol, bar_start, timeframe, rule_version)`.

`bar_comparison_status`는 `MATCHED | DIVERGED | MISSING_IEX | MISSING_SIP`다. 이 값은 두 bar의 차이를 나타낼 뿐 alert 확정 여부가 아니다. SIP 조회는 window end가 현재 시각보다 최소 15분 이전인 닫힌 window에만 수행한다. reconciliation은 IEX와 SIP 원천 bar를 수정하지 않는 파생 증거이며, 같은 window를 재실행해도 한 행으로 upsert되어야 한다.

Alert 단위 SIP 재평가는 별도 계약으로 저장한다.

```json
{
  "reconciliation_id": "sha256:...",
  "alert_id": "alert:...",
  "sip_feature_version": 1,
  "sip_observations": {
    "return_5m": 0.029,
    "volume_zscore": 3.7,
    "atr_normalized_move": 2.0
  },
  "decision": "CONFIRMED_SIP",
  "reason_codes": ["RETURN_SPIKE", "VOLUME_SPIKE"],
  "rule_version": 1,
  "evaluated_at": "2026-08-13T14:16:05Z"
}
```

`alert_reconciliations`의 unique key는 `(alert_id, rule_version)`이다. `decision`만 anomaly alert 상태를 전이시키며 bar의 `MATCHED/DIVERGED`만으로 확정 또는 기각하지 않는다.

## 10. Anomaly alert

```json
{
  "alert_id": "uuid-or-deterministic-hash",
  "symbol": "NVDA",
  "alert_type": "PRICE_VOLUME_SPIKE",
  "event_timestamp": "2026-08-13T14:00:00Z",
  "severity": "HIGH",
  "observations": {
    "return_5m": 0.032,
    "volume_zscore": 4.1,
    "atr_normalized_move": 2.2
  },
  "reason_codes": ["RETURN_SPIKE", "VOLUME_SPIKE"],
  "threshold_version": 1,
  "source": "alpaca",
  "feed": "iex",
  "baseline_feed": "iex",
  "status": "PRELIMINARY_IEX",
  "reconciliation_id": null,
  "reconciled_at": null,
  "created_at": "2026-08-13T14:00:02Z"
}
```

같은 symbol, anomaly window, alert type, threshold version, source, feed에서 동일한 입력은 같은 결과를 만들어야 한다. IEX alert의 허용 상태 전이는 다음과 같다.

```text
PRELIMINARY_IEX
├── CONFIRMED_SIP
└── REJECTED_AFTER_RECONCILIATION
```

SIP 조회 실패나 bar 누락은 확정 또는 기각 사유가 아니므로 `PRELIMINARY_IEX`를 유지한다. 상태 전이는 idempotent해야 하며 `alert_status_history`에 이전 상태, 다음 상태, reconciliation id, rule version, 시각을 기록한다. Alert는 관측된 이상 변화다. 후속 전략이 입력 후보로 사용할 수 있지만 alert 하나만으로 매수·매도 주문을 만들지는 않는다.

## 11. Market signal — Optional

```json
{
  "signal_id": "uuid",
  "as_of": "2026-08-13T14:00:00Z",
  "universe_version": 1,
  "regime": "BULL",
  "risk_state": "NORMAL",
  "score": 0.41,
  "confidence": 0.74,
  "technical_score": 0.55,
  "macro_score": 0.10,
  "event_score": 0.40,
  "effective_weights": {
    "technical": 0.4,
    "macro": 0.3,
    "event": 0.3
  },
  "reasons": [
    {
      "code": "SEMI_TREND_POSITIVE",
      "message": "SMH and SOXX are above session VWAP",
      "observations": {"SMH": 0.32, "SOXX": 0.28}
    }
  ],
  "input_freshness": {
    "market_seconds": 4,
    "macro_seconds": 86400,
    "event_seconds": 900
  },
  "signal_version": 1,
  "created_at": "2026-08-13T14:00:02Z"
}
```

`confidence`는 수익 확률이 아니다. 입력 coverage, freshness, agreement, 품질을 요약한 시스템 신뢰도다. 후속 매매 전략은 별도 기대수익·위험·비용 평가를 수행해야 하며 이 의미를 API/UI에 명시한다.

## 12. PostgreSQL logical tables

| Table | 주요 목적 | Unique/idempotency key |
| --- | --- | --- |
| `symbols` | universe, role, active interval | `symbol` |
| `market_bars` | 1분 OHLCV | symbol/bar/timeframe/source/feed |
| `technical_features` | 계산 snapshot | symbol/as_of/version/source/feed |
| `market_bar_reconciliations` | 같은 window의 IEX/SIP 차이와 판정 | symbol/bar/timeframe/rule_version |
| `alert_reconciliations` | SIP feature로 alert 규칙을 다시 평가한 증거와 결정 | alert_id/rule_version |
| `macro_series` | series metadata | series_id |
| `macro_observations` | 값과 vintage | series/date/realtime_start |
| `economic_events` | release event, optional forecast | event_type/reference/released/source |
| `macro_event_impacts` | 발표 전후 종목별 반응과 baseline/control 비교 | event/symbol/feed/window/analysis_version |
| `macro_event_baseline_impacts` | 이벤트별 1·2·3주 전 동일 요일·현지시각 비교값 | event/week-offset/symbol/window/baseline-version |
| `macro_impact_reports` | 동일 발표 유형의 반복 사례 집계와 한계 | event_type/universe/period/analysis_version |
| `news_articles` (optional) | normalized news와 처리상태 | source/source_article_id, news_hash |
| `llm_analyses` (optional) | cache/audit | news_hash/prompt/schema/provider/model |
| `market_events` (optional) | validated structured events | event_id |
| `anomaly_alerts` | 설명 가능한 가격·거래량 이상 징후와 현재 검증 상태 | alert_id, symbol/window/type/version/source/feed |
| `alert_status_history` | preliminary/confirmed/rejected 전이 감사 기록 | alert_id/to/reconciliation_id |
| `market_signals` (optional) | 후속 전략이 평가할 composite snapshot; 주문 아님 | as_of/signal_version/universe_version |
| `pipeline_status` | source/Spark query/Airflow freshness | component/instance |
| `dead_letters` (optional) | queryable DLQ index | event_id/error_code |

Kafka raw event를 PostgreSQL에 별도 tick table로 장기 저장하지 않는다.

주문 의도, broker order, fill, position, cash, exposure와 risk-limit 이력은 Stage A table에 넣지 않는다. 해당 계약은 point-in-time backtest와 paper trading이 검증된 후 별도 versioned execution model로 설계한다.

## 13. Data quality reason codes

최소 reason code:

```text
UNKNOWN_SYMBOL
INVALID_PRICE
INVALID_VOLUME
INVALID_TIMESTAMP
FUTURE_TIMESTAMP
DUPLICATE_EVENT
OUT_OF_ORDER_EVENT
SOURCE_STALE
INSUFFICIENT_WARMUP
RECONCILIATION_PENDING
SIP_BAR_MISSING
IEX_SIP_PRICE_DIVERGENCE
IEX_SIP_VOLUME_DIVERGENCE
MISSING_MACRO_VALUE
OFFICIAL_RELEASE_TIME_MISSING
VINTAGE_UNAVAILABLE
INSUFFICIENT_EVENT_SAMPLES
PARTIAL_MARKET_COVERAGE
FORECAST_UNAVAILABLE
LLM_SCHEMA_INVALID
LLM_RATE_LIMITED
LLM_BUDGET_EXHAUSTED
```

validation 실패는 exception 문자열만 저장하지 않고 위 code, source, schema version, event id, 발생 시각을 남긴다. secret이나 전체 authorization metadata는 저장하지 않는다.

## 14. Retention

MVP 기본값은 [데이터 수집·수명주기](data-lifecycle.md)를 따른다.

- raw market Kafka: 24h
- IEX/SIP bars, features, alerts, reconciliation/history: PostgreSQL 90일 rolling
- FRED observations/vintage: MVP 자동 삭제 없음
- economic events, macro impact rows/reports: MVP 자동 삭제 없음
- optional news metadata: 30일, 기사 전체 본문 저장 안 함
- DLQ: 7일
- structured raw log: 14일
- 작은 deterministic fixture와 aggregate report: repository 보존
- 임시 live raw capture: 최종 발표 30일 후 삭제

90일 cleanup은 feed와 business key를 보존한 batch delete로 수행하고, 실행 전 row/date 범위와 backup 상태를 기록한다. 보존 기간은 실제 byte 수를 측정한 뒤 변경할 수 있지만 변경 근거를 남긴다. S3/Parquet 장기 archive는 stretch goal이다.
