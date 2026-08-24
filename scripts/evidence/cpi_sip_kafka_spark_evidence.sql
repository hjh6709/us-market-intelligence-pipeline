\set evidence_symbol 'NVDA'
\set evidence_start '2026-08-12 11:30:00+00'
\set evidence_end '2026-08-12 13:31:00+00'

-- 1. Spark가 SIP raw trade로 재구성해 저장한 결과 요약
SELECT
    count(*) AS reconstructed_bar_rows,
    count(DISTINCT (symbol, bar_start, timeframe, source, feed)) AS business_keys,
    sum(trade_count) AS reconstructed_trade_count,
    min(bar_start) AS first_bar_start,
    max(bar_start) AS last_bar_start
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source = 'alpaca_replay'
  AND feed = 'sip'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz;

-- 2. Alpaca provider bar와 Spark 재구성 bar를 분리해 비교
SELECT
    source,
    feed,
    count(*) AS bar_rows,
    sum(trade_count) AS trade_count_sum,
    min(bar_start) AS first_bar_start,
    max(bar_start) AS last_bar_start
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source IN ('alpaca', 'alpaca_replay')
  AND feed = 'sip'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz
GROUP BY source, feed
ORDER BY source;

-- 3. provider bar와 Spark 재구성 bar의 행별 parity: mismatch는 모두 0
WITH provider AS (
    SELECT symbol, bar_start, open, high, low, close, volume, trade_count, vwap
    FROM market_bars
    WHERE symbol = :'evidence_symbol'
      AND source = 'alpaca'
      AND feed = 'sip'
      AND condition_policy = 'provider_aggregated_v1'
      AND bar_start >= :'evidence_start'::timestamptz
      AND bar_start < :'evidence_end'::timestamptz
), replay AS (
    SELECT symbol, bar_start, open, high, low, close, volume, trade_count, vwap
    FROM market_bars
    WHERE symbol = :'evidence_symbol'
      AND source = 'alpaca_replay'
      AND feed = 'sip'
      AND condition_policy = 'alpaca_sip_minute_v1'
      AND bar_start >= :'evidence_start'::timestamptz
      AND bar_start < :'evidence_end'::timestamptz
)
SELECT
    count(*) FILTER (WHERE provider.symbol IS NOT NULL) AS provider_bars,
    count(*) FILTER (WHERE replay.symbol IS NOT NULL) AS replay_bars,
    count(*) FILTER (
        WHERE provider.symbol IS NOT NULL AND replay.symbol IS NOT NULL
    ) AS joined_bars,
    count(*) FILTER (
        WHERE provider.open IS DISTINCT FROM replay.open
           OR provider.high IS DISTINCT FROM replay.high
           OR provider.low IS DISTINCT FROM replay.low
           OR provider.close IS DISTINCT FROM replay.close
    ) AS ohlc_mismatch_bars,
    count(*) FILTER (
        WHERE provider.volume IS DISTINCT FROM replay.volume
    ) AS volume_mismatch_bars,
    count(*) FILTER (
        WHERE provider.trade_count IS DISTINCT FROM replay.trade_count
    ) AS trade_count_mismatch_bars,
    count(*) FILTER (
        WHERE provider.vwap IS DISTINCT FROM replay.vwap
    ) AS vwap_mismatch_bars
FROM provider
FULL OUTER JOIN replay USING (symbol, bar_start);

-- 4. business key 중복 검사: 정상 결과는 0행
SELECT
    symbol,
    bar_start,
    timeframe,
    source,
    feed,
    count(*) AS duplicate_rows
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source = 'alpaca_replay'
  AND feed = 'sip'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz
GROUP BY symbol, bar_start, timeframe, source, feed
HAVING count(*) > 1;

-- 5. 실제 저장 행 예시: 발표 화면에서만 확인하고 결과값은 Git에 저장하지 않음
(SELECT symbol, bar_start, open, high, low, close, volume, trade_count, vwap
 FROM market_bars
 WHERE symbol = :'evidence_symbol'
   AND source = 'alpaca_replay'
   AND feed = 'sip'
   AND bar_start >= :'evidence_start'::timestamptz
   AND bar_start < :'evidence_end'::timestamptz
 ORDER BY bar_start
 LIMIT 3)
UNION ALL
(SELECT symbol, bar_start, open, high, low, close, volume, trade_count, vwap
 FROM market_bars
 WHERE symbol = :'evidence_symbol'
   AND source = 'alpaca_replay'
   AND feed = 'sip'
   AND bar_start >= :'evidence_start'::timestamptz
   AND bar_start < :'evidence_end'::timestamptz
 ORDER BY bar_start DESC
 LIMIT 3)
ORDER BY bar_start;
