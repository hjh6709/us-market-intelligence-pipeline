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

-- 3. business key 중복 검사: 정상 결과는 0행
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

-- 4. 실제 저장 행 예시: 발표 화면에서만 확인하고 결과값은 Git에 저장하지 않음
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
