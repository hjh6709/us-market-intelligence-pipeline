\set evidence_symbol 'SMH'
\set evidence_start '2026-08-19 19:50:00+00'
\set evidence_end '2026-08-19 19:56:00+00'

SELECT
    count(*) AS final_bar_rows,
    count(DISTINCT (symbol, bar_start, timeframe, source, feed)) AS business_keys,
    min(bar_start) AS first_bar_start,
    max(bar_start) AS last_bar_start,
    sum(volume) AS aggregated_volume,
    sum(trade_count) AS aggregated_trade_count
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source = 'alpaca'
  AND feed = 'iex'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz;

SELECT
    symbol,
    bar_start,
    timeframe,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap,
    source,
    feed,
    is_final
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source = 'alpaca'
  AND feed = 'iex'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz
ORDER BY bar_start;

SELECT
    symbol,
    bar_start,
    timeframe,
    source,
    feed,
    count(*) AS duplicate_rows
FROM market_bars
WHERE symbol = :'evidence_symbol'
  AND source = 'alpaca'
  AND feed = 'iex'
  AND bar_start >= :'evidence_start'::timestamptz
  AND bar_start < :'evidence_end'::timestamptz
GROUP BY symbol, bar_start, timeframe, source, feed
HAVING count(*) > 1;
