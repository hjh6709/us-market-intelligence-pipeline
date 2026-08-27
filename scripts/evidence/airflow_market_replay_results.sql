SELECT
    symbol,
    count(*) AS bars,
    min(bar_start) AS first_bar,
    max(bar_start) AS last_bar,
    sum(trade_count) AS trade_count_sum
FROM market_bars
WHERE source = 'alpaca_replay'
  AND feed = 'sip'
  AND symbol IN ('SPY', 'QQQ', 'SMH', 'NVDA')
  AND bar_start >= TIMESTAMPTZ '2026-08-12 12:25:00+00'
  AND bar_start < TIMESTAMPTZ '2026-08-12 12:35:00+00'
GROUP BY symbol
ORDER BY symbol;

SELECT
    symbol,
    bar_start,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap,
    source,
    feed
FROM market_bars
WHERE source = 'alpaca_replay'
  AND feed = 'sip'
  AND symbol IN ('SPY', 'QQQ', 'SMH', 'NVDA')
  AND bar_start >= TIMESTAMPTZ '2026-08-12 12:25:00+00'
  AND bar_start < TIMESTAMPTZ '2026-08-12 12:27:00+00'
ORDER BY symbol, bar_start;
