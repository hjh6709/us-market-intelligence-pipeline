SELECT
    e.reference_period,
    s.symbol,
    COUNT(b.*) AS bars,
    MIN(b.bar_start) AS first_bar,
    MAX(b.bar_start) AS last_bar
FROM economic_events e
CROSS JOIN (VALUES ('SPY'), ('QQQ'), ('SMH'), ('NVDA')) AS s(symbol)
LEFT JOIN market_bars b
    ON b.symbol = s.symbol
   AND b.source = 'alpaca'
   AND b.feed = 'sip'
   AND b.timeframe = '1m'
   AND b.bar_start BETWEEN e.released_at - INTERVAL '60 minutes'
                           AND e.released_at + INTERVAL '60 minutes'
WHERE e.event_type = 'CPI'
GROUP BY e.reference_period, s.symbol
ORDER BY e.reference_period, s.symbol;

SELECT
    symbol,
    COUNT(*) AS bars,
    MIN(bar_start) AS first_bar,
    MAX(bar_start) AS last_bar
FROM market_bars
WHERE source = 'alpaca'
  AND feed = 'sip'
  AND condition_policy = 'provider_aggregated_v1'
GROUP BY symbol
ORDER BY symbol;

SELECT COUNT(*) AS duplicate_keys
FROM (
    SELECT symbol, bar_start, timeframe, source, feed
    FROM market_bars
    WHERE source = 'alpaca' AND feed = 'sip'
    GROUP BY symbol, bar_start, timeframe, source, feed
    HAVING COUNT(*) > 1
) duplicates;
