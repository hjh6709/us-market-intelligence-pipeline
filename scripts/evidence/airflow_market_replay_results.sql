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
  AND bar_start >= TIMESTAMPTZ '2026-08-12 11:30:00+00'
  AND bar_start < TIMESTAMPTZ '2026-08-12 13:31:00+00'
GROUP BY symbol
ORDER BY symbol;

WITH expected_minutes AS (
    SELECT generate_series(
        TIMESTAMPTZ '2026-08-12 11:30:00+00',
        TIMESTAMPTZ '2026-08-12 13:30:00+00',
        INTERVAL '1 minute'
    ) AS bar_start
),
symbols(symbol) AS (
    VALUES ('SPY'), ('QQQ'), ('SMH'), ('NVDA')
)
SELECT
    symbols.symbol,
    to_char(
        expected_minutes.bar_start AT TIME ZONE 'America/New_York',
        'HH24:MI'
    ) AS missing_et,
    to_char(
        expected_minutes.bar_start AT TIME ZONE 'UTC',
        'HH24:MI'
    ) AS missing_utc
FROM symbols
CROSS JOIN expected_minutes
LEFT JOIN market_bars
  ON market_bars.symbol = symbols.symbol
 AND market_bars.bar_start = expected_minutes.bar_start
 AND market_bars.timeframe = '1m'
 AND market_bars.source = 'alpaca_replay'
 AND market_bars.feed = 'sip'
WHERE market_bars.symbol IS NULL
ORDER BY symbols.symbol, expected_minutes.bar_start;

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
