SELECT
    count(*) AS total_rows,
    count(DISTINCT (symbol, bar_start, timeframe, source, feed)) AS business_keys
FROM market_bars;

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
    spark_batch_id
FROM market_bars
ORDER BY bar_start, symbol;

SELECT
    symbol,
    bar_start,
    timeframe,
    source,
    feed,
    count(*) AS duplicate_rows
FROM market_bars
GROUP BY symbol, bar_start, timeframe, source, feed
HAVING count(*) > 1;
