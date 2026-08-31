-- Run before and after replaying the same archive selection.
SELECT
    COUNT(*) AS market_bar_rows,
    COUNT(*) - COUNT(DISTINCT (symbol, bar_start, timeframe, source, feed))
        AS duplicate_business_keys,
    MD5(
        STRING_AGG(
            CONCAT_WS(
                '|', symbol, bar_start::TEXT, timeframe, source, feed,
                open::TEXT, high::TEXT, low::TEXT, close::TEXT,
                volume::TEXT, trade_count::TEXT, vwap::TEXT
            ),
            E'\n' ORDER BY symbol, bar_start, timeframe, source, feed
        )
    ) AS result_hash
FROM market_bars
WHERE source = 'alpaca_replay';

-- No future-dated macro vintage may be joined to an event.
SELECT COUNT(*) AS point_in_time_violations
FROM macro_event_contexts AS context
JOIN economic_events AS event USING (economic_event_id)
WHERE context.observation_date
          > (event.released_at AT TIME ZONE 'America/New_York')::DATE
   OR context.realtime_start
          > (event.released_at AT TIME ZONE 'America/New_York')::DATE;

-- Daily rate and VIX observations must predate the 08:30 ET CPI release day.
SELECT COUNT(*) AS same_day_daily_series_observations
FROM macro_event_contexts AS context
JOIN economic_events AS event USING (economic_event_id)
WHERE context.series_id IN ('DFF', 'DGS2', 'DGS10', 'VIXCLS')
  AND context.observation_date
          >= (event.released_at AT TIME ZONE 'America/New_York')::DATE;
