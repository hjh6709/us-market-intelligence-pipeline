SELECT
    symbol,
    window_name,
    COUNT(*) AS events,
    COUNT(return_pct) AS calculated,
    COUNT(*) FILTER (WHERE coverage_status = 'COMPLETE') AS complete,
    COUNT(*) FILTER (
        WHERE coverage_status = 'PARTIAL_MARKET_COVERAGE'
    ) AS partial,
    ROUND(AVG(return_pct), 4) AS avg_return_pct,
    ROUND(AVG(market_relative_return_pct), 4) AS avg_spy_relative_return_pct
FROM macro_event_impacts
WHERE analysis_version = 'cpi_sip_v1'
GROUP BY symbol, window_name
ORDER BY symbol, window_name;

SELECT
    COUNT(*) AS impacts,
    COUNT(DISTINCT economic_event_id) AS events,
    COUNT(*) FILTER (WHERE benchmark_return_pct IS NULL) AS missing_benchmark,
    COUNT(*) FILTER (
        WHERE symbol = 'SPY' AND market_relative_return_pct <> 0
    ) AS invalid_spy_relative
FROM macro_event_impacts
WHERE analysis_version = 'cpi_sip_v1';

SELECT COUNT(*) AS duplicate_keys
FROM (
    SELECT
        economic_event_id,
        symbol,
        source,
        feed,
        window_name,
        analysis_version
    FROM macro_event_impacts
    GROUP BY
        economic_event_id,
        symbol,
        source,
        feed,
        window_name,
        analysis_version
    HAVING COUNT(*) > 1
) duplicates;
