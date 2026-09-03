\pset pager off

SELECT pipeline_run_id, dag_id, status, started_at, finished_at
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;

SELECT pipeline_run_id, stage, status, COUNT(*) AS work_items,
       SUM(COALESCE(input_count, 0)) AS input_rows,
       SUM(COALESCE(output_count, 0)) AS output_rows
FROM pipeline_work_items
GROUP BY pipeline_run_id, stage, status
ORDER BY pipeline_run_id DESC, stage, status;

SELECT pipeline_run_id, status, alert_status, COUNT(*) AS checks
FROM pipeline_run_checks
GROUP BY pipeline_run_id, status, alert_status
ORDER BY pipeline_run_id DESC, status, alert_status;

SELECT timeframe, coverage_status, COUNT(*) AS rows
FROM market_bars
WHERE timeframe IN ('1m', '3m', '5m', '1d')
GROUP BY timeframe, coverage_status
ORDER BY timeframe, coverage_status;

SELECT COUNT(*) AS business_key_duplicates
FROM (
    SELECT symbol, bar_start, timeframe, source, feed
    FROM market_bars
    GROUP BY symbol, bar_start, timeframe, source, feed
    HAVING COUNT(*) > 1
) duplicate_keys;
