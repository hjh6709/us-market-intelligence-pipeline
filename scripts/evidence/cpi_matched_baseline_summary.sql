SELECT
    coverage_status,
    COUNT(*) AS metrics
FROM macro_event_baseline_impacts
WHERE baseline_version = 'same_weekday_1_2_3w_v1'
GROUP BY coverage_status
ORDER BY coverage_status;

SELECT
    baseline_sample_size,
    COUNT(*) AS event_impacts
FROM macro_event_impacts
WHERE analysis_version = 'cpi_sip_v1'
GROUP BY baseline_sample_size
ORDER BY baseline_sample_size;

SELECT
    symbol,
    window_name,
    COUNT(*) AS usable_events,
    ROUND(AVG(return_vs_matched_baseline_pct), 4)
        AS avg_return_vs_baseline_pct,
    ROUND(AVG(volume_ratio_vs_matched_baseline), 3)
        AS avg_volume_ratio
FROM macro_event_impacts
WHERE analysis_version = 'cpi_sip_v1'
  AND coverage_status = 'COMPLETE'
  AND baseline_sample_size >= 2
GROUP BY symbol, window_name
ORDER BY symbol, window_name;

SELECT COUNT(*) AS duplicate_baselines
FROM (
    SELECT
        economic_event_id,
        control_offset_weeks,
        symbol,
        source,
        feed,
        window_name,
        baseline_version
    FROM macro_event_baseline_impacts
    GROUP BY
        economic_event_id,
        control_offset_weeks,
        symbol,
        source,
        feed,
        window_name,
        baseline_version
    HAVING COUNT(*) > 1
) duplicates;
