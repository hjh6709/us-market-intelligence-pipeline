ALTER TABLE market_bars
    ADD COLUMN IF NOT EXISTS source_bar_count SMALLINT,
    ADD COLUMN IF NOT EXISTS expected_bar_count SMALLINT,
    ADD COLUMN IF NOT EXISTS coverage_status TEXT;

ALTER TABLE market_bars
    DROP CONSTRAINT IF EXISTS market_bars_source_count_valid;

ALTER TABLE market_bars
    ADD CONSTRAINT market_bars_source_count_valid CHECK (
        (
            source_bar_count IS NULL
            AND expected_bar_count IS NULL
            AND coverage_status IS NULL
        )
        OR (
            source_bar_count > 0
            AND expected_bar_count > 0
            AND source_bar_count <= expected_bar_count
            AND coverage_status IN ('COMPLETE', 'PARTIAL')
        )
    );
