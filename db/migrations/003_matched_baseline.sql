CREATE TABLE IF NOT EXISTS macro_event_baseline_impacts (
    baseline_impact_id TEXT PRIMARY KEY,
    economic_event_id TEXT NOT NULL
        REFERENCES economic_events(economic_event_id),
    control_offset_weeks SMALLINT NOT NULL
        CHECK (control_offset_weeks BETWEEN 1 AND 3),
    matched_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL,
    feed TEXT NOT NULL,
    session_scope TEXT NOT NULL,
    window_name TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18, 6),
    close_price NUMERIC(18, 6),
    return_pct NUMERIC(18, 8),
    volume BIGINT CHECK (volume IS NULL OR volume >= 0),
    realized_volatility NUMERIC(18, 8),
    coverage_status TEXT NOT NULL,
    coverage_reason TEXT,
    baseline_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT macro_event_baseline_window_valid
        CHECK (window_end > window_start),
    CONSTRAINT macro_event_baseline_unique_analysis
        UNIQUE (
            economic_event_id,
            control_offset_weeks,
            symbol,
            source,
            feed,
            window_name,
            baseline_version
        )
);

CREATE INDEX IF NOT EXISTS macro_event_baseline_event_idx
    ON macro_event_baseline_impacts (
        economic_event_id,
        symbol,
        window_name,
        control_offset_weeks
    );

ALTER TABLE macro_event_impacts
    ADD COLUMN IF NOT EXISTS matched_baseline_return_pct NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS return_vs_matched_baseline_pct NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS matched_baseline_volume NUMERIC(24, 4),
    ADD COLUMN IF NOT EXISTS volume_ratio_vs_matched_baseline NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS matched_baseline_volatility NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS volatility_ratio_vs_matched_baseline NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS baseline_sample_size SMALLINT,
    ADD COLUMN IF NOT EXISTS baseline_version TEXT;
