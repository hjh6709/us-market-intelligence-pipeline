CREATE TABLE IF NOT EXISTS macro_series (
    series_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    frequency TEXT NOT NULL,
    units TEXT NOT NULL,
    seasonal_adjustment TEXT,
    source TEXT NOT NULL DEFAULT 'fred',
    source_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS macro_observations (
    series_id TEXT NOT NULL REFERENCES macro_series(series_id),
    observation_date DATE NOT NULL,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    value NUMERIC,
    source TEXT NOT NULL DEFAULT 'fred',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (series_id, observation_date, realtime_start),
    CONSTRAINT macro_observations_realtime_period_valid
        CHECK (realtime_end >= realtime_start)
);

CREATE INDEX IF NOT EXISTS macro_observations_point_in_time_idx
    ON macro_observations (series_id, realtime_start, observation_date DESC);

CREATE TABLE IF NOT EXISTS economic_events (
    economic_event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    reference_period TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ NOT NULL,
    original_timezone TEXT NOT NULL,
    previous NUMERIC,
    forecast NUMERIC,
    actual NUMERIC,
    surprise NUMERIC,
    unit TEXT,
    release_source TEXT NOT NULL,
    release_source_url TEXT NOT NULL,
    value_source TEXT,
    vintage_as_of DATE,
    quality_status TEXT NOT NULL DEFAULT 'READY',
    quality_reason TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_events_surprise_inputs_present
        CHECK (surprise IS NULL OR (forecast IS NOT NULL AND actual IS NOT NULL)),
    CONSTRAINT economic_events_unique_release
        UNIQUE (event_type, reference_period, released_at, release_source)
);

CREATE INDEX IF NOT EXISTS economic_events_release_time_idx
    ON economic_events (event_type, released_at DESC);

CREATE TABLE IF NOT EXISTS macro_event_impacts (
    impact_id TEXT PRIMARY KEY,
    economic_event_id TEXT NOT NULL
        REFERENCES economic_events(economic_event_id),
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
    benchmark_symbol TEXT,
    benchmark_return_pct NUMERIC(18, 8),
    market_relative_return_pct NUMERIC(18, 8),
    coverage_status TEXT NOT NULL,
    coverage_reason TEXT,
    analysis_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT macro_event_impacts_window_valid
        CHECK (window_end > window_start),
    CONSTRAINT macro_event_impacts_unique_analysis
        UNIQUE (
            economic_event_id,
            symbol,
            source,
            feed,
            window_name,
            analysis_version
        )
);

CREATE INDEX IF NOT EXISTS macro_event_impacts_event_symbol_idx
    ON macro_event_impacts (economic_event_id, symbol, window_start);
