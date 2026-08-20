CREATE TABLE IF NOT EXISTS macro_series (
    series_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    frequency TEXT NOT NULL,
    units TEXT NOT NULL,
    seasonal_adjustment TEXT NOT NULL,
    observation_start DATE NOT NULL,
    observation_end DATE NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    notes TEXT,
    source TEXT NOT NULL CHECK (source = 'fred'),
    ingested_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT macro_series_observation_range_valid
        CHECK (observation_start <= observation_end)
);

CREATE TABLE IF NOT EXISTS macro_observations (
    series_id TEXT NOT NULL REFERENCES macro_series(series_id),
    observation_date DATE NOT NULL,
    value NUMERIC,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    source TEXT NOT NULL CHECK (source = 'fred'),
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (series_id, observation_date, realtime_start),
    CONSTRAINT macro_observations_realtime_range_valid
        CHECK (realtime_start <= realtime_end)
);

CREATE INDEX IF NOT EXISTS macro_observations_as_of_idx
    ON macro_observations (
        series_id,
        observation_date DESC,
        realtime_start DESC
    );

CREATE INDEX IF NOT EXISTS macro_observations_vintage_idx
    ON macro_observations (series_id, realtime_start, observation_date);
