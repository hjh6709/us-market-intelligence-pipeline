CREATE TABLE IF NOT EXISTS macro_event_contexts (
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    series_id TEXT NOT NULL REFERENCES macro_series(series_id),
    observation_date DATE NOT NULL,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    value NUMERIC,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (economic_event_id, series_id),
    CONSTRAINT macro_event_contexts_realtime_period_valid
        CHECK (realtime_end >= realtime_start)
);
