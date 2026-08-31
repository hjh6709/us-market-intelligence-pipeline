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

CREATE TABLE IF NOT EXISTS pipeline_experiment_runs (
    experiment_run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    experiment_mode TEXT NOT NULL,
    fault_mode TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL,
    raw_input_trades BIGINT NOT NULL DEFAULT 0,
    kafka_published BIGINT NOT NULL DEFAULT 0,
    kafka_consumed BIGINT NOT NULL DEFAULT 0,
    spark_input BIGINT NOT NULL DEFAULT 0,
    spark_invalid BIGINT NOT NULL DEFAULT 0,
    spark_duplicates BIGINT NOT NULL DEFAULT 0,
    spark_output_bars BIGINT NOT NULL DEFAULT 0,
    postgres_stored_bars BIGINT NOT NULL DEFAULT 0,
    postgres_business_key_duplicates BIGINT NOT NULL DEFAULT 0,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    events_per_second DOUBLE PRECISION NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    recovery_of_run_id TEXT,
    error_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_experiment_failures (
    experiment_run_id TEXT NOT NULL
        REFERENCES pipeline_experiment_runs(experiment_run_id),
    failure_type TEXT NOT NULL,
    failed_step TEXT NOT NULL,
    error_type TEXT NOT NULL,
    recovery_run_id TEXT,
    recovered BOOLEAN NOT NULL DEFAULT FALSE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (experiment_run_id, failure_type)
);
