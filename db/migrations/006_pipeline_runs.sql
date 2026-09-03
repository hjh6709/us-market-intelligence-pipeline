CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    config_json JSONB NOT NULL,
    config_hash TEXT NOT NULL,
    data_cutoff TIMESTAMPTZ NOT NULL,
    code_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_config_hash
    ON pipeline_runs (config_hash, started_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_work_items (
    pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs (pipeline_run_id),
    economic_event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DATA_NOT_AVAILABLE')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    manifest_path TEXT,
    input_count BIGINT,
    output_count BIGINT,
    error_code TEXT,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_run_id, economic_event_id, symbol, stage)
);

CREATE TABLE IF NOT EXISTS pipeline_run_checks (
    pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs (pipeline_run_id),
    economic_event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stage TEXT NOT NULL,
    check_name TEXT NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    status TEXT NOT NULL CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    alert_status TEXT NOT NULL CHECK (alert_status IN ('NONE', 'OPEN', 'RESOLVED')),
    checked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        pipeline_run_id,
        economic_event_id,
        symbol,
        stage,
        check_name
    )
);
