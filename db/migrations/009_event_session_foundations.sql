CREATE TABLE IF NOT EXISTS economic_release_observations (
    release_observation_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    metric_name TEXT NOT NULL,
    revision_number INTEGER NOT NULL CHECK (revision_number >= 0),
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    released_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_release_observations_observation_time_valid
        CHECK (observed_at >= released_at),
    CONSTRAINT economic_release_observations_payload_hash_valid
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT economic_release_observations_identity
        UNIQUE (economic_event_id, metric_name, revision_number, observed_at),
    CONSTRAINT economic_release_observations_reference_identity
        UNIQUE (release_observation_id, economic_event_id, metric_name)
);

CREATE INDEX IF NOT EXISTS economic_release_observations_pit_idx
    ON economic_release_observations
    (economic_event_id, metric_name, observed_at DESC, revision_number DESC);

CREATE TABLE IF NOT EXISTS economic_consensus_snapshots (
    consensus_snapshot_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    metric_name TEXT NOT NULL,
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    provider TEXT NOT NULL,
    contributor_count INTEGER CHECK (contributor_count IS NULL OR contributor_count >= 0),
    observed_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_consensus_snapshots_payload_hash_valid
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT economic_consensus_snapshots_identity
        UNIQUE (economic_event_id, metric_name, provider, observed_at),
    CONSTRAINT economic_consensus_snapshots_reference_identity
        UNIQUE (consensus_snapshot_id, economic_event_id, metric_name)
);

CREATE INDEX IF NOT EXISTS economic_consensus_snapshots_pit_idx
    ON economic_consensus_snapshots
    (economic_event_id, metric_name, observed_at DESC);

CREATE TABLE IF NOT EXISTS economic_surprises (
    economic_surprise_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    metric_name TEXT NOT NULL,
    actual_observation_id BIGINT NOT NULL,
    consensus_snapshot_id BIGINT NOT NULL,
    surprise_value NUMERIC NOT NULL,
    standardized_surprise NUMERIC,
    algorithm_version TEXT NOT NULL,
    pipeline_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_surprises_actual_identity_fk
        FOREIGN KEY (actual_observation_id, economic_event_id, metric_name)
        REFERENCES economic_release_observations
            (release_observation_id, economic_event_id, metric_name),
    CONSTRAINT economic_surprises_consensus_identity_fk
        FOREIGN KEY (consensus_snapshot_id, economic_event_id, metric_name)
        REFERENCES economic_consensus_snapshots
            (consensus_snapshot_id, economic_event_id, metric_name),
    CONSTRAINT economic_surprises_input_version_identity
        UNIQUE (actual_observation_id, consensus_snapshot_id, algorithm_version)
);

CREATE OR REPLACE FUNCTION reject_immutable_fact_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; insert a new fact instead', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS economic_release_observations_immutable
    ON economic_release_observations;
CREATE TRIGGER economic_release_observations_immutable
    BEFORE UPDATE OR DELETE ON economic_release_observations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

DROP TRIGGER IF EXISTS economic_consensus_snapshots_immutable
    ON economic_consensus_snapshots;
CREATE TRIGGER economic_consensus_snapshots_immutable
    BEFORE UPDATE OR DELETE ON economic_consensus_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

DROP TRIGGER IF EXISTS economic_surprises_immutable
    ON economic_surprises;
CREATE TRIGGER economic_surprises_immutable
    BEFORE UPDATE OR DELETE ON economic_surprises
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS trading_sessions (
    exchange TEXT NOT NULL,
    session_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    is_early_close BOOLEAN NOT NULL DEFAULT FALSE,
    exchange_timezone TEXT NOT NULL,
    calendar_source TEXT NOT NULL,
    calendar_version TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, session_date, calendar_version),
    CONSTRAINT trading_sessions_interval_valid CHECK (closes_at > opens_at)
);

CREATE INDEX IF NOT EXISTS trading_sessions_lookup_idx
    ON trading_sessions (exchange, session_date DESC);

CREATE TABLE IF NOT EXISTS economic_event_markers (
    economic_event_marker_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    marker_kind TEXT NOT NULL,
    marker_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_event_markers_kind_valid
        CHECK (marker_kind IN ('RELEASE', 'STATEMENT', 'PRESS_CONFERENCE')),
    CONSTRAINT economic_event_markers_identity
        UNIQUE (economic_event_id, marker_kind, marker_at)
);

CREATE INDEX IF NOT EXISTS economic_event_markers_event_idx
    ON economic_event_markers (economic_event_id, marker_at);
