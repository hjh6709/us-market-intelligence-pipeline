-- Corrected, still-unpopulated foundation for canonical event/session semantics.
-- Legacy economic_events and legacy analytics remain unchanged compatibility data.

CREATE TABLE IF NOT EXISTS canonical_economic_events (
    economic_event_id TEXT PRIMARY KEY,
    legacy_economic_event_id TEXT UNIQUE REFERENCES economic_events(economic_event_id),
    event_type TEXT NOT NULL,
    reference_period TEXT NOT NULL,
    official_source TEXT NOT NULL,
    official_source_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS economic_event_lifecycle_versions (
    event_lifecycle_version_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    lifecycle_version INTEGER NOT NULL CHECK (lifecycle_version >= 1),
    scheduled_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    event_status TEXT NOT NULL CHECK (
        event_status IN ('SCHEDULED', 'RELEASED', 'RESCHEDULED', 'CANCELED', 'CORRECTED')
    ),
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_event_lifecycle_release_state_valid CHECK (
        (event_status IN ('RELEASED', 'CORRECTED') AND released_at IS NOT NULL)
        OR (event_status IN ('SCHEDULED', 'RESCHEDULED', 'CANCELED') AND released_at IS NULL)
    ),
    CONSTRAINT economic_event_lifecycle_first_seen_valid CHECK (
        published_at IS NULL OR first_observed_at >= published_at
    ),
    CONSTRAINT economic_event_lifecycle_identity UNIQUE (economic_event_id, lifecycle_version)
);

CREATE TABLE IF NOT EXISTS economic_observation_registry (
    observation_code TEXT PRIMARY KEY CHECK (observation_code ~ '^[A-Z][A-Z0-9_]*$'),
    canonical_unit TEXT NOT NULL,
    value_semantics TEXT NOT NULL,
    source_family TEXT NOT NULL,
    frequency TEXT NOT NULL,
    transform_semantics TEXT NOT NULL,
    CONSTRAINT economic_observation_registry_code_unit UNIQUE (observation_code, canonical_unit)
);

INSERT INTO economic_observation_registry (
    observation_code, canonical_unit, value_semantics,
    source_family, frequency, transform_semantics
) VALUES
    ('CPI_HEADLINE_MOM', 'PERCENT', 'period-over-period percent change', 'BLS_CPI', 'MONTHLY', 'MOM'),
    ('CPI_HEADLINE_YOY', 'PERCENT', 'year-over-year percent change', 'BLS_CPI', 'MONTHLY', 'YOY'),
    ('CPI_CORE_MOM', 'PERCENT', 'period-over-period percent change excluding food and energy', 'BLS_CPI', 'MONTHLY', 'MOM'),
    ('CPI_CORE_YOY', 'PERCENT', 'year-over-year percent change excluding food and energy', 'BLS_CPI', 'MONTHLY', 'YOY'),
    ('NFP_PAYROLL_CHANGE', 'THOUSANDS_PERSONS', 'monthly change in nonfarm payroll employment', 'BLS_EMPLOYMENT', 'MONTHLY', 'LEVEL_CHANGE'),
    ('UNEMPLOYMENT_RATE', 'PERCENT', 'civilian unemployment rate', 'BLS_EMPLOYMENT', 'MONTHLY', 'LEVEL'),
    ('AVERAGE_HOURLY_EARNINGS_MOM', 'PERCENT', 'period-over-period percent change', 'BLS_EMPLOYMENT', 'MONTHLY', 'MOM'),
    ('AVERAGE_HOURLY_EARNINGS_YOY', 'PERCENT', 'year-over-year percent change', 'BLS_EMPLOYMENT', 'MONTHLY', 'YOY'),
    ('PCE_HEADLINE_MOM', 'PERCENT', 'period-over-period percent change', 'BEA_PCE', 'MONTHLY', 'MOM'),
    ('PCE_HEADLINE_YOY', 'PERCENT', 'year-over-year percent change', 'BEA_PCE', 'MONTHLY', 'YOY'),
    ('PCE_CORE_MOM', 'PERCENT', 'period-over-period percent change excluding food and energy', 'BEA_PCE', 'MONTHLY', 'MOM'),
    ('PCE_CORE_YOY', 'PERCENT', 'year-over-year percent change excluding food and energy', 'BEA_PCE', 'MONTHLY', 'YOY'),
    ('FED_TARGET_LOWER', 'PERCENT', 'lower bound of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_TARGET_UPPER', 'PERCENT', 'upper bound of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_TARGET_MIDPOINT', 'PERCENT', 'midpoint of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_RATE_CHANGE_BP', 'BASIS_POINTS', 'change in target range midpoint', 'FED_POLICY', 'EVENT', 'LEVEL_CHANGE')
ON CONFLICT (observation_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS economic_release_observations (
    release_observation_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    observation_code TEXT NOT NULL,
    revision_number INTEGER NOT NULL CHECK (revision_number >= 0),
    source_revision_id TEXT,
    revision_type TEXT NOT NULL CHECK (revision_type IN ('INITIAL', 'REVISION', 'CORRECTION')),
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_release_observations_revision_type_valid CHECK (
        (revision_number = 0 AND revision_type = 'INITIAL')
        OR (revision_number > 0 AND revision_type IN ('REVISION', 'CORRECTION'))
    ),
    CONSTRAINT economic_release_observations_source_time_valid CHECK (
        first_observed_at >= published_at
    ),
    CONSTRAINT economic_release_observations_code_unit_fk
        FOREIGN KEY (observation_code, unit)
        REFERENCES economic_observation_registry (observation_code, canonical_unit),
    CONSTRAINT economic_release_observations_identity
        UNIQUE (economic_event_id, observation_code, revision_number),
    CONSTRAINT economic_release_observations_reference_identity
        UNIQUE (release_observation_id, economic_event_id, observation_code, unit)
);

CREATE UNIQUE INDEX IF NOT EXISTS economic_release_observations_source_revision_identity
    ON economic_release_observations
    (economic_event_id, observation_code, source, source_revision_id)
    WHERE source_revision_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS economic_release_observations_pit_idx
    ON economic_release_observations
    (economic_event_id, observation_code, published_at DESC, revision_number DESC);

CREATE OR REPLACE FUNCTION record_economic_release_observation(
    p_economic_event_id TEXT,
    p_observation_code TEXT,
    p_revision_number INTEGER,
    p_source_revision_id TEXT,
    p_revision_type TEXT,
    p_value NUMERIC,
    p_unit TEXT,
    p_published_at TIMESTAMPTZ,
    p_first_observed_at TIMESTAMPTZ,
    p_source TEXT,
    p_source_url TEXT,
    p_payload_sha256 TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    existing economic_release_observations%ROWTYPE;
    result_id BIGINT;
BEGIN
    INSERT INTO economic_release_observations (
        economic_event_id, observation_code, revision_number,
        source_revision_id, revision_type, value, unit, published_at,
        first_observed_at, source, source_url, payload_sha256
    ) VALUES (
        p_economic_event_id, p_observation_code, p_revision_number,
        p_source_revision_id, p_revision_type, p_value, p_unit, p_published_at,
        p_first_observed_at, p_source, p_source_url, p_payload_sha256
    )
    ON CONFLICT (economic_event_id, observation_code, revision_number) DO NOTHING
    RETURNING release_observation_id INTO result_id;

    IF result_id IS NOT NULL THEN
        RETURN result_id;
    END IF;

    SELECT * INTO STRICT existing
    FROM economic_release_observations
    WHERE economic_event_id = p_economic_event_id
      AND observation_code = p_observation_code
      AND revision_number = p_revision_number;

    IF existing.value IS NOT DISTINCT FROM p_value
       AND existing.unit = p_unit
       AND existing.payload_sha256 = p_payload_sha256
       AND existing.source = p_source
       AND existing.source_revision_id IS NOT DISTINCT FROM p_source_revision_id
    THEN
        RETURN existing.release_observation_id;
    END IF;

    RAISE EXCEPTION 'CONFLICTING_SOURCE_FACT: %/% revision %',
        p_economic_event_id, p_observation_code, p_revision_number
        USING ERRCODE = '23505';
END;
$$;

CREATE TABLE IF NOT EXISTS economic_consensus_snapshots (
    consensus_snapshot_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    observation_code TEXT NOT NULL,
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_snapshot_id TEXT,
    contributor_count INTEGER CHECK (contributor_count IS NULL OR contributor_count >= 0),
    snapshot_at TIMESTAMPTZ NOT NULL,
    provider_updated_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_consensus_snapshots_code_unit_fk
        FOREIGN KEY (observation_code, unit)
        REFERENCES economic_observation_registry (observation_code, canonical_unit),
    CONSTRAINT economic_consensus_snapshots_identity
        UNIQUE (economic_event_id, observation_code, provider, snapshot_at),
    CONSTRAINT economic_consensus_snapshots_reference_identity
        UNIQUE (consensus_snapshot_id, economic_event_id, observation_code, unit)
);

CREATE UNIQUE INDEX IF NOT EXISTS economic_consensus_provider_snapshot_identity
    ON economic_consensus_snapshots
    (economic_event_id, observation_code, provider, provider_snapshot_id)
    WHERE provider_snapshot_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS economic_consensus_snapshots_pit_idx
    ON economic_consensus_snapshots
    (economic_event_id, observation_code, snapshot_at DESC);

CREATE TABLE IF NOT EXISTS economic_event_markers (
    economic_event_marker_id TEXT PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    marker_kind TEXT NOT NULL CHECK (
        marker_kind IN ('RELEASE', 'STATEMENT', 'PRESS_CONFERENCE')
    ),
    marker_role TEXT NOT NULL CHECK (marker_role IN ('PRIMARY', 'SECONDARY')),
    marker_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_event_markers_source_time_valid CHECK (
        published_at IS NULL OR first_observed_at >= published_at
    ),
    CONSTRAINT economic_event_markers_kind_identity UNIQUE (economic_event_id, marker_kind)
);

CREATE UNIQUE INDEX IF NOT EXISTS economic_event_markers_one_primary
    ON economic_event_markers (economic_event_id)
    WHERE marker_role = 'PRIMARY';

CREATE INDEX IF NOT EXISTS economic_event_markers_event_idx
    ON economic_event_markers (economic_event_id, marker_at);

CREATE OR REPLACE FUNCTION select_canonical_prerelease_consensus(
    p_economic_event_id TEXT,
    p_observation_code TEXT
)
RETURNS BIGINT
LANGUAGE sql
STABLE
AS $$
    SELECT consensus.consensus_snapshot_id
    FROM economic_consensus_snapshots AS consensus
    JOIN economic_event_markers AS marker
      ON marker.economic_event_id = consensus.economic_event_id
     AND marker.marker_role = 'PRIMARY'
    WHERE consensus.economic_event_id = p_economic_event_id
      AND consensus.observation_code = p_observation_code
      AND consensus.snapshot_at < marker.marker_at
    ORDER BY consensus.snapshot_at DESC, consensus.consensus_snapshot_id DESC
    LIMIT 1
$$;

CREATE TABLE IF NOT EXISTS economic_surprises (
    economic_surprise_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    observation_code TEXT NOT NULL,
    unit TEXT NOT NULL,
    actual_observation_id BIGINT NOT NULL,
    consensus_snapshot_id BIGINT NOT NULL,
    surprise_value NUMERIC NOT NULL,
    standardized_surprise NUMERIC,
    algorithm_version TEXT NOT NULL,
    pipeline_run_id TEXT REFERENCES pipeline_runs(pipeline_run_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_surprises_actual_identity_fk
        FOREIGN KEY (actual_observation_id, economic_event_id, observation_code, unit)
        REFERENCES economic_release_observations
            (release_observation_id, economic_event_id, observation_code, unit),
    CONSTRAINT economic_surprises_consensus_identity_fk
        FOREIGN KEY (consensus_snapshot_id, economic_event_id, observation_code, unit)
        REFERENCES economic_consensus_snapshots
            (consensus_snapshot_id, economic_event_id, observation_code, unit),
    CONSTRAINT economic_surprises_input_version_identity
        UNIQUE (actual_observation_id, consensus_snapshot_id, algorithm_version)
);

CREATE OR REPLACE FUNCTION enforce_canonical_surprise()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    actual economic_release_observations%ROWTYPE;
    consensus economic_consensus_snapshots%ROWTYPE;
    canonical_consensus_id BIGINT;
BEGIN
    SELECT * INTO STRICT actual
    FROM economic_release_observations
    WHERE release_observation_id = NEW.actual_observation_id;
    SELECT * INTO STRICT consensus
    FROM economic_consensus_snapshots
    WHERE consensus_snapshot_id = NEW.consensus_snapshot_id;

    IF actual.revision_number <> 0 OR actual.revision_type <> 'INITIAL' THEN
        RAISE EXCEPTION 'canonical surprise requires initial official observation'
            USING ERRCODE = '23514';
    END IF;

    canonical_consensus_id := select_canonical_prerelease_consensus(
        NEW.economic_event_id, NEW.observation_code
    );
    IF canonical_consensus_id IS NULL OR canonical_consensus_id <> NEW.consensus_snapshot_id THEN
        RAISE EXCEPTION 'canonical surprise requires latest pre-release consensus'
            USING ERRCODE = '23514';
    END IF;

    IF actual.unit <> consensus.unit OR actual.unit <> NEW.unit THEN
        RAISE EXCEPTION 'canonical surprise requires compatible units'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.surprise_value <> actual.value - consensus.value THEN
        RAISE EXCEPTION 'stored surprise must equal actual minus consensus'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS economic_surprises_canonical_inputs ON economic_surprises;
CREATE TRIGGER economic_surprises_canonical_inputs
    BEFORE INSERT ON economic_surprises
    FOR EACH ROW EXECUTE FUNCTION enforce_canonical_surprise();

CREATE OR REPLACE FUNCTION reject_immutable_fact_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; insert a new fact instead', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS economic_event_lifecycle_versions_immutable
    ON economic_event_lifecycle_versions;
CREATE TRIGGER economic_event_lifecycle_versions_immutable
    BEFORE UPDATE OR DELETE ON economic_event_lifecycle_versions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

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

DROP TRIGGER IF EXISTS economic_event_markers_immutable
    ON economic_event_markers;
CREATE TRIGGER economic_event_markers_immutable
    BEFORE UPDATE OR DELETE ON economic_event_markers
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

DROP TRIGGER IF EXISTS economic_surprises_immutable
    ON economic_surprises;
CREATE TRIGGER economic_surprises_immutable
    BEFORE UPDATE OR DELETE ON economic_surprises
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS trading_sessions (
    market_code TEXT NOT NULL,
    session_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    session_day_type TEXT NOT NULL CHECK (session_day_type IN ('REGULAR', 'EARLY_CLOSE')),
    exchange_timezone TEXT NOT NULL,
    calendar_source TEXT NOT NULL,
    calendar_snapshot_id TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market_code, session_date, calendar_snapshot_id),
    CONSTRAINT trading_sessions_interval_valid CHECK (closes_at > opens_at),
    CONSTRAINT trading_sessions_trusted_timezone CHECK (
        market_code <> 'US_EQUITIES' OR exchange_timezone = 'America/New_York'
    ),
    CONSTRAINT trading_sessions_open_local_date_valid CHECK (
        (opens_at AT TIME ZONE exchange_timezone)::date = session_date
    ),
    CONSTRAINT trading_sessions_close_local_date_valid CHECK (
        (closes_at AT TIME ZONE exchange_timezone)::date = session_date
    )
);

CREATE INDEX IF NOT EXISTS trading_sessions_lookup_idx
    ON trading_sessions (market_code, session_date DESC, calendar_snapshot_id);

CREATE TABLE IF NOT EXISTS validation_reconstructed_bars (
    validation_run_id TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    checkpoint_namespace TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bar_start TIMESTAMPTZ NOT NULL,
    timeframe TEXT NOT NULL,
    open NUMERIC(18, 6) NOT NULL CHECK (open > 0),
    high NUMERIC(18, 6) NOT NULL CHECK (high > 0),
    low NUMERIC(18, 6) NOT NULL CHECK (low > 0),
    close NUMERIC(18, 6) NOT NULL CHECK (close > 0),
    volume BIGINT NOT NULL CHECK (volume >= 0),
    trade_count BIGINT NOT NULL CHECK (trade_count >= 0),
    vwap NUMERIC(18, 6),
    source TEXT NOT NULL,
    feed TEXT NOT NULL,
    is_final BOOLEAN NOT NULL CHECK (is_final),
    condition_policy TEXT NOT NULL,
    spark_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT validation_reconstructed_bars_high_valid
        CHECK (high >= GREATEST(open, low, close)),
    CONSTRAINT validation_reconstructed_bars_low_valid
        CHECK (low <= LEAST(open, high, close)),
    PRIMARY KEY (
        validation_run_id, processor_version, symbol, bar_start,
        timeframe, source, feed
    )
);

CREATE INDEX IF NOT EXISTS validation_reconstructed_bars_lookup_idx
    ON validation_reconstructed_bars (symbol, timeframe, bar_start DESC);

CREATE OR REPLACE VIEW market_bar_origin_comparison AS
SELECT
    'PROVIDER_AGGREGATE'::TEXT AS bar_origin,
    NULL::TEXT AS validation_run_id,
    NULL::TEXT AS processor_version,
    symbol, bar_start, timeframe, open, high, low, close,
    volume, trade_count, vwap, source, feed, condition_policy
FROM market_bars
UNION ALL
SELECT
    'RAW_RECONSTRUCTED'::TEXT AS bar_origin,
    validation_run_id,
    processor_version,
    symbol, bar_start, timeframe, open, high, low, close,
    volume, trade_count, vwap, source, feed, condition_policy
FROM validation_reconstructed_bars;
