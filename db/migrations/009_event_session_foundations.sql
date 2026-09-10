-- Canonical foundation only. Legacy economic_events, serving, and analytics stay unchanged.

CREATE OR REPLACE FUNCTION reject_immutable_fact_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; insert a new fact instead', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

CREATE TABLE IF NOT EXISTS canonical_economic_events (
    economic_event_id TEXT PRIMARY KEY,
    legacy_economic_event_id TEXT UNIQUE REFERENCES economic_events(economic_event_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('CPI', 'EMPLOYMENT', 'PCE', 'FOMC')),
    reference_period TEXT NOT NULL,
    official_source TEXT NOT NULL,
    official_source_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT canonical_economic_events_type_identity
        UNIQUE (economic_event_id, event_type)
);

CREATE OR REPLACE FUNCTION enforce_canonical_event_identity_immutability()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.economic_event_id IS DISTINCT FROM OLD.economic_event_id
       OR NEW.event_type IS DISTINCT FROM OLD.event_type
       OR NEW.reference_period IS DISTINCT FROM OLD.reference_period
       OR NEW.official_source IS DISTINCT FROM OLD.official_source
    THEN
        RAISE EXCEPTION 'canonical event identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS canonical_economic_events_identity_immutable
    ON canonical_economic_events;
CREATE TRIGGER canonical_economic_events_identity_immutable
    BEFORE UPDATE ON canonical_economic_events
    FOR EACH ROW EXECUTE FUNCTION enforce_canonical_event_identity_immutability();

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
    CONSTRAINT economic_event_lifecycle_identity
        UNIQUE (economic_event_id, lifecycle_version)
);

CREATE OR REPLACE FUNCTION enforce_economic_event_lifecycle_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    previous_version INTEGER;
    previous_status TEXT;
BEGIN
    SELECT lifecycle_version, event_status
      INTO previous_version, previous_status
      FROM economic_event_lifecycle_versions
     WHERE economic_event_id = NEW.economic_event_id
     ORDER BY lifecycle_version DESC
     LIMIT 1;

    IF previous_version IS NULL THEN
        IF NEW.lifecycle_version <> 1 OR NEW.event_status <> 'SCHEDULED' THEN
            RAISE EXCEPTION 'invalid initial lifecycle state'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.lifecycle_version <> previous_version + 1 THEN
        RAISE EXCEPTION 'lifecycle version must be consecutive'
            USING ERRCODE = '23514';
    END IF;

    IF NOT (
        (previous_status = 'SCHEDULED' AND NEW.event_status IN ('RESCHEDULED', 'RELEASED', 'CANCELED'))
        OR (previous_status = 'RESCHEDULED' AND NEW.event_status IN ('RESCHEDULED', 'RELEASED', 'CANCELED'))
        OR (previous_status = 'RELEASED' AND NEW.event_status = 'CORRECTED')
        OR (previous_status = 'CORRECTED' AND NEW.event_status = 'CORRECTED')
    ) THEN
        RAISE EXCEPTION 'invalid lifecycle transition: % -> %', previous_status, NEW.event_status
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS economic_event_lifecycle_chain ON economic_event_lifecycle_versions;
CREATE TRIGGER economic_event_lifecycle_chain
    BEFORE INSERT ON economic_event_lifecycle_versions
    FOR EACH ROW EXECUTE FUNCTION enforce_economic_event_lifecycle_chain();

DROP TRIGGER IF EXISTS economic_event_lifecycle_versions_immutable ON economic_event_lifecycle_versions;
CREATE TRIGGER economic_event_lifecycle_versions_immutable
    BEFORE UPDATE OR DELETE ON economic_event_lifecycle_versions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE OR REPLACE FUNCTION record_economic_event_lifecycle(
    p_economic_event_id TEXT,
    p_lifecycle_version INTEGER,
    p_scheduled_at TIMESTAMPTZ,
    p_released_at TIMESTAMPTZ,
    p_event_status TEXT,
    p_source TEXT,
    p_source_url TEXT,
    p_published_at TIMESTAMPTZ,
    p_first_observed_at TIMESTAMPTZ,
    p_payload_sha256 TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    existing economic_event_lifecycle_versions%ROWTYPE;
    result_id BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_economic_event_id, 0));
    SELECT * INTO existing
      FROM economic_event_lifecycle_versions
     WHERE economic_event_id = p_economic_event_id
       AND lifecycle_version = p_lifecycle_version;
    IF FOUND THEN
        IF existing.scheduled_at = p_scheduled_at
           AND existing.released_at IS NOT DISTINCT FROM p_released_at
           AND existing.event_status = p_event_status
           AND existing.source = p_source
           AND existing.source_url = p_source_url
           AND existing.published_at IS NOT DISTINCT FROM p_published_at
           AND existing.payload_sha256 = p_payload_sha256
        THEN
            RETURN existing.event_lifecycle_version_id;
        END IF;
        RAISE EXCEPTION 'CONFLICTING_LIFECYCLE_FACT: % version %',
            p_economic_event_id, p_lifecycle_version USING ERRCODE = '23505';
    END IF;

    INSERT INTO economic_event_lifecycle_versions (
        economic_event_id, lifecycle_version, scheduled_at, released_at,
        event_status, source, source_url, published_at, first_observed_at,
        payload_sha256
    ) VALUES (
        p_economic_event_id, p_lifecycle_version, p_scheduled_at, p_released_at,
        p_event_status, p_source, p_source_url, p_published_at,
        p_first_observed_at, p_payload_sha256
    ) RETURNING event_lifecycle_version_id INTO result_id;
    RETURN result_id;
END;
$$;

CREATE OR REPLACE VIEW current_economic_event_lifecycle AS
SELECT DISTINCT ON (economic_event_id) *
  FROM economic_event_lifecycle_versions
 ORDER BY economic_event_id, lifecycle_version DESC;

CREATE TABLE IF NOT EXISTS economic_observation_registry (
    observation_code TEXT PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type IN ('CPI', 'EMPLOYMENT', 'PCE', 'FOMC')),
    canonical_unit TEXT NOT NULL,
    value_semantics TEXT NOT NULL,
    source_family TEXT NOT NULL,
    frequency TEXT NOT NULL,
    transform_semantics TEXT NOT NULL,
    CONSTRAINT economic_observation_registry_code_format
        CHECK (observation_code ~ '^[A-Z][A-Z0-9_]*$'),
    CONSTRAINT economic_observation_registry_ontology_identity
        UNIQUE (event_type, observation_code, canonical_unit)
);

INSERT INTO economic_observation_registry (
    observation_code, event_type, canonical_unit, value_semantics,
    source_family, frequency, transform_semantics
) VALUES
    ('CPI_HEADLINE_MOM', 'CPI', 'PERCENT', 'period-over-period percent change', 'BLS_CPI', 'MONTHLY', 'MOM'),
    ('CPI_HEADLINE_YOY', 'CPI', 'PERCENT', 'year-over-year percent change', 'BLS_CPI', 'MONTHLY', 'YOY'),
    ('CPI_CORE_MOM', 'CPI', 'PERCENT', 'period-over-period percent change excluding food and energy', 'BLS_CPI', 'MONTHLY', 'MOM'),
    ('CPI_CORE_YOY', 'CPI', 'PERCENT', 'year-over-year percent change excluding food and energy', 'BLS_CPI', 'MONTHLY', 'YOY'),
    ('NFP_PAYROLL_CHANGE', 'EMPLOYMENT', 'THOUSANDS_PERSONS', 'monthly change in nonfarm payroll employment', 'BLS_EMPLOYMENT', 'MONTHLY', 'LEVEL_CHANGE'),
    ('UNEMPLOYMENT_RATE', 'EMPLOYMENT', 'PERCENT', 'civilian unemployment rate', 'BLS_EMPLOYMENT', 'MONTHLY', 'LEVEL'),
    ('AVERAGE_HOURLY_EARNINGS_MOM', 'EMPLOYMENT', 'PERCENT', 'period-over-period percent change', 'BLS_EMPLOYMENT', 'MONTHLY', 'MOM'),
    ('AVERAGE_HOURLY_EARNINGS_YOY', 'EMPLOYMENT', 'PERCENT', 'year-over-year percent change', 'BLS_EMPLOYMENT', 'MONTHLY', 'YOY'),
    ('PCE_HEADLINE_MOM', 'PCE', 'PERCENT', 'period-over-period percent change', 'BEA_PCE', 'MONTHLY', 'MOM'),
    ('PCE_HEADLINE_YOY', 'PCE', 'PERCENT', 'year-over-year percent change', 'BEA_PCE', 'MONTHLY', 'YOY'),
    ('PCE_CORE_MOM', 'PCE', 'PERCENT', 'period-over-period percent change excluding food and energy', 'BEA_PCE', 'MONTHLY', 'MOM'),
    ('PCE_CORE_YOY', 'PCE', 'PERCENT', 'year-over-year percent change excluding food and energy', 'BEA_PCE', 'MONTHLY', 'YOY'),
    ('FED_TARGET_LOWER', 'FOMC', 'PERCENT', 'lower bound of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_TARGET_UPPER', 'FOMC', 'PERCENT', 'upper bound of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_TARGET_MIDPOINT', 'FOMC', 'PERCENT', 'midpoint of target range', 'FED_POLICY', 'EVENT', 'LEVEL'),
    ('FED_RATE_CHANGE_BP', 'FOMC', 'BASIS_POINTS', 'change in target range midpoint', 'FED_POLICY', 'EVENT', 'LEVEL_CHANGE')
ON CONFLICT (observation_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS economic_release_observations (
    release_observation_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
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
    CONSTRAINT economic_release_observations_source_time_valid
        CHECK (first_observed_at >= published_at),
    CONSTRAINT economic_release_observations_event_fk
        FOREIGN KEY (economic_event_id, event_type)
        REFERENCES canonical_economic_events (economic_event_id, event_type),
    CONSTRAINT economic_release_observations_ontology_fk
        FOREIGN KEY (event_type, observation_code, unit)
        REFERENCES economic_observation_registry (event_type, observation_code, canonical_unit),
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
    source_existing economic_release_observations%ROWTYPE;
    event_kind TEXT;
    result_id BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'economic-observation:' || p_economic_event_id || ':' || p_observation_code,
        0
    ));
    SELECT event_type INTO STRICT event_kind
      FROM canonical_economic_events WHERE economic_event_id = p_economic_event_id;
    IF p_source_revision_id IS NOT NULL THEN
        SELECT * INTO source_existing
          FROM economic_release_observations
         WHERE economic_event_id = p_economic_event_id
           AND observation_code = p_observation_code
           AND source = p_source
           AND source_revision_id = p_source_revision_id;
        IF FOUND AND source_existing.revision_number <> p_revision_number THEN
            RAISE EXCEPTION 'CONFLICTING_SOURCE_FACT: source revision % names revisions % and %',
                p_source_revision_id, source_existing.revision_number, p_revision_number
                USING ERRCODE = '23505';
        END IF;
    END IF;
    SELECT * INTO existing
      FROM economic_release_observations
     WHERE economic_event_id = p_economic_event_id
       AND observation_code = p_observation_code
       AND revision_number = p_revision_number;
    IF FOUND THEN
        IF existing.revision_type = p_revision_type
           AND existing.value = p_value
           AND existing.unit = p_unit
           AND existing.published_at = p_published_at
           AND existing.source = p_source
           AND existing.source_revision_id IS NOT DISTINCT FROM p_source_revision_id
           AND existing.payload_sha256 = p_payload_sha256
        THEN
            RETURN existing.release_observation_id;
        END IF;
        RAISE EXCEPTION 'CONFLICTING_SOURCE_FACT: %/% revision %',
            p_economic_event_id, p_observation_code, p_revision_number
            USING ERRCODE = '23505';
    END IF;

    INSERT INTO economic_release_observations (
        economic_event_id, event_type, observation_code, revision_number,
        source_revision_id, revision_type, value, unit, published_at,
        first_observed_at, source, source_url, payload_sha256
    ) VALUES (
        p_economic_event_id, event_kind, p_observation_code, p_revision_number,
        p_source_revision_id, p_revision_type, p_value, p_unit, p_published_at,
        p_first_observed_at, p_source, p_source_url, p_payload_sha256
    ) RETURNING release_observation_id INTO result_id;
    RETURN result_id;
END;
$$;

DROP TRIGGER IF EXISTS economic_release_observations_immutable ON economic_release_observations;
CREATE TRIGGER economic_release_observations_immutable
    BEFORE UPDATE OR DELETE ON economic_release_observations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS economic_consensus_snapshots (
    consensus_snapshot_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    observation_code TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_snapshot_id TEXT,
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    contributor_count INTEGER CHECK (contributor_count IS NULL OR contributor_count >= 0),
    snapshot_at TIMESTAMPTZ NOT NULL,
    provider_updated_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_consensus_snapshots_source_time_valid
        CHECK (first_observed_at >= snapshot_at),
    CONSTRAINT economic_consensus_snapshots_event_fk
        FOREIGN KEY (economic_event_id, event_type)
        REFERENCES canonical_economic_events (economic_event_id, event_type),
    CONSTRAINT economic_consensus_snapshots_ontology_fk
        FOREIGN KEY (event_type, observation_code, unit)
        REFERENCES economic_observation_registry (event_type, observation_code, canonical_unit),
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
    (economic_event_id, observation_code, provider, snapshot_at DESC);

CREATE OR REPLACE FUNCTION record_economic_consensus_snapshot(
    p_economic_event_id TEXT,
    p_observation_code TEXT,
    p_provider TEXT,
    p_provider_snapshot_id TEXT,
    p_value NUMERIC,
    p_unit TEXT,
    p_contributor_count INTEGER,
    p_snapshot_at TIMESTAMPTZ,
    p_provider_updated_at TIMESTAMPTZ,
    p_first_observed_at TIMESTAMPTZ,
    p_source_url TEXT,
    p_payload_sha256 TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    existing economic_consensus_snapshots%ROWTYPE;
    event_kind TEXT;
    result_id BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'economic-consensus:' || p_economic_event_id || ':'
        || p_observation_code || ':' || p_provider,
        0
    ));
    SELECT event_type INTO STRICT event_kind
      FROM canonical_economic_events WHERE economic_event_id = p_economic_event_id;
    SELECT * INTO existing
      FROM economic_consensus_snapshots
     WHERE economic_event_id = p_economic_event_id
       AND observation_code = p_observation_code
       AND provider = p_provider
       AND snapshot_at = p_snapshot_at;
    IF FOUND THEN
        IF existing.provider_snapshot_id IS NOT DISTINCT FROM p_provider_snapshot_id
           AND existing.value = p_value
           AND existing.unit = p_unit
           AND existing.contributor_count IS NOT DISTINCT FROM p_contributor_count
           AND existing.provider_updated_at IS NOT DISTINCT FROM p_provider_updated_at
           AND existing.source_url = p_source_url
           AND existing.payload_sha256 = p_payload_sha256
        THEN
            RETURN existing.consensus_snapshot_id;
        END IF;
        RAISE EXCEPTION 'CONFLICTING_SOURCE_FACT: consensus %/%/%',
            p_economic_event_id, p_observation_code, p_provider
            USING ERRCODE = '23505';
    END IF;

    INSERT INTO economic_consensus_snapshots (
        economic_event_id, event_type, observation_code, provider,
        provider_snapshot_id, value, unit, contributor_count, snapshot_at,
        provider_updated_at, first_observed_at, source_url, payload_sha256
    ) VALUES (
        p_economic_event_id, event_kind, p_observation_code, p_provider,
        p_provider_snapshot_id, p_value, p_unit, p_contributor_count,
        p_snapshot_at, p_provider_updated_at, p_first_observed_at,
        p_source_url, p_payload_sha256
    ) RETURNING consensus_snapshot_id INTO result_id;
    RETURN result_id;
END;
$$;

DROP TRIGGER IF EXISTS economic_consensus_snapshots_immutable ON economic_consensus_snapshots;
CREATE TRIGGER economic_consensus_snapshots_immutable
    BEFORE UPDATE OR DELETE ON economic_consensus_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS economic_event_markers (
    economic_event_marker_id TEXT PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    marker_kind TEXT NOT NULL CHECK (marker_kind IN ('RELEASE', 'STATEMENT', 'PRESS_CONFERENCE')),
    marker_role TEXT NOT NULL CHECK (marker_role IN ('PRIMARY', 'SECONDARY')),
    marker_revision INTEGER NOT NULL CHECK (marker_revision >= 1),
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
    CONSTRAINT economic_event_markers_revision_identity
        UNIQUE (economic_event_id, marker_kind, marker_revision),
    CONSTRAINT economic_event_markers_reference_identity
        UNIQUE (economic_event_marker_id, economic_event_id)
);

CREATE OR REPLACE VIEW current_economic_event_markers AS
SELECT DISTINCT ON (economic_event_id, marker_kind) *
  FROM economic_event_markers
 ORDER BY economic_event_id, marker_kind, marker_revision DESC;

CREATE OR REPLACE FUNCTION enforce_event_marker_revision_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    prior_revision INTEGER;
    prior_role TEXT;
    event_marker_count INTEGER;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'economic-event-marker:' || NEW.economic_event_id,
        0
    ));
    SELECT marker_revision, marker_role INTO prior_revision, prior_role
      FROM economic_event_markers
     WHERE economic_event_id = NEW.economic_event_id
       AND marker_kind = NEW.marker_kind
     ORDER BY marker_revision DESC
     LIMIT 1;
    SELECT count(*) INTO event_marker_count
      FROM economic_event_markers
     WHERE economic_event_id = NEW.economic_event_id;

    IF prior_revision IS NULL AND NEW.marker_revision <> 1 THEN
        RAISE EXCEPTION 'first marker revision must be 1' USING ERRCODE = '23514';
    ELSIF prior_revision IS NOT NULL AND NEW.marker_revision <> prior_revision + 1 THEN
        RAISE EXCEPTION 'marker revision must be consecutive' USING ERRCODE = '23514';
    END IF;
    IF prior_role = 'PRIMARY' AND NEW.marker_role <> 'PRIMARY' THEN
        RAISE EXCEPTION 'current primary marker cannot be demoted by revision'
            USING ERRCODE = '23514';
    END IF;
    IF event_marker_count = 0 AND NEW.marker_role <> 'PRIMARY' THEN
        RAISE EXCEPTION 'first event marker must be primary' USING ERRCODE = '23514';
    END IF;
    IF NEW.marker_role = 'PRIMARY' AND EXISTS (
        SELECT 1 FROM current_economic_event_markers
         WHERE economic_event_id = NEW.economic_event_id
           AND marker_kind <> NEW.marker_kind
           AND marker_role = 'PRIMARY'
    ) THEN
        RAISE EXCEPTION 'event already has a different current primary marker'
            USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS economic_event_marker_revision_chain ON economic_event_markers;
CREATE TRIGGER economic_event_marker_revision_chain
    BEFORE INSERT ON economic_event_markers
    FOR EACH ROW EXECUTE FUNCTION enforce_event_marker_revision_chain();

DROP TRIGGER IF EXISTS economic_event_markers_immutable ON economic_event_markers;
CREATE TRIGGER economic_event_markers_immutable
    BEFORE UPDATE OR DELETE ON economic_event_markers
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE INDEX IF NOT EXISTS economic_event_markers_event_idx
    ON economic_event_markers (economic_event_id, marker_kind, marker_revision DESC);

CREATE OR REPLACE FUNCTION select_canonical_prerelease_consensus(
    p_economic_event_id TEXT,
    p_observation_code TEXT,
    p_provider TEXT
)
RETURNS BIGINT
LANGUAGE sql
STABLE
AS $$
    SELECT consensus.consensus_snapshot_id
      FROM economic_consensus_snapshots AS consensus
      JOIN current_economic_event_markers AS marker
        ON marker.economic_event_id = consensus.economic_event_id
       AND marker.marker_role = 'PRIMARY'
     WHERE consensus.economic_event_id = p_economic_event_id
       AND consensus.observation_code = p_observation_code
       AND consensus.provider = p_provider
       AND consensus.snapshot_at < marker.marker_at
     ORDER BY consensus.snapshot_at DESC, consensus.consensus_snapshot_id DESC
     LIMIT 1
$$;

CREATE TABLE IF NOT EXISTS economic_surprises (
    economic_surprise_id BIGSERIAL PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES canonical_economic_events(economic_event_id),
    observation_code TEXT NOT NULL,
    unit TEXT NOT NULL,
    economic_event_marker_id TEXT NOT NULL,
    actual_observation_id BIGINT NOT NULL,
    consensus_snapshot_id BIGINT NOT NULL,
    surprise_value NUMERIC NOT NULL,
    standardized_surprise NUMERIC CHECK (standardized_surprise IS NULL),
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
    CONSTRAINT economic_surprises_marker_identity_fk
        FOREIGN KEY (economic_event_marker_id, economic_event_id)
        REFERENCES economic_event_markers
            (economic_event_marker_id, economic_event_id),
    CONSTRAINT economic_surprises_input_version_identity
        UNIQUE (
            economic_event_marker_id, actual_observation_id,
            consensus_snapshot_id, algorithm_version
        )
);

ALTER TABLE economic_surprises
    ADD COLUMN IF NOT EXISTS economic_event_marker_id TEXT;

ALTER TABLE economic_surprises
    DROP CONSTRAINT IF EXISTS economic_surprises_input_version_identity;
ALTER TABLE economic_surprises
    ADD CONSTRAINT economic_surprises_input_version_identity UNIQUE (
        economic_event_marker_id, actual_observation_id,
        consensus_snapshot_id, algorithm_version
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'economic_surprises_marker_identity_fk'
           AND conrelid = 'economic_surprises'::regclass
    ) THEN
        ALTER TABLE economic_surprises
            ADD CONSTRAINT economic_surprises_marker_identity_fk
            FOREIGN KEY (economic_event_marker_id, economic_event_id)
            REFERENCES economic_event_markers
                (economic_event_marker_id, economic_event_id);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_canonical_surprise()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    actual economic_release_observations%ROWTYPE;
    consensus economic_consensus_snapshots%ROWTYPE;
    current_marker economic_event_markers%ROWTYPE;
    canonical_consensus_id BIGINT;
BEGIN
    SELECT * INTO STRICT actual FROM economic_release_observations
     WHERE release_observation_id = NEW.actual_observation_id;
    SELECT * INTO STRICT consensus FROM economic_consensus_snapshots
     WHERE consensus_snapshot_id = NEW.consensus_snapshot_id;
    SELECT * INTO STRICT current_marker FROM current_economic_event_markers
     WHERE economic_event_id = NEW.economic_event_id AND marker_role = 'PRIMARY';

    IF NEW.economic_event_marker_id IS NULL THEN
        NEW.economic_event_marker_id := current_marker.economic_event_marker_id;
    ELSIF NEW.economic_event_marker_id <> current_marker.economic_event_marker_id THEN
        RAISE EXCEPTION 'canonical surprise requires current primary marker'
            USING ERRCODE = '23514';
    END IF;

    IF actual.revision_number <> 0 OR actual.revision_type <> 'INITIAL' THEN
        RAISE EXCEPTION 'canonical surprise requires initial official observation'
            USING ERRCODE = '23514';
    END IF;
    IF actual.published_at < current_marker.marker_at THEN
        RAISE EXCEPTION 'initial actual cannot be available before current primary marker'
            USING ERRCODE = '23514';
    END IF;
    canonical_consensus_id := select_canonical_prerelease_consensus(
        NEW.economic_event_id, NEW.observation_code, consensus.provider
    );
    IF canonical_consensus_id IS NULL OR canonical_consensus_id <> NEW.consensus_snapshot_id THEN
        RAISE EXCEPTION 'canonical surprise requires latest pre-release provider consensus'
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
    IF NEW.algorithm_version <> 'canonical_event_surprise_v1' THEN
        RAISE EXCEPTION 'unsupported canonical surprise algorithm version'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS economic_surprises_canonical_inputs ON economic_surprises;
CREATE TRIGGER economic_surprises_canonical_inputs
    BEFORE INSERT ON economic_surprises
    FOR EACH ROW EXECUTE FUNCTION enforce_canonical_surprise();

DROP TRIGGER IF EXISTS economic_surprises_immutable ON economic_surprises;
CREATE TRIGGER economic_surprises_immutable
    BEFORE UPDATE OR DELETE ON economic_surprises
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE OR REPLACE VIEW current_canonical_surprises AS
SELECT surprise.*
  FROM economic_surprises AS surprise
  JOIN economic_release_observations AS actual
    ON actual.release_observation_id = surprise.actual_observation_id
  JOIN economic_consensus_snapshots AS consensus
    ON consensus.consensus_snapshot_id = surprise.consensus_snapshot_id
  JOIN current_economic_event_markers AS marker
    ON marker.economic_event_marker_id = surprise.economic_event_marker_id
   AND marker.economic_event_id = surprise.economic_event_id
   AND marker.marker_role = 'PRIMARY'
 WHERE actual.revision_number = 0
   AND actual.revision_type = 'INITIAL'
   AND actual.published_at >= marker.marker_at
   AND surprise.consensus_snapshot_id = select_canonical_prerelease_consensus(
       surprise.economic_event_id, surprise.observation_code, consensus.provider
   )
   AND actual.unit = consensus.unit
   AND actual.unit = surprise.unit
   AND surprise.surprise_value = actual.value - consensus.value
   AND surprise.standardized_surprise IS NULL
   AND surprise.algorithm_version = 'canonical_event_surprise_v1';

CREATE TABLE IF NOT EXISTS calendar_snapshots (
    calendar_snapshot_id TEXT PRIMARY KEY,
    market_code TEXT NOT NULL CHECK (market_code = 'US_EQUITIES'),
    exchange_timezone TEXT NOT NULL CHECK (exchange_timezone = 'America/New_York'),
    calendar_source TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT calendar_snapshots_reference_identity
        UNIQUE (calendar_snapshot_id, market_code, exchange_timezone)
);

DROP TRIGGER IF EXISTS calendar_snapshots_immutable ON calendar_snapshots;
CREATE TRIGGER calendar_snapshots_immutable
    BEFORE UPDATE OR DELETE ON calendar_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS trading_sessions (
    calendar_snapshot_id TEXT NOT NULL REFERENCES calendar_snapshots(calendar_snapshot_id),
    session_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    session_day_type TEXT NOT NULL CHECK (session_day_type IN ('REGULAR', 'EARLY_CLOSE')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (calendar_snapshot_id, session_date),
    CONSTRAINT trading_sessions_interval_valid CHECK (closes_at > opens_at)
);

CREATE OR REPLACE FUNCTION enforce_trading_session_local_date()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    parent_timezone TEXT;
BEGIN
    SELECT exchange_timezone INTO STRICT parent_timezone
      FROM calendar_snapshots
     WHERE calendar_snapshot_id = NEW.calendar_snapshot_id;
    IF (NEW.opens_at AT TIME ZONE parent_timezone)::DATE <> NEW.session_date
       OR (NEW.closes_at AT TIME ZONE parent_timezone)::DATE <> NEW.session_date
    THEN
        RAISE EXCEPTION 'session local dates must match session_date'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trading_sessions_local_date_valid ON trading_sessions;
CREATE TRIGGER trading_sessions_local_date_valid
    BEFORE INSERT ON trading_sessions
    FOR EACH ROW EXECUTE FUNCTION enforce_trading_session_local_date();

DROP TRIGGER IF EXISTS trading_sessions_immutable ON trading_sessions;
CREATE TRIGGER trading_sessions_immutable
    BEFORE UPDATE OR DELETE ON trading_sessions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE INDEX IF NOT EXISTS trading_sessions_lookup_idx
    ON trading_sessions (calendar_snapshot_id, session_date DESC);

CREATE TABLE IF NOT EXISTS validation_runs (
    validation_run_id TEXT PRIMARY KEY,
    workload_id TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    checkpoint_namespace TEXT NOT NULL,
    source_manifest_identity TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT validation_runs_reference_identity
        UNIQUE (validation_run_id, processor_version, checkpoint_namespace)
);

CREATE OR REPLACE FUNCTION record_validation_run(
    p_validation_run_id TEXT,
    p_workload_id TEXT,
    p_processor_version TEXT,
    p_checkpoint_namespace TEXT,
    p_source_manifest_identity TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    existing validation_runs%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'validation-run:' || p_validation_run_id,
        0
    ));
    SELECT * INTO existing FROM validation_runs
     WHERE validation_run_id = p_validation_run_id;
    IF FOUND THEN
        IF existing.workload_id = p_workload_id
           AND existing.processor_version = p_processor_version
           AND existing.checkpoint_namespace = p_checkpoint_namespace
           AND existing.source_manifest_identity IS NOT DISTINCT FROM p_source_manifest_identity
        THEN
            RETURN existing.validation_run_id;
        END IF;
        RAISE EXCEPTION 'VALIDATION_RUN_CONFLICT: %', p_validation_run_id
            USING ERRCODE = '23505';
    END IF;
    INSERT INTO validation_runs (
        validation_run_id, workload_id, processor_version,
        checkpoint_namespace, source_manifest_identity
    ) VALUES (
        p_validation_run_id, p_workload_id, p_processor_version,
        p_checkpoint_namespace, p_source_manifest_identity
    );
    RETURN p_validation_run_id;
END;
$$;

DROP TRIGGER IF EXISTS validation_runs_immutable ON validation_runs;
CREATE TRIGGER validation_runs_immutable
    BEFORE UPDATE OR DELETE ON validation_runs
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

CREATE TABLE IF NOT EXISTS validation_reconstructed_bars (
    validation_reconstructed_bar_id BIGSERIAL PRIMARY KEY,
    validation_run_id TEXT NOT NULL REFERENCES validation_runs(validation_run_id),
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT validation_reconstructed_bars_high_valid
        CHECK (high >= GREATEST(open, low, close)),
    CONSTRAINT validation_reconstructed_bars_low_valid
        CHECK (low <= LEAST(open, high, close)),
    CONSTRAINT validation_reconstructed_bars_identity
        UNIQUE (validation_run_id, symbol, bar_start, timeframe, source, feed)
);

CREATE OR REPLACE FUNCTION record_validation_reconstructed_bar(
    p_validation_run_id TEXT,
    p_symbol TEXT,
    p_bar_start TIMESTAMPTZ,
    p_timeframe TEXT,
    p_open NUMERIC,
    p_high NUMERIC,
    p_low NUMERIC,
    p_close NUMERIC,
    p_volume BIGINT,
    p_trade_count BIGINT,
    p_vwap NUMERIC,
    p_source TEXT,
    p_feed TEXT,
    p_is_final BOOLEAN,
    p_condition_policy TEXT,
    p_spark_batch_id BIGINT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    existing validation_reconstructed_bars%ROWTYPE;
    result_id BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format(
            'validation-bar:%s|%s|%s|%s|%s|%s',
            p_validation_run_id, p_symbol, p_bar_start, p_timeframe, p_source, p_feed
        ),
        0
    ));
    SELECT * INTO existing FROM validation_reconstructed_bars
     WHERE validation_run_id = p_validation_run_id
       AND symbol = p_symbol
       AND bar_start = p_bar_start
       AND timeframe = p_timeframe
       AND source = p_source
       AND feed = p_feed;
    IF FOUND THEN
        IF existing.open = p_open AND existing.high = p_high
           AND existing.low = p_low AND existing.close = p_close
           AND existing.volume = p_volume AND existing.trade_count = p_trade_count
           AND existing.vwap IS NOT DISTINCT FROM p_vwap
           AND existing.is_final = p_is_final
           AND existing.condition_policy = p_condition_policy
        THEN
            RETURN existing.validation_reconstructed_bar_id;
        END IF;
        RAISE EXCEPTION 'VALIDATION_BAR_DETERMINISM_CONFLICT: % %', p_symbol, p_bar_start
            USING ERRCODE = '23505';
    END IF;
    INSERT INTO validation_reconstructed_bars (
        validation_run_id, symbol, bar_start, timeframe,
        open, high, low, close, volume, trade_count, vwap,
        source, feed, is_final, condition_policy, spark_batch_id
    ) VALUES (
        p_validation_run_id, p_symbol, p_bar_start, p_timeframe,
        p_open, p_high, p_low, p_close, p_volume, p_trade_count, p_vwap,
        p_source, p_feed, p_is_final, p_condition_policy, p_spark_batch_id
    ) RETURNING validation_reconstructed_bar_id INTO result_id;
    RETURN result_id;
END;
$$;

DROP TRIGGER IF EXISTS validation_reconstructed_bars_immutable ON validation_reconstructed_bars;
CREATE TRIGGER validation_reconstructed_bars_immutable
    BEFORE UPDATE OR DELETE ON validation_reconstructed_bars
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact_mutation();

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
    bars.validation_run_id,
    runs.processor_version,
    bars.symbol, bars.bar_start, bars.timeframe,
    bars.open, bars.high, bars.low, bars.close,
    bars.volume, bars.trade_count, bars.vwap,
    bars.source, bars.feed, bars.condition_policy
FROM validation_reconstructed_bars AS bars
JOIN validation_runs AS runs USING (validation_run_id);
