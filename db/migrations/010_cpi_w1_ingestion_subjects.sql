-- CPI W1 additive ingestion/evidence foundation.
-- Legacy pipeline_* telemetry and migrations 001-009 remain unchanged.

CREATE TABLE IF NOT EXISTS data_sources (
    source_code TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);

INSERT INTO data_sources (source_code, display_name)
VALUES ('BLS', 'U.S. Bureau of Labor Statistics')
ON CONFLICT (source_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id UUID PRIMARY KEY,
    execution_scope TEXT NOT NULL CHECK (
        execution_scope IN ('ECONOMIC_COLLECT', 'ECONOMIC_PROMOTE')
    ),
    data_domain TEXT NOT NULL CHECK (data_domain = 'ECONOMIC'),
    job_type TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    run_mode TEXT NOT NULL CHECK (run_mode IN ('LIVE', 'BACKFILL', 'REPLAY')),
    trigger_idempotency_key TEXT,
    scheduled_for TIMESTAMPTZ,
    replay_of_run_id UUID REFERENCES ingestion_runs(run_id),
    source_revision TEXT NOT NULL,
    workload_artifact_digest TEXT NOT NULL,
    job_contract_version TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'CREATED' CHECK (
        state IN ('CREATED', 'RUNNING', 'TERMINAL')
    ),
    outcome TEXT CHECK (outcome IN ('SUCCEEDED', 'PARTIAL', 'FAILED', 'NO_WORK')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT ingestion_runs_scope_identity
        UNIQUE (run_id, execution_scope, data_domain),
    CONSTRAINT ingestion_runs_replay_reference_valid CHECK (
        (run_mode = 'REPLAY' AND replay_of_run_id IS NOT NULL AND replay_of_run_id <> run_id)
        OR (run_mode IN ('LIVE', 'BACKFILL') AND replay_of_run_id IS NULL)
    ),
    CONSTRAINT ingestion_runs_state_timestamps_valid CHECK (
        (state = 'CREATED' AND outcome IS NULL AND started_at IS NULL AND finished_at IS NULL)
        OR (state = 'RUNNING' AND outcome IS NULL AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (state = 'TERMINAL' AND outcome IS NOT NULL AND finished_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ingestion_runs_trigger_idempotency
    ON ingestion_runs (
        data_domain, execution_scope, job_type, trigger_type, trigger_idempotency_key
    )
    WHERE trigger_idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingestion_work_items (
    work_item_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    execution_scope TEXT NOT NULL CHECK (
        execution_scope IN ('ECONOMIC_COLLECT', 'ECONOMIC_PROMOTE')
    ),
    data_domain TEXT NOT NULL CHECK (data_domain = 'ECONOMIC'),
    work_key TEXT NOT NULL,
    input_artifact_id UUID,
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING', 'CLAIMED', 'TERMINAL')
    ),
    outcome TEXT CHECK (
        outcome IN ('SUCCEEDED', 'FAILED', 'QUARANTINED', 'DATA_NOT_AVAILABLE', 'SKIPPED')
    ),
    reason_code TEXT,
    next_claim_at TIMESTAMPTZ,
    claim_generation INTEGER NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    claim_token UUID,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ingestion_work_items_run_fk
        FOREIGN KEY (run_id, execution_scope, data_domain)
        REFERENCES ingestion_runs (run_id, execution_scope, data_domain),
    CONSTRAINT ingestion_work_items_scope_identity
        UNIQUE (work_item_id, execution_scope, data_domain),
    CONSTRAINT ingestion_work_items_business_identity
        UNIQUE (run_id, work_key),
    CONSTRAINT ingestion_work_items_state_claim_valid CHECK (
        (state = 'PENDING' AND outcome IS NULL AND claim_token IS NULL AND lease_until IS NULL)
        OR (state = 'CLAIMED' AND outcome IS NULL AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state = 'TERMINAL' AND outcome IS NOT NULL AND claim_token IS NULL AND lease_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ingestion_work_items_claimable_idx
    ON ingestion_work_items (data_domain, execution_scope, state, next_claim_at, lease_until);

CREATE TABLE IF NOT EXISTS ingestion_attempts (
    attempt_id UUID PRIMARY KEY,
    work_item_id UUID NOT NULL,
    execution_scope TEXT NOT NULL CHECK (
        execution_scope IN ('ECONOMIC_COLLECT', 'ECONOMIC_PROMOTE')
    ),
    data_domain TEXT NOT NULL CHECK (data_domain = 'ECONOMIC'),
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    state TEXT NOT NULL DEFAULT 'RUNNING' CHECK (state IN ('RUNNING', 'TERMINAL')),
    outcome TEXT CHECK (
        outcome IN ('SUCCEEDED', 'FAILED', 'QUARANTINED', 'DATA_NOT_AVAILABLE')
    ),
    reason_code TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    CONSTRAINT ingestion_attempts_work_fk
        FOREIGN KEY (work_item_id, execution_scope, data_domain)
        REFERENCES ingestion_work_items (work_item_id, execution_scope, data_domain),
    CONSTRAINT ingestion_attempts_scope_identity
        UNIQUE (attempt_id, execution_scope, data_domain),
    CONSTRAINT ingestion_attempts_execution_identity
        UNIQUE (work_item_id, attempt_number),
    CONSTRAINT ingestion_attempts_state_timestamps_valid CHECK (
        (state = 'RUNNING' AND outcome IS NULL AND finished_at IS NULL)
        OR (state = 'TERMINAL' AND outcome IS NOT NULL AND finished_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS source_artifacts (
    artifact_id UUID PRIMARY KEY,
    data_domain TEXT NOT NULL CHECK (data_domain = 'ECONOMIC'),
    source_code TEXT NOT NULL REFERENCES data_sources(source_code),
    artifact_contract_kind TEXT NOT NULL,
    source_contract_version TEXT NOT NULL,
    locator_key TEXT NOT NULL,
    retrieval_url TEXT,
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_type TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    created_by_attempt_id UUID NOT NULL,
    created_by_execution_scope TEXT NOT NULL CHECK (
        created_by_execution_scope = 'ECONOMIC_COLLECT'
    ),
    content_state TEXT NOT NULL CHECK (
        content_state IN ('RETAINED', 'NOT_RETAINED', 'DELETED_BY_POLICY')
    ),
    storage_uri TEXT,
    storage_generation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT source_artifacts_attempt_fk
        FOREIGN KEY (created_by_attempt_id, created_by_execution_scope, data_domain)
        REFERENCES ingestion_attempts (attempt_id, execution_scope, data_domain),
    CONSTRAINT source_artifacts_domain_identity
        UNIQUE (artifact_id, data_domain),
    CONSTRAINT source_artifacts_capture_identity
        UNIQUE (created_by_attempt_id, locator_key, content_sha256),
    CONSTRAINT source_artifacts_retention_valid CHECK (
        (content_state IN ('RETAINED', 'DELETED_BY_POLICY')
            AND storage_uri IS NOT NULL AND storage_generation IS NOT NULL)
        OR (content_state = 'NOT_RETAINED'
            AND storage_uri IS NULL AND storage_generation IS NULL)
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ingestion_work_items_input_artifact_fk'
           AND conrelid = 'ingestion_work_items'::regclass
    ) THEN
        ALTER TABLE ingestion_work_items
            ADD CONSTRAINT ingestion_work_items_input_artifact_fk
            FOREIGN KEY (input_artifact_id, data_domain)
            REFERENCES source_artifacts (artifact_id, data_domain);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS interpretation_subjects (
    subject_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL CHECK (
        subject_type IN (
            'SCHEDULE_ASSERTION',
            'EVENT_DISCLOSURE_LINK',
            'DISCLOSURE_ARTIFACT_LINK',
            'DISCLOSURE_MARKER_ASSERTION',
            'OFFICIAL_OBSERVATION_ASSERTION'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT interpretation_subjects_typed_identity
        UNIQUE (subject_id, subject_type)
);


CREATE OR REPLACE FUNCTION enforce_ingestion_initial_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME = 'ingestion_runs' AND NEW.state <> 'CREATED' THEN
        RAISE EXCEPTION 'new ingestion run must start CREATED'
            USING ERRCODE = '23514';
    ELSIF TG_TABLE_NAME = 'ingestion_work_items' AND NEW.state <> 'PENDING' THEN
        RAISE EXCEPTION 'new ingestion work must start PENDING'
            USING ERRCODE = '23514';
    ELSIF TG_TABLE_NAME = 'ingestion_attempts' AND NEW.state <> 'RUNNING' THEN
        RAISE EXCEPTION 'new ingestion attempt must start RUNNING'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_runs_initial_state_guard ON ingestion_runs;
CREATE TRIGGER ingestion_runs_initial_state_guard
    BEFORE INSERT ON ingestion_runs
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_initial_state();

DROP TRIGGER IF EXISTS ingestion_work_items_initial_state_guard ON ingestion_work_items;
CREATE TRIGGER ingestion_work_items_initial_state_guard
    BEFORE INSERT ON ingestion_work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_initial_state();

DROP TRIGGER IF EXISTS ingestion_attempts_initial_state_guard ON ingestion_attempts;
CREATE TRIGGER ingestion_attempts_initial_state_guard
    BEFORE INSERT ON ingestion_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_initial_state();

CREATE OR REPLACE FUNCTION enforce_ingestion_run_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'TERMINAL' AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'terminal ingestion run has no outgoing transition'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'CREATED' AND NEW.state IN ('RUNNING', 'TERMINAL'))
        OR (OLD.state = 'RUNNING' AND NEW.state = 'TERMINAL')
    ) THEN
        RAISE EXCEPTION 'invalid ingestion run transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state = 'TERMINAL' AND OLD.state <> 'TERMINAL' AND EXISTS (
        SELECT 1 FROM ingestion_work_items
         WHERE run_id = NEW.run_id AND state <> 'TERMINAL'
    ) THEN
        RAISE EXCEPTION 'cannot terminalize ingestion run with non-terminal work'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_runs_transition_guard ON ingestion_runs;
CREATE TRIGGER ingestion_runs_transition_guard
    BEFORE UPDATE ON ingestion_runs
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_run_transition();

CREATE OR REPLACE FUNCTION enforce_ingestion_work_insert_parent_open()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    parent_state TEXT;
BEGIN
    SELECT state
      INTO parent_state
      FROM ingestion_runs
     WHERE run_id = NEW.run_id
       AND execution_scope = NEW.execution_scope
       AND data_domain = NEW.data_domain
     FOR UPDATE;

    IF FOUND AND parent_state = 'TERMINAL' THEN
        RAISE EXCEPTION 'terminal ingestion run cannot receive new work'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_work_items_parent_open_guard ON ingestion_work_items;
CREATE TRIGGER ingestion_work_items_parent_open_guard
    BEFORE INSERT ON ingestion_work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_work_insert_parent_open();

CREATE OR REPLACE FUNCTION enforce_ingestion_work_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'TERMINAL' AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'terminal ingestion work has no outgoing transition'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'PENDING' AND NEW.state = 'CLAIMED')
        OR (OLD.state = 'PENDING' AND NEW.state = 'TERMINAL' AND NEW.outcome = 'SKIPPED')
        OR (OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'TERMINAL'))
    ) THEN
        RAISE EXCEPTION 'invalid ingestion work transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'PENDING' AND NEW.state = 'CLAIMED'
       AND NEW.claim_generation <> OLD.claim_generation + 1 THEN
        RAISE EXCEPTION 'claim generation must advance on claim'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
       AND NEW.claim_generation NOT IN (OLD.claim_generation, OLD.claim_generation + 1) THEN
        RAISE EXCEPTION 'claim generation may only stay stable or advance by one'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
       AND NEW.claim_generation = OLD.claim_generation + 1
       AND OLD.lease_until > CURRENT_TIMESTAMP THEN
        RAISE EXCEPTION 'active claim cannot be reclaimed before lease expiry'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'TERMINAL')
       AND NEW.claim_generation <> OLD.claim_generation THEN
        RAISE EXCEPTION 'terminal/retry transition must preserve claim generation'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_work_items_transition_guard ON ingestion_work_items;
CREATE TRIGGER ingestion_work_items_transition_guard
    BEFORE UPDATE ON ingestion_work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_work_transition();

CREATE OR REPLACE FUNCTION enforce_ingestion_attempt_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'TERMINAL' AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'terminal ingestion attempt has no outgoing transition'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (OLD.state = 'RUNNING' AND NEW.state = 'TERMINAL') THEN
        RAISE EXCEPTION 'invalid ingestion attempt transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_attempts_transition_guard ON ingestion_attempts;
CREATE TRIGGER ingestion_attempts_transition_guard
    BEFORE UPDATE ON ingestion_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_attempt_transition();
