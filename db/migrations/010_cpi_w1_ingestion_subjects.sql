-- CPI W1 additive ingestion/evidence foundation.
-- Legacy pipeline_* telemetry and migrations 001-009 remain unchanged.

CREATE TABLE IF NOT EXISTS data_sources (
    source_code TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);

INSERT INTO data_sources (source_code, display_name)
VALUES ('BLS', 'U.S. Bureau of Labor Statistics')
ON CONFLICT (source_code) DO NOTHING;

DO $bls_seed$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM data_sources
         WHERE source_code = 'BLS'
           AND display_name = 'U.S. Bureau of Labor Statistics'
    ) THEN
        RAISE EXCEPTION 'BLS source registry seed mismatch'
            USING ERRCODE = '23514';
    END IF;
END;
$bls_seed$;

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
    ),
    CONSTRAINT ingestion_work_items_reason_valid CHECK (
        (state IN ('PENDING', 'CLAIMED') AND reason_code IS NULL)
        OR (state = 'TERMINAL' AND outcome = 'SUCCEEDED' AND reason_code IS NULL)
        OR (
            state = 'TERMINAL'
            AND outcome IN ('FAILED', 'QUARANTINED', 'DATA_NOT_AVAILABLE', 'SKIPPED')
            AND reason_code IS NOT NULL
            AND reason_code ~ '^[A-Z][A-Z0-9_]*$'
        )
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
    ),
    CONSTRAINT ingestion_attempts_reason_valid CHECK (
        (state = 'RUNNING' AND reason_code IS NULL)
        OR (state = 'TERMINAL' AND outcome = 'SUCCEEDED' AND reason_code IS NULL)
        OR (
            state = 'TERMINAL'
            AND outcome IN ('FAILED', 'QUARANTINED', 'DATA_NOT_AVAILABLE')
            AND reason_code IS NOT NULL
            AND reason_code ~ '^[A-Z][A-Z0-9_]*$'
        )
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

CREATE OR REPLACE FUNCTION enforce_ingestion_attempt_claim_alignment()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $claim_alignment$
DECLARE
    parent_state TEXT;
    parent_generation INTEGER;
BEGIN
    SELECT state, claim_generation
      INTO STRICT parent_state, parent_generation
      FROM ingestion_work_items
     WHERE work_item_id = NEW.work_item_id;

    IF parent_state <> 'CLAIMED' THEN
        RAISE EXCEPTION 'ingestion attempt requires claimed work ownership'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.attempt_number <> parent_generation THEN
        RAISE EXCEPTION 'attempt number must equal current claim generation'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$claim_alignment$;

DROP TRIGGER IF EXISTS ingestion_attempts_claim_alignment_guard ON ingestion_attempts;
CREATE TRIGGER ingestion_attempts_claim_alignment_guard
    BEFORE INSERT ON ingestion_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_attempt_claim_alignment();

CREATE OR REPLACE FUNCTION enforce_ingestion_run_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $run_transition$
DECLARE
    total_work INTEGER;
    non_skipped_work INTEGER;
    successful_work INTEGER;
    failed_work INTEGER;
    expected_outcome TEXT;
BEGIN
    IF NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.execution_scope IS DISTINCT FROM OLD.execution_scope
       OR NEW.data_domain IS DISTINCT FROM OLD.data_domain
       OR NEW.job_type IS DISTINCT FROM OLD.job_type
       OR NEW.trigger_type IS DISTINCT FROM OLD.trigger_type
       OR NEW.run_mode IS DISTINCT FROM OLD.run_mode
       OR NEW.trigger_idempotency_key IS DISTINCT FROM OLD.trigger_idempotency_key
       OR NEW.scheduled_for IS DISTINCT FROM OLD.scheduled_for
       OR NEW.replay_of_run_id IS DISTINCT FROM OLD.replay_of_run_id
       OR NEW.source_revision IS DISTINCT FROM OLD.source_revision
       OR NEW.workload_artifact_digest IS DISTINCT FROM OLD.workload_artifact_digest
       OR NEW.job_contract_version IS DISTINCT FROM OLD.job_contract_version
       OR NEW.config_fingerprint IS DISTINCT FROM OLD.config_fingerprint
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'ingestion run identity and lineage are immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'TERMINAL' THEN
        RAISE EXCEPTION 'terminal ingestion run is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
        RAISE EXCEPTION 'ingestion run started_at is immutable once set'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'CREATED' AND NEW.state IN ('RUNNING', 'TERMINAL'))
        OR (OLD.state = 'RUNNING' AND NEW.state = 'TERMINAL')
    ) THEN
        RAISE EXCEPTION 'invalid ingestion run transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state = 'TERMINAL' THEN
        IF EXISTS (
            SELECT 1 FROM ingestion_work_items
             WHERE run_id = NEW.run_id AND state <> 'TERMINAL'
        ) THEN
            RAISE EXCEPTION 'cannot terminalize ingestion run with non-terminal work'
                USING ERRCODE = '23514';
        END IF;

        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE outcome <> 'SKIPPED'),
            COUNT(*) FILTER (WHERE outcome IN ('SUCCEEDED', 'DATA_NOT_AVAILABLE')),
            COUNT(*) FILTER (WHERE outcome = 'FAILED')
          INTO total_work, non_skipped_work, successful_work, failed_work
          FROM ingestion_work_items
         WHERE run_id = NEW.run_id;

        IF total_work = 0 THEN
            IF OLD.state = 'CREATED' AND NEW.outcome IN ('NO_WORK', 'FAILED') THEN
                RETURN NEW;
            END IF;
            expected_outcome := 'NO_WORK';
        ELSIF non_skipped_work = 0 THEN
            expected_outcome := 'NO_WORK';
        ELSIF successful_work = non_skipped_work THEN
            expected_outcome := 'SUCCEEDED';
        ELSIF failed_work = non_skipped_work THEN
            expected_outcome := 'FAILED';
        ELSE
            expected_outcome := 'PARTIAL';
        END IF;

        IF NEW.outcome <> expected_outcome THEN
            RAISE EXCEPTION 'ingestion run outcome does not match terminal work aggregation'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$run_transition$;

DROP TRIGGER IF EXISTS ingestion_runs_transition_guard ON ingestion_runs;
CREATE TRIGGER ingestion_runs_transition_guard
    BEFORE UPDATE ON ingestion_runs
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_run_transition();

CREATE OR REPLACE FUNCTION finalize_ingestion_run_if_complete(
    p_run_id UUID
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $finalize_run$
DECLARE
    run_state TEXT;
    total_work INTEGER;
    non_skipped_work INTEGER;
    successful_work INTEGER;
    failed_work INTEGER;
    expected_outcome TEXT;
BEGIN
    SELECT state
      INTO STRICT run_state
      FROM ingestion_runs
     WHERE run_id = p_run_id
     FOR UPDATE;

    IF run_state = 'TERMINAL' THEN
        RETURN FALSE;
    END IF;

    IF EXISTS (
        SELECT 1
          FROM ingestion_work_items
         WHERE run_id = p_run_id
           AND state <> 'TERMINAL'
    ) THEN
        RETURN FALSE;
    END IF;

    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE outcome <> 'SKIPPED'),
        COUNT(*) FILTER (WHERE outcome IN ('SUCCEEDED', 'DATA_NOT_AVAILABLE')),
        COUNT(*) FILTER (WHERE outcome = 'FAILED')
      INTO total_work, non_skipped_work, successful_work, failed_work
      FROM ingestion_work_items
     WHERE run_id = p_run_id;

    IF total_work = 0 OR non_skipped_work = 0 THEN
        expected_outcome := 'NO_WORK';
    ELSIF successful_work = non_skipped_work THEN
        expected_outcome := 'SUCCEEDED';
    ELSIF failed_work = non_skipped_work THEN
        expected_outcome := 'FAILED';
    ELSE
        expected_outcome := 'PARTIAL';
    END IF;

    UPDATE ingestion_runs
       SET state = 'TERMINAL',
           outcome = expected_outcome,
           finished_at = CURRENT_TIMESTAMP
     WHERE run_id = p_run_id;

    RETURN TRUE;
END;
$finalize_run$;

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
AS $work_transition$
BEGIN
    IF NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
       OR NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.execution_scope IS DISTINCT FROM OLD.execution_scope
       OR NEW.data_domain IS DISTINCT FROM OLD.data_domain
       OR NEW.work_key IS DISTINCT FROM OLD.work_key
       OR NEW.input_artifact_id IS DISTINCT FROM OLD.input_artifact_id
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'ingestion work identity and lineage are immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'TERMINAL' THEN
        RAISE EXCEPTION 'terminal ingestion work is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'PENDING' AND NEW.state = 'PENDING'
       AND NEW.claim_generation <> OLD.claim_generation THEN
        RAISE EXCEPTION 'pending work cannot change claim generation without a claim'
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
       AND NEW.claim_generation = OLD.claim_generation
       AND NEW.claim_token IS DISTINCT FROM OLD.claim_token THEN
        RAISE EXCEPTION 'claim token is immutable within one ownership generation'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
       AND NEW.claim_generation = OLD.claim_generation + 1 THEN
        IF OLD.lease_until > CURRENT_TIMESTAMP THEN
            RAISE EXCEPTION 'active claim cannot be reclaimed before lease expiry'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.claim_token = OLD.claim_token THEN
            RAISE EXCEPTION 'reclaimed ownership requires a new claim token'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF OLD.state = 'CLAIMED' AND NEW.state IN ('PENDING', 'TERMINAL')
       AND NEW.claim_generation <> OLD.claim_generation THEN
        RAISE EXCEPTION 'terminal/retry transition must preserve claim generation'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state = 'TERMINAL' THEN
        IF NEW.outcome = 'SKIPPED' THEN
            IF EXISTS (
                SELECT 1 FROM ingestion_attempts
                 WHERE work_item_id = NEW.work_item_id
            ) THEN
                RAISE EXCEPTION 'SKIPPED work cannot have an execution attempt'
                    USING ERRCODE = '23514';
            END IF;
        ELSE
            IF NOT EXISTS (
                SELECT 1 FROM ingestion_attempts
                 WHERE work_item_id = NEW.work_item_id
                   AND attempt_number = OLD.claim_generation
                   AND state = 'TERMINAL'
                   AND outcome = NEW.outcome
            ) THEN
                RAISE EXCEPTION 'executed terminal work requires matching current terminal attempt'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    END IF;

    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$work_transition$;

DROP TRIGGER IF EXISTS ingestion_work_items_transition_guard ON ingestion_work_items;
CREATE TRIGGER ingestion_work_items_transition_guard
    BEFORE UPDATE ON ingestion_work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_work_transition();

CREATE OR REPLACE FUNCTION enforce_ingestion_attempt_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $attempt_transition$
BEGIN
    IF NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
       OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
       OR NEW.execution_scope IS DISTINCT FROM OLD.execution_scope
       OR NEW.data_domain IS DISTINCT FROM OLD.data_domain
       OR NEW.attempt_number IS DISTINCT FROM OLD.attempt_number
       OR NEW.started_at IS DISTINCT FROM OLD.started_at
    THEN
        RAISE EXCEPTION 'ingestion attempt identity and lineage are immutable'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.state = 'TERMINAL' THEN
        RAISE EXCEPTION 'terminal ingestion attempt is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.state <> OLD.state
       AND NOT (OLD.state = 'RUNNING' AND NEW.state = 'TERMINAL') THEN
        RAISE EXCEPTION 'invalid ingestion attempt transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$attempt_transition$;

DROP TRIGGER IF EXISTS ingestion_attempts_transition_guard ON ingestion_attempts;
CREATE TRIGGER ingestion_attempts_transition_guard
    BEFORE UPDATE ON ingestion_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_attempt_transition();


CREATE OR REPLACE FUNCTION enforce_source_artifact_forensic_immutability()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'source_artifacts is forensic evidence and cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.artifact_id IS DISTINCT FROM OLD.artifact_id
       OR NEW.data_domain IS DISTINCT FROM OLD.data_domain
       OR NEW.source_code IS DISTINCT FROM OLD.source_code
       OR NEW.artifact_contract_kind IS DISTINCT FROM OLD.artifact_contract_kind
       OR NEW.source_contract_version IS DISTINCT FROM OLD.source_contract_version
       OR NEW.locator_key IS DISTINCT FROM OLD.locator_key
       OR NEW.retrieval_url IS DISTINCT FROM OLD.retrieval_url
       OR NEW.content_sha256 IS DISTINCT FROM OLD.content_sha256
       OR NEW.content_type IS DISTINCT FROM OLD.content_type
       OR NEW.captured_at IS DISTINCT FROM OLD.captured_at
       OR NEW.created_by_attempt_id IS DISTINCT FROM OLD.created_by_attempt_id
       OR NEW.created_by_execution_scope IS DISTINCT FROM OLD.created_by_execution_scope
       OR NEW.storage_uri IS DISTINCT FROM OLD.storage_uri
       OR NEW.storage_generation IS DISTINCT FROM OLD.storage_generation
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'source artifact forensic metadata is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NOT (
        OLD.content_state = 'RETAINED'
        AND NEW.content_state = 'DELETED_BY_POLICY'
    ) THEN
        RAISE EXCEPTION 'only RETAINED -> DELETED_BY_POLICY is allowed for source artifacts'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS source_artifacts_forensic_immutable ON source_artifacts;
CREATE TRIGGER source_artifacts_forensic_immutable
    BEFORE UPDATE OR DELETE ON source_artifacts
    FOR EACH ROW EXECUTE FUNCTION enforce_source_artifact_forensic_immutability();
