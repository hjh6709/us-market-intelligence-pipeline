-- CPI W1 interpretation governance and economic serving-control history.
-- Additive above migration 012; production IAM/GRANT topology is intentionally deferred.

CREATE TABLE IF NOT EXISTS promotion_release_evidence_snapshots (
    evidence_snapshot_id UUID PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK (
        schema_version = 'cpi-w1-promotion-evidence-snapshot-v1'
    ),
    release_subject_digest TEXT NOT NULL CHECK (
        release_subject_digest ~ '^[0-9a-f]{64}$'
    ),
    corpus_snapshot_digest TEXT NOT NULL CHECK (
        corpus_snapshot_digest ~ '^[0-9a-f]{64}$'
    ),
    expected_diff_approvals_digest TEXT NOT NULL CHECK (
        expected_diff_approvals_digest ~ '^[0-9a-f]{64}$'
    ),
    replay_result_digest TEXT NOT NULL CHECK (
        replay_result_digest ~ '^[0-9a-f]{64}$'
    ),
    tested_job_contract_version TEXT NOT NULL CHECK (
        tested_job_contract_version ~ '^[a-z][a-z0-9-]*-v[1-9][0-9]*$'
    ),
    tested_source_revision TEXT NOT NULL CHECK (
        BTRIM(tested_source_revision) <> ''
        AND tested_source_revision = BTRIM(tested_source_revision)
    ),
    tested_workload_artifact_digest TEXT CHECK (
        tested_workload_artifact_digest IS NULL
        OR tested_workload_artifact_digest ~ '^[0-9a-f]{64}$'
    ),
    evidence_policy_version TEXT NOT NULL CHECK (
        evidence_policy_version = 'cpi-w1-evidence-v1'
    ),
    evidence_snapshot_digest TEXT NOT NULL UNIQUE CHECK (
        evidence_snapshot_digest ~ '^[0-9a-f]{64}$'
    ),
    created_by_subject TEXT NOT NULL CHECK (
        BTRIM(created_by_subject) <> ''
        AND created_by_subject = BTRIM(created_by_subject)
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT promotion_release_evidence_snapshot_exact_reference
        UNIQUE (
            evidence_snapshot_id, release_subject_digest,
            evidence_snapshot_digest
        )
);

DO $promotion_release_evidence_exact_reference$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'promotion_release_evidence_snapshots'::regclass
           AND conname = 'promotion_release_evidence_snapshot_exact_reference'
    ) THEN
        ALTER TABLE promotion_release_evidence_snapshots
            ADD CONSTRAINT promotion_release_evidence_snapshot_exact_reference
            UNIQUE (
                evidence_snapshot_id, release_subject_digest,
                evidence_snapshot_digest
            );
    END IF;
END;
$promotion_release_evidence_exact_reference$;

CREATE TABLE IF NOT EXISTS promotion_release_authorization_materials (
    authorization_material_id UUID PRIMARY KEY,
    release_subject_digest TEXT NOT NULL CHECK (
        release_subject_digest ~ '^[0-9a-f]{64}$'
    ),
    evidence_snapshot_id UUID NOT NULL,
    evidence_snapshot_digest TEXT NOT NULL CHECK (
        evidence_snapshot_digest ~ '^[0-9a-f]{64}$'
    ),
    gate_decision_digest TEXT NOT NULL CHECK (
        gate_decision_digest ~ '^[0-9a-f]{64}$'
    ),
    gate_policy_version TEXT NOT NULL CHECK (
        gate_policy_version = 'cpi-w1-gate-v2'
    ),
    authorization_policy_version TEXT NOT NULL CHECK (
        authorization_policy_version = 'cpi-w1-authorization-v1'
    ),
    executor_source_revision TEXT NOT NULL CHECK (
        BTRIM(executor_source_revision) <> ''
        AND executor_source_revision = BTRIM(executor_source_revision)
    ),
    executor_workload_artifact_digest TEXT NOT NULL CHECK (
        executor_workload_artifact_digest ~ '^[0-9a-f]{64}$'
    ),
    executor_job_contract_version TEXT NOT NULL CHECK (
        executor_job_contract_version ~ '^[a-z][a-z0-9-]*-v[1-9][0-9]*$'
    ),
    review_ref TEXT NOT NULL CHECK (
        BTRIM(review_ref) <> '' AND review_ref = BTRIM(review_ref)
    ),
    review_digest TEXT NOT NULL CHECK (review_digest ~ '^[0-9a-f]{64}$'),
    authorization_material_digest TEXT NOT NULL UNIQUE CHECK (
        authorization_material_digest ~ '^[0-9a-f]{64}$'
    ),
    created_by_subject TEXT NOT NULL CHECK (
        BTRIM(created_by_subject) <> ''
        AND created_by_subject = BTRIM(created_by_subject)
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT promotion_release_authorization_exact_evidence_fk
        FOREIGN KEY (evidence_snapshot_id, release_subject_digest, evidence_snapshot_digest)
        REFERENCES promotion_release_evidence_snapshots (
            evidence_snapshot_id, release_subject_digest,
            evidence_snapshot_digest
        ),
    CONSTRAINT promotion_release_authorization_material_subject_reference
        UNIQUE (authorization_material_id, release_subject_digest)
);

CREATE TABLE IF NOT EXISTS promotion_release_authorizations (
    authorization_id UUID PRIMARY KEY,
    authorization_material_id UUID NOT NULL,
    release_subject_digest TEXT NOT NULL CHECK (
        release_subject_digest ~ '^[0-9a-f]{64}$'
    ),
    grant_reason_code TEXT NOT NULL CHECK (
        grant_reason_code ~ '^[A-Z][A-Z0-9_]*$'
    ),
    created_by_subject TEXT NOT NULL CHECK (
        BTRIM(created_by_subject) <> ''
        AND created_by_subject = BTRIM(created_by_subject)
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT promotion_release_authorization_material_fk
        FOREIGN KEY (authorization_material_id, release_subject_digest)
        REFERENCES promotion_release_authorization_materials (
            authorization_material_id, release_subject_digest
        )
);

CREATE TABLE IF NOT EXISTS promotion_release_control_decisions (
    control_decision_id UUID PRIMARY KEY,
    authorization_id UUID NOT NULL
        REFERENCES promotion_release_authorizations(authorization_id),
    expected_control_version INTEGER NOT NULL CHECK (
        expected_control_version >= 0
    ),
    control_version INTEGER NOT NULL CHECK (control_version >= 1),
    state TEXT NOT NULL CHECK (state IN ('APPROVED', 'REVOKED')),
    reason_code TEXT NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]*$'),
    actor_subject TEXT NOT NULL CHECK (
        BTRIM(actor_subject) <> '' AND actor_subject = BTRIM(actor_subject)
    ),
    review_ref TEXT NOT NULL CHECK (
        BTRIM(review_ref) <> '' AND review_ref = BTRIM(review_ref)
    ),
    review_digest TEXT NOT NULL CHECK (review_digest ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT promotion_release_control_version_increment CHECK (
        control_version = expected_control_version + 1
    ),
    CONSTRAINT promotion_release_control_version_identity
        UNIQUE (authorization_id, control_version)
);

CREATE OR REPLACE FUNCTION enforce_promotion_release_control_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $promotion_release_control_transition$
DECLARE
    current_version INTEGER := 0;
    current_state TEXT;
BEGIN
    PERFORM 1
      FROM promotion_release_authorizations
     WHERE authorization_id = NEW.authorization_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'promotion authorization does not exist'
            USING ERRCODE = '23503';
    END IF;

    SELECT control_version, state
      INTO current_version, current_state
      FROM promotion_release_control_decisions
     WHERE authorization_id = NEW.authorization_id
     ORDER BY control_version DESC
     LIMIT 1;
    IF NOT FOUND THEN
        current_version := 0;
        current_state := NULL;
    END IF;

    IF NEW.expected_control_version <> current_version
       OR NEW.control_version <> current_version + 1 THEN
        RAISE EXCEPTION 'promotion release control expected version mismatch'
            USING ERRCODE = '40001';
    END IF;
    IF current_state IS NULL AND NEW.state <> 'APPROVED' THEN
        RAISE EXCEPTION 'initial promotion authorization control must be APPROVED'
            USING ERRCODE = '23514';
    END IF;
    IF current_state = 'APPROVED' AND NEW.state <> 'REVOKED' THEN
        RAISE EXCEPTION 'approved promotion authorization only permits revocation'
            USING ERRCODE = '23514';
    END IF;
    IF current_state = 'REVOKED' THEN
        RAISE EXCEPTION 'revoked promotion authorization cannot be reactivated'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$promotion_release_control_transition$;

DROP TRIGGER IF EXISTS promotion_release_control_transition_guard
    ON promotion_release_control_decisions;
CREATE TRIGGER promotion_release_control_transition_guard
    BEFORE INSERT ON promotion_release_control_decisions
    FOR EACH ROW EXECUTE FUNCTION enforce_promotion_release_control_transition();

CREATE OR REPLACE FUNCTION apply_promotion_release_control(
    p_authorization_id UUID,
    p_expected_control_version INTEGER,
    p_state TEXT,
    p_reason_code TEXT,
    p_actor_subject TEXT,
    p_review_ref TEXT,
    p_review_digest TEXT
)
RETURNS UUID
LANGUAGE plpgsql
AS $apply_promotion_release_control$
DECLARE
    decision_id UUID := gen_random_uuid();
BEGIN
    INSERT INTO promotion_release_control_decisions (
        control_decision_id, authorization_id, expected_control_version,
        control_version, state, reason_code, actor_subject, review_ref,
        review_digest
    ) VALUES (
        decision_id, p_authorization_id, p_expected_control_version,
        p_expected_control_version + 1, p_state, p_reason_code,
        p_actor_subject, p_review_ref, p_review_digest
    );
    RETURN decision_id;
END;
$apply_promotion_release_control$;

CREATE OR REPLACE FUNCTION resolve_promotion_release_authorization(
    p_release_subject_digest TEXT,
    p_executor_source_revision TEXT,
    p_executor_workload_artifact_digest TEXT,
    p_executor_job_contract_version TEXT
)
RETURNS UUID
LANGUAGE plpgsql
STABLE
AS $resolve_promotion_release_authorization$
DECLARE
    matches UUID[];
BEGIN
    SELECT ARRAY_AGG(a.authorization_id ORDER BY a.authorization_id)
      INTO matches
      FROM promotion_release_authorizations a
      JOIN promotion_release_authorization_materials m
        ON m.authorization_material_id = a.authorization_material_id
      JOIN LATERAL (
            SELECT state
              FROM promotion_release_control_decisions c
             WHERE c.authorization_id = a.authorization_id
             ORDER BY c.control_version DESC
             LIMIT 1
      ) effective ON effective.state = 'APPROVED'
     WHERE a.release_subject_digest = p_release_subject_digest
       AND m.executor_source_revision = p_executor_source_revision
       AND m.executor_workload_artifact_digest = p_executor_workload_artifact_digest
       AND m.executor_job_contract_version = p_executor_job_contract_version;

    IF COALESCE(CARDINALITY(matches), 0) = 0 THEN
        RETURN NULL;
    END IF;
    IF CARDINALITY(matches) > 1 THEN
        RAISE EXCEPTION 'ambiguous active promotion authorization'
            USING ERRCODE = '23514';
    END IF;
    RETURN matches[1];
END;
$resolve_promotion_release_authorization$;

ALTER TABLE ingestion_attempts
    ADD COLUMN IF NOT EXISTS release_authorization_id UUID,
    ADD COLUMN IF NOT EXISTS release_control_decision_id UUID,
    ADD COLUMN IF NOT EXISTS executor_source_revision TEXT,
    ADD COLUMN IF NOT EXISTS executor_workload_artifact_digest TEXT,
    ADD COLUMN IF NOT EXISTS executor_job_contract_version TEXT;

DO $attempt_authorization_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'promotion_release_control_decisions'::regclass
           AND conname = 'promotion_release_control_decision_authorization_reference'
    ) THEN
        ALTER TABLE promotion_release_control_decisions
            ADD CONSTRAINT promotion_release_control_decision_authorization_reference
            UNIQUE (control_decision_id, authorization_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'ingestion_attempts'::regclass
           AND conname = 'ingestion_attempts_executor_provenance_valid'
    ) THEN
        ALTER TABLE ingestion_attempts
            ADD CONSTRAINT ingestion_attempts_executor_provenance_valid CHECK (
                BTRIM(executor_source_revision) <> ''
                AND executor_source_revision = BTRIM(executor_source_revision)
                AND executor_workload_artifact_digest ~ '^[0-9a-f]{64}$'
                AND executor_job_contract_version ~ '^[a-z][a-z0-9-]*-v[1-9][0-9]*$'
            ) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'ingestion_attempts'::regclass
           AND conname = 'ingestion_attempts_authorization_scope_valid'
    ) THEN
        ALTER TABLE ingestion_attempts
            ADD CONSTRAINT ingestion_attempts_authorization_scope_valid CHECK (
                (
                    execution_scope = 'ECONOMIC_COLLECT'
                    AND release_authorization_id IS NULL
                    AND release_control_decision_id IS NULL
                )
                OR (
                    execution_scope = 'ECONOMIC_PROMOTE'
                    AND release_authorization_id IS NOT NULL
                    AND release_control_decision_id IS NOT NULL
                )
            ) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'ingestion_attempts'::regclass
           AND conname = 'ingestion_attempts_release_authorization_fk'
    ) THEN
        ALTER TABLE ingestion_attempts
            ADD CONSTRAINT ingestion_attempts_release_authorization_fk
            FOREIGN KEY (release_authorization_id)
            REFERENCES promotion_release_authorizations(authorization_id)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'ingestion_attempts'::regclass
           AND conname = 'ingestion_attempts_release_control_decision_fk'
    ) THEN
        ALTER TABLE ingestion_attempts
            ADD CONSTRAINT ingestion_attempts_release_control_decision_fk
            FOREIGN KEY (release_control_decision_id, release_authorization_id)
            REFERENCES promotion_release_control_decisions(
                control_decision_id, authorization_id
            ) NOT VALID;
    END IF;
END;
$attempt_authorization_constraints$;

CREATE OR REPLACE FUNCTION enforce_cpi_attempt_release_authorization()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $attempt_release_authorization$
DECLARE
    work_subject_digest TEXT;
    authorization_subject_digest TEXT;
    authorized_source_revision TEXT;
    authorized_workload_digest TEXT;
    authorized_job_contract_version TEXT;
    effective_control_decision_id UUID;
    effective_control_state TEXT;
BEGIN
    IF NEW.execution_scope = 'ECONOMIC_COLLECT' THEN
        IF NEW.release_authorization_id IS NOT NULL
           OR NEW.release_control_decision_id IS NOT NULL THEN
            RAISE EXCEPTION 'collect attempt cannot bind release authorization'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.release_authorization_id IS NULL
       OR NEW.release_control_decision_id IS NULL THEN
        RAISE EXCEPTION 'promotion attempt requires exact release authorization'
            USING ERRCODE = '23514';
    END IF;

    SELECT release_subject_digest
      INTO STRICT work_subject_digest
      FROM ingestion_work_items
     WHERE work_item_id = NEW.work_item_id;

    SELECT a.release_subject_digest,
           m.executor_source_revision,
           m.executor_workload_artifact_digest,
           m.executor_job_contract_version
      INTO authorization_subject_digest,
           authorized_source_revision,
           authorized_workload_digest,
           authorized_job_contract_version
      FROM promotion_release_authorizations a
      JOIN promotion_release_authorization_materials m
        ON m.authorization_material_id = a.authorization_material_id
     WHERE a.authorization_id = NEW.release_authorization_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'promotion attempt requires exact release authorization'
            USING ERRCODE = '23514';
    END IF;
    IF authorization_subject_digest IS DISTINCT FROM work_subject_digest THEN
        RAISE EXCEPTION 'attempt release authorization subject mismatch'
            USING ERRCODE = '23514';
    END IF;
    IF authorized_source_revision IS DISTINCT FROM NEW.executor_source_revision
       OR authorized_workload_digest IS DISTINCT FROM NEW.executor_workload_artifact_digest
       OR authorized_job_contract_version IS DISTINCT FROM NEW.executor_job_contract_version THEN
        RAISE EXCEPTION 'attempt executor provenance does not match authorization'
            USING ERRCODE = '23514';
    END IF;

    SELECT control_decision_id, state
      INTO effective_control_decision_id, effective_control_state
      FROM promotion_release_control_decisions
     WHERE authorization_id = NEW.release_authorization_id
     ORDER BY control_version DESC
     LIMIT 1;

    IF effective_control_state IS DISTINCT FROM 'APPROVED'
       OR effective_control_decision_id IS DISTINCT FROM NEW.release_control_decision_id THEN
        RAISE EXCEPTION 'attempt release authorization is not approved'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$attempt_release_authorization$;

DROP TRIGGER IF EXISTS ingestion_attempts_release_authorization_guard
    ON ingestion_attempts;
CREATE TRIGGER ingestion_attempts_release_authorization_guard
    BEFORE INSERT ON ingestion_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_attempt_release_authorization();

CREATE TABLE IF NOT EXISTS interpretation_requests (
    request_id UUID PRIMARY KEY,
    subject_id UUID NOT NULL REFERENCES interpretation_subjects(subject_id),
    requested_state TEXT NOT NULL CHECK (requested_state IN ('VALID', 'INVALID')),
    expected_decision_version INTEGER NOT NULL CHECK (expected_decision_version >= 0),
    reason_code TEXT NOT NULL CHECK (
        BTRIM(reason_code) <> '' AND reason_code = BTRIM(reason_code)
    ),
    case_ref TEXT,
    governance_policy_version TEXT NOT NULL CHECK (
        governance_policy_version = 'cpi-governance-v1'
    ),
    proposer_subject TEXT NOT NULL CHECK (
        BTRIM(proposer_subject) <> '' AND proposer_subject = BTRIM(proposer_subject)
    ),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT interpretation_requests_proposer_reference
        UNIQUE (request_id, proposer_subject),
    CONSTRAINT interpretation_requests_subject_reference
        UNIQUE (request_id, subject_id),
    CONSTRAINT interpretation_requests_state_reference
        UNIQUE (request_id, subject_id, requested_state)
);

CREATE OR REPLACE FUNCTION set_cpi_governance_request_expiry()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.governance_policy_version <> 'cpi-governance-v1' THEN
        RAISE EXCEPTION 'unsupported CPI governance policy'
            USING ERRCODE = '23514';
    END IF;
    NEW.expires_at := NEW.requested_at + interval '24 hours';
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS interpretation_requests_expiry_guard
    ON interpretation_requests;
CREATE TRIGGER interpretation_requests_expiry_guard
    BEFORE INSERT ON interpretation_requests
    FOR EACH ROW EXECUTE FUNCTION set_cpi_governance_request_expiry();

CREATE TABLE IF NOT EXISTS interpretation_approvals (
    approval_id UUID PRIMARY KEY,
    request_id UUID NOT NULL,
    proposer_subject TEXT NOT NULL,
    approver_subject TEXT NOT NULL CHECK (
        BTRIM(approver_subject) <> '' AND approver_subject = BTRIM(approver_subject)
    ),
    approval_decision TEXT NOT NULL CHECK (
        approval_decision IN ('APPROVE', 'REJECT')
    ),
    decided_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT interpretation_approvals_request_proposer_fk
        FOREIGN KEY (request_id, proposer_subject)
        REFERENCES interpretation_requests(request_id, proposer_subject),
    CONSTRAINT interpretation_approvals_independent_actor
        CHECK (approver_subject <> proposer_subject),
    CONSTRAINT interpretation_approvals_one_vote
        UNIQUE (request_id, approver_subject)
);

CREATE TABLE IF NOT EXISTS interpretation_decisions (
    interpretation_decision_id UUID PRIMARY KEY,
    subject_id UUID NOT NULL REFERENCES interpretation_subjects(subject_id),
    decision_version INTEGER NOT NULL CHECK (decision_version >= 1),
    decision_state TEXT NOT NULL CHECK (decision_state IN ('VALID', 'INVALID')),
    request_id UUID NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT interpretation_decisions_request_state_fk
        FOREIGN KEY (request_id, subject_id, decision_state)
        REFERENCES interpretation_requests(request_id, subject_id, requested_state),
    CONSTRAINT interpretation_decisions_subject_version
        UNIQUE (subject_id, decision_version)
);

CREATE TABLE IF NOT EXISTS economic_serving_control_decisions (
    control_decision_id UUID PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN ('CPI_DOMAIN', 'EVENT_OCCURRENCE')
    ),
    event_occurrence_id UUID REFERENCES core_event_occurrences(event_occurrence_id),
    expected_control_version INTEGER NOT NULL CHECK (expected_control_version >= 0),
    control_version INTEGER NOT NULL CHECK (control_version >= 1),
    state TEXT NOT NULL CHECK (state IN ('ENABLED', 'WITHHELD')),
    reason_code TEXT NOT NULL CHECK (
        BTRIM(reason_code) <> '' AND reason_code = BTRIM(reason_code)
    ),
    applied_at TIMESTAMPTZ NOT NULL,
    actor_subject TEXT NOT NULL CHECK (
        BTRIM(actor_subject) <> '' AND actor_subject = BTRIM(actor_subject)
    ),
    case_ref TEXT,
    verified_knowledge_fingerprint TEXT CHECK (
        verified_knowledge_fingerprint IS NULL
        OR verified_knowledge_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT economic_serving_control_scope_valid CHECK (
        (scope_kind = 'CPI_DOMAIN' AND event_occurrence_id IS NULL)
        OR
        (scope_kind = 'EVENT_OCCURRENCE' AND event_occurrence_id IS NOT NULL)
    ),
    CONSTRAINT economic_serving_control_version_increment CHECK (
        control_version = expected_control_version + 1
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS economic_serving_control_domain_version
    ON economic_serving_control_decisions (scope_kind, control_version)
    WHERE scope_kind = 'CPI_DOMAIN';

CREATE UNIQUE INDEX IF NOT EXISTS economic_serving_control_event_version
    ON economic_serving_control_decisions (event_occurrence_id, control_version)
    WHERE scope_kind = 'EVENT_OCCURRENCE';

CREATE TABLE IF NOT EXISTS business_audit_events (
    audit_event_id UUID PRIMARY KEY,
    action_kind TEXT NOT NULL CHECK (
        action_kind IN (
            'INTERPRETATION_DECISION_APPLIED',
            'ECONOMIC_SERVING_CONTROL_APPLIED',
            'INGESTION_WORK_PAUSED',
            'INGESTION_WORK_RESUMED',
            'CPI_PROMOTION_DEFERRED'
        )
    ),
    actor_subject TEXT NOT NULL CHECK (
        BTRIM(actor_subject) <> '' AND actor_subject = BTRIM(actor_subject)
    ),
    interpretation_decision_id UUID
        REFERENCES interpretation_decisions(interpretation_decision_id),
    control_decision_id UUID
        REFERENCES economic_serving_control_decisions(control_decision_id),
    work_item_id UUID REFERENCES ingestion_work_items(work_item_id),
    source_artifact_id UUID REFERENCES source_artifacts(artifact_id),
    case_ref TEXT,
    verification_ref TEXT,
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT business_audit_target_valid CHECK (
        (
            action_kind = 'INTERPRETATION_DECISION_APPLIED'
            AND interpretation_decision_id IS NOT NULL
            AND control_decision_id IS NULL
            AND work_item_id IS NULL
            AND source_artifact_id IS NULL
        )
        OR
        (
            action_kind = 'ECONOMIC_SERVING_CONTROL_APPLIED'
            AND interpretation_decision_id IS NULL
            AND control_decision_id IS NOT NULL
            AND work_item_id IS NULL
            AND source_artifact_id IS NULL
        )
        OR
        (
            action_kind IN ('INGESTION_WORK_PAUSED', 'INGESTION_WORK_RESUMED')
            AND interpretation_decision_id IS NULL
            AND control_decision_id IS NULL
            AND work_item_id IS NOT NULL
            AND source_artifact_id IS NULL
        )
        OR
        (
            action_kind = 'CPI_PROMOTION_DEFERRED'
            AND interpretation_decision_id IS NULL
            AND control_decision_id IS NULL
            AND work_item_id IS NULL
            AND source_artifact_id IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS business_audit_cpi_promotion_deferred_identity
    ON business_audit_events (
        source_artifact_id,
        verification_ref,
        ((event_payload ->> 'gate_fingerprint'))
    )
    WHERE action_kind = 'CPI_PROMOTION_DEFERRED';

CREATE OR REPLACE FUNCTION record_cpi_promotion_deferred(
    p_source_artifact_id UUID,
    p_extractor_contract_version TEXT,
    p_reason_code TEXT,
    p_review_ref TEXT,
    p_gate_fingerprint TEXT
)
RETURNS UUID
LANGUAGE plpgsql
AS $promotion_deferred$
DECLARE
    event_id UUID;
BEGIN
    IF p_extractor_contract_version IS NULL
       OR BTRIM(p_extractor_contract_version) = '' THEN
        RAISE EXCEPTION 'deferred promotion extractor is required'
            USING ERRCODE = '23514';
    END IF;
    IF p_reason_code IS NULL OR p_reason_code !~ '^[A-Z][A-Z0-9_]*$' THEN
        RAISE EXCEPTION 'deferred promotion reason must be canonical'
            USING ERRCODE = '23514';
    END IF;
    IF p_review_ref IS NULL OR BTRIM(p_review_ref) = '' THEN
        RAISE EXCEPTION 'deferred promotion review reference is required'
            USING ERRCODE = '23514';
    END IF;
    IF p_gate_fingerprint IS NULL
       OR p_gate_fingerprint !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'deferred promotion gate fingerprint must be lowercase SHA-256'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, source_artifact_id,
        verification_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'CPI_PROMOTION_DEFERRED', 'system:cpi-orchestrator',
        p_source_artifact_id, p_extractor_contract_version,
        jsonb_build_object(
            'reason_code', p_reason_code,
            'review_ref', p_review_ref,
            'extractor_contract_version', p_extractor_contract_version,
            'gate_fingerprint', p_gate_fingerprint
        ),
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (
        source_artifact_id,
        verification_ref,
        ((event_payload ->> 'gate_fingerprint'))
    )
        WHERE action_kind = 'CPI_PROMOTION_DEFERRED'
    DO NOTHING
    RETURNING audit_event_id INTO event_id;

    IF event_id IS NULL THEN
        SELECT audit_event_id
          INTO STRICT event_id
          FROM business_audit_events
         WHERE action_kind = 'CPI_PROMOTION_DEFERRED'
           AND source_artifact_id = p_source_artifact_id
           AND verification_ref = p_extractor_contract_version
           AND event_payload ->> 'gate_fingerprint' = p_gate_fingerprint;
    END IF;

    RETURN event_id;
END;
$promotion_deferred$;

CREATE OR REPLACE FUNCTION pause_pending_cpi_ingestion_work(
    p_work_item_id UUID,
    p_work_reason_code TEXT,
    p_actor_subject TEXT,
    p_case_ref TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $pause_pending_work$
DECLARE
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
    paused_generation INTEGER;
BEGIN
    IF p_work_reason_code IS NULL
       OR p_work_reason_code !~ '^[A-Z][A-Z0-9_]*$' THEN
        RAISE EXCEPTION 'pause reason must be canonical' USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = ''
       OR p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'pause actor must be canonical' USING ERRCODE = '23514';
    END IF;

    UPDATE ingestion_work_items
       SET state='PAUSED', outcome=NULL, reason_code=p_work_reason_code,
           claim_token=NULL, lease_until=NULL, next_claim_at=NULL
     WHERE work_item_id=p_work_item_id
       AND state='PENDING'
     RETURNING claim_generation INTO paused_generation;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'admission pause requires pending work'
            USING ERRCODE = '40001';
    END IF;

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, work_item_id,
        case_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'INGESTION_WORK_PAUSED', p_actor_subject,
        p_work_item_id, p_case_ref,
        jsonb_build_object(
            'source_state', 'PENDING',
            'claim_generation', paused_generation,
            'work_reason_code', p_work_reason_code
        ),
        applied_time
    );
END;
$pause_pending_work$;

CREATE OR REPLACE FUNCTION pause_reclaimed_cpi_ingestion_work(
    p_work_item_id UUID,
    p_attempt_id UUID,
    p_claim_generation INTEGER,
    p_claim_token UUID,
    p_work_reason_code TEXT,
    p_actor_subject TEXT,
    p_case_ref TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $pause_reclaimed_work$
DECLARE
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
    IF p_work_reason_code IS NULL
       OR p_work_reason_code !~ '^[A-Z][A-Z0-9_]*$' THEN
        RAISE EXCEPTION 'work pause reason must be canonical'
            USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = ''
       OR p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'pause actor must be canonical' USING ERRCODE = '23514';
    END IF;

    PERFORM 1
      FROM ingestion_work_items w
     WHERE w.work_item_id=p_work_item_id
       AND w.state='CLAIMED'
       AND w.claim_generation=p_claim_generation
       AND w.claim_token=p_claim_token
       AND w.lease_until <= CURRENT_TIMESTAMP;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reclaim pause requires expired current ownership'
            USING ERRCODE = '40001';
    END IF;

    PERFORM 1
      FROM ingestion_attempts a
     WHERE a.attempt_id=p_attempt_id
       AND a.work_item_id=p_work_item_id
       AND a.attempt_number=p_claim_generation
       AND a.state='TERMINAL'
       AND a.outcome='FAILED'
       AND a.reason_code='LEASE_EXPIRED_RECLAIM';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reclaim pause requires closed expired attempt'
            USING ERRCODE = '40001';
    END IF;

    UPDATE ingestion_work_items
       SET state='PAUSED', outcome=NULL, reason_code=p_work_reason_code,
           claim_token=NULL, lease_until=NULL, next_claim_at=NULL
     WHERE work_item_id=p_work_item_id
       AND state='CLAIMED'
       AND claim_generation=p_claim_generation
       AND claim_token=p_claim_token;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reclaim pause lost current work ownership'
            USING ERRCODE = '40001';
    END IF;

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, work_item_id,
        case_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'INGESTION_WORK_PAUSED', p_actor_subject,
        p_work_item_id, p_case_ref,
        jsonb_build_object(
            'source_state', 'CLAIMED',
            'claim_generation', p_claim_generation,
            'attempt_reason_code', 'LEASE_EXPIRED_RECLAIM',
            'work_reason_code', p_work_reason_code,
            'attempt_id', p_attempt_id
        ),
        applied_time
    );
END;
$pause_reclaimed_work$;

DROP FUNCTION IF EXISTS pause_cpi_ingestion_work(
    UUID, UUID, INTEGER, UUID, TEXT, TEXT, TEXT
);

CREATE OR REPLACE FUNCTION pause_cpi_ingestion_work(
    p_work_item_id UUID,
    p_attempt_id UUID,
    p_claim_generation INTEGER,
    p_claim_token UUID,
    p_attempt_reason_code TEXT,
    p_work_reason_code TEXT,
    p_actor_subject TEXT,
    p_case_ref TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $pause_work$
DECLARE
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
    IF p_attempt_reason_code IS NULL
       OR p_attempt_reason_code !~ '^[A-Z][A-Z0-9_]*$' THEN
        RAISE EXCEPTION 'attempt pause reason must be canonical'
            USING ERRCODE = '23514';
    END IF;
    IF p_work_reason_code IS NULL
       OR p_work_reason_code !~ '^[A-Z][A-Z0-9_]*$' THEN
        RAISE EXCEPTION 'work pause reason must be canonical'
            USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = ''
       OR p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'pause actor must be canonical' USING ERRCODE = '23514';
    END IF;

    UPDATE ingestion_attempts a
       SET state='TERMINAL', outcome='FAILED', reason_code=p_attempt_reason_code,
           finished_at=applied_time
      FROM ingestion_work_items w
     WHERE a.attempt_id=p_attempt_id
       AND a.work_item_id=w.work_item_id
       AND w.work_item_id=p_work_item_id
       AND w.state='CLAIMED'
       AND w.claim_generation=p_claim_generation
       AND w.claim_token=p_claim_token
       AND w.lease_until > CURRENT_TIMESTAMP
       AND a.state='RUNNING'
       AND a.attempt_number=w.claim_generation;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pause requires current claim ownership' USING ERRCODE = '40001';
    END IF;

    UPDATE ingestion_work_items
       SET state='PAUSED', outcome=NULL, reason_code=p_work_reason_code,
           claim_token=NULL, lease_until=NULL, next_claim_at=NULL
     WHERE work_item_id=p_work_item_id
       AND state='CLAIMED'
       AND claim_generation=p_claim_generation
       AND claim_token=p_claim_token;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pause lost current work ownership' USING ERRCODE = '40001';
    END IF;

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, work_item_id,
        case_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'INGESTION_WORK_PAUSED', p_actor_subject,
        p_work_item_id, p_case_ref,
        jsonb_build_object(
            'source_state', 'CLAIMED',
            'claim_generation', p_claim_generation,
            'attempt_reason_code', p_attempt_reason_code,
            'work_reason_code', p_work_reason_code,
            'attempt_id', p_attempt_id
        ),
        applied_time
    );
END;
$pause_work$;

CREATE OR REPLACE FUNCTION resume_cpi_ingestion_work(
    p_work_item_id UUID,
    p_actor_subject TEXT,
    p_case_ref TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $resume_work$
DECLARE
    prior_reason TEXT;
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = ''
       OR p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'resume actor must be canonical' USING ERRCODE = '23514';
    END IF;
    IF p_case_ref IS NULL OR BTRIM(p_case_ref) = ''
       OR p_case_ref <> BTRIM(p_case_ref) THEN
        RAISE EXCEPTION 'resume requires canonical case reference' USING ERRCODE = '23514';
    END IF;

    SELECT reason_code INTO STRICT prior_reason
      FROM ingestion_work_items
     WHERE work_item_id=p_work_item_id AND state='PAUSED'
     FOR UPDATE;

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, work_item_id,
        case_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'INGESTION_WORK_RESUMED', p_actor_subject,
        p_work_item_id, p_case_ref,
        jsonb_build_object('prior_pause_reason', prior_reason),
        applied_time
    );

    UPDATE ingestion_work_items
       SET state='PENDING', outcome=NULL, reason_code=NULL,
           claim_token=NULL, lease_until=NULL, next_claim_at=applied_time
     WHERE work_item_id=p_work_item_id AND state='PAUSED';
END;
$resume_work$;

CREATE OR REPLACE FUNCTION lock_cpi_governance_subject_events(
    p_subject_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    affected_event UUID;
BEGIN
    FOR affected_event IN
        SELECT DISTINCT event_occurrence_id
          FROM (
                SELECT s.event_occurrence_id
                  FROM event_schedule_assertions s
                 WHERE s.schedule_assertion_id = p_subject_id
                UNION
                SELECT l.event_occurrence_id
                  FROM event_disclosure_links l
                 WHERE l.disclosure_link_id = p_subject_id
                UNION
                SELECT l.event_occurrence_id
                  FROM event_disclosure_artifacts a
                  JOIN event_disclosure_links l
                    ON l.disclosure_id = a.disclosure_id
                 WHERE a.disclosure_artifact_link_id = p_subject_id
                UNION
                SELECT l.event_occurrence_id
                  FROM disclosure_marker_assertions m
                  JOIN event_disclosure_artifacts a
                    ON a.disclosure_artifact_link_id =
                       m.disclosure_artifact_link_id
                  JOIN event_disclosure_links l
                    ON l.disclosure_id = a.disclosure_id
                 WHERE m.marker_assertion_id = p_subject_id
                UNION
                SELECT o.event_occurrence_id
                  FROM official_observation_assertions o
                 WHERE o.assertion_id = p_subject_id
          ) affected
         ORDER BY event_occurrence_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('CPI_EVENT:' || affected_event::TEXT, 0)
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION apply_interpretation_decision(
    p_request_id UUID,
    p_actor_subject TEXT
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    req interpretation_requests%ROWTYPE;
    current_version INTEGER := 0;
    current_state TEXT := 'VALID';
    approve_count INTEGER := 0;
    reject_count INTEGER := 0;
    decision_id UUID := gen_random_uuid();
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = '' THEN
        RAISE EXCEPTION 'activation actor is required'
            USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'activation actor must be canonical'
            USING ERRCODE = '23514';
    END IF;

    SELECT * INTO STRICT req
      FROM interpretation_requests
     WHERE request_id = p_request_id;

    PERFORM lock_cpi_governance_subject_events(req.subject_id);

    PERFORM 1
      FROM interpretation_subjects
     WHERE subject_id = req.subject_id
     FOR UPDATE;

    IF NOT EXISTS (
        SELECT 1
          FROM interpretation_subjects s
         WHERE s.subject_id = req.subject_id
           AND (
               (s.subject_type = 'SCHEDULE_ASSERTION'
                    AND EXISTS (
                        SELECT 1 FROM event_schedule_assertions e
                         WHERE e.schedule_assertion_id = s.subject_id
                    ))
               OR
               (s.subject_type = 'EVENT_DISCLOSURE_LINK'
                    AND EXISTS (
                        SELECT 1 FROM event_disclosure_links e
                         WHERE e.disclosure_link_id = s.subject_id
                    ))
               OR
               (s.subject_type = 'DISCLOSURE_ARTIFACT_LINK'
                    AND EXISTS (
                        SELECT 1 FROM event_disclosure_artifacts e
                         WHERE e.disclosure_artifact_link_id = s.subject_id
                    ))
               OR
               (s.subject_type = 'DISCLOSURE_MARKER_ASSERTION'
                    AND EXISTS (
                        SELECT 1 FROM disclosure_marker_assertions e
                         WHERE e.marker_assertion_id = s.subject_id
                    ))
               OR
               (s.subject_type = 'OFFICIAL_OBSERVATION_ASSERTION'
                    AND EXISTS (
                        SELECT 1 FROM official_observation_assertions e
                         WHERE e.assertion_id = s.subject_id
                    ))
           )
    ) THEN
        RAISE EXCEPTION 'interpretation subject has no matching typed evidence'
            USING ERRCODE = '23514';
    END IF;

    IF CURRENT_TIMESTAMP < req.requested_at THEN
        RAISE EXCEPTION 'interpretation request is not active yet'
            USING ERRCODE = '23514';
    END IF;

    IF CURRENT_TIMESTAMP >= req.expires_at THEN
        RAISE EXCEPTION 'interpretation request expired'
            USING ERRCODE = '23514';
    END IF;

    SELECT decision_version, decision_state
      INTO current_version, current_state
      FROM interpretation_decisions
     WHERE subject_id = req.subject_id
     ORDER BY decision_version DESC
     LIMIT 1;

    IF NOT FOUND THEN
        current_version := 0;
        current_state := 'VALID';
    END IF;

    IF req.expected_decision_version <> current_version THEN
        RAISE EXCEPTION 'interpretation decision version conflict'
            USING ERRCODE = '40001';
    END IF;

    IF req.requested_state = current_state THEN
        RAISE EXCEPTION 'interpretation decision would be a no-op'
            USING ERRCODE = '23514';
    END IF;

    SELECT
        COUNT(*) FILTER (WHERE approval_decision = 'APPROVE'),
        COUNT(*) FILTER (WHERE approval_decision = 'REJECT')
      INTO approve_count, reject_count
      FROM interpretation_approvals
     WHERE request_id = req.request_id;

    IF approve_count < 1 OR reject_count <> 0 THEN
        RAISE EXCEPTION 'governance approval requirement not satisfied'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO interpretation_decisions (
        interpretation_decision_id, subject_id, decision_version,
        decision_state, request_id, applied_at
    ) VALUES (
        decision_id, req.subject_id, current_version + 1,
        req.requested_state, req.request_id, applied_time
    );

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject,
        interpretation_decision_id, case_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'INTERPRETATION_DECISION_APPLIED', p_actor_subject,
        decision_id, req.case_ref,
        jsonb_build_object(
            'request_id', req.request_id,
            'subject_id', req.subject_id,
            'decision_version', current_version + 1,
            'decision_state', req.requested_state,
            'governance_policy_version', req.governance_policy_version
        ),
        applied_time
    );

    RETURN decision_id;
END;
$$;

CREATE OR REPLACE FUNCTION apply_economic_serving_control(
    p_scope_kind TEXT,
    p_event_occurrence_id UUID,
    p_expected_control_version INTEGER,
    p_state TEXT,
    p_reason_code TEXT,
    p_actor_subject TEXT,
    p_case_ref TEXT DEFAULT NULL,
    p_verified_knowledge_fingerprint TEXT DEFAULT NULL,
    p_verification_ref TEXT DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    current_version INTEGER := 0;
    current_state TEXT := 'ENABLED';
    decision_id UUID := gen_random_uuid();
    applied_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
    scope_key TEXT;
BEGIN
    IF p_scope_kind NOT IN ('CPI_DOMAIN', 'EVENT_OCCURRENCE') THEN
        RAISE EXCEPTION 'invalid serving-control scope'
            USING ERRCODE = '23514';
    END IF;
    IF (p_scope_kind = 'CPI_DOMAIN' AND p_event_occurrence_id IS NOT NULL)
       OR (p_scope_kind = 'EVENT_OCCURRENCE' AND p_event_occurrence_id IS NULL) THEN
        RAISE EXCEPTION 'serving-control scope/event mismatch'
            USING ERRCODE = '23514';
    END IF;
    IF p_state NOT IN ('ENABLED', 'WITHHELD') THEN
        RAISE EXCEPTION 'invalid serving-control state'
            USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject IS NULL OR BTRIM(p_actor_subject) = '' THEN
        RAISE EXCEPTION 'serving-control actor is required'
            USING ERRCODE = '23514';
    END IF;
    IF p_actor_subject <> BTRIM(p_actor_subject) THEN
        RAISE EXCEPTION 'serving-control actor must be canonical'
            USING ERRCODE = '23514';
    END IF;

    IF p_scope_kind = 'EVENT_OCCURRENCE' THEN
        PERFORM 1
          FROM core_event_occurrences
         WHERE event_occurrence_id = p_event_occurrence_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'event occurrence does not exist'
                USING ERRCODE = '23503';
        END IF;
        PERFORM pg_advisory_xact_lock(
            hashtextextended('CPI_EVENT:' || p_event_occurrence_id::TEXT, 0)
        );
    END IF;

    scope_key := p_scope_kind || ':' || COALESCE(p_event_occurrence_id::TEXT, 'DOMAIN');
    PERFORM pg_advisory_xact_lock(hashtextextended(scope_key, 0));

    IF p_scope_kind = 'CPI_DOMAIN' THEN
        SELECT control_version, state
          INTO current_version, current_state
          FROM economic_serving_control_decisions
         WHERE scope_kind = 'CPI_DOMAIN'
         ORDER BY control_version DESC
         LIMIT 1;
    ELSE
        SELECT control_version, state
          INTO current_version, current_state
          FROM economic_serving_control_decisions
         WHERE scope_kind = 'EVENT_OCCURRENCE'
           AND event_occurrence_id = p_event_occurrence_id
         ORDER BY control_version DESC
         LIMIT 1;
    END IF;

    IF NOT FOUND THEN
        current_version := 0;
        current_state := 'ENABLED';
    END IF;

    IF p_expected_control_version <> current_version THEN
        RAISE EXCEPTION 'serving-control version conflict'
            USING ERRCODE = '40001';
    END IF;

    IF p_state = current_state THEN
        RAISE EXCEPTION 'serving-control decision would be a no-op'
            USING ERRCODE = '23514';
    END IF;

    IF p_state = 'ENABLED' AND current_state = 'WITHHELD' THEN
        IF p_case_ref IS NULL OR BTRIM(p_case_ref) = '' THEN
            RAISE EXCEPTION 're-enable requires a case reference'
                USING ERRCODE = '23514';
        END IF;
        IF p_scope_kind = 'EVENT_OCCURRENCE'
           AND (
               p_verified_knowledge_fingerprint IS NULL
               OR p_verified_knowledge_fingerprint !~ '^[0-9a-f]{64}$'
           ) THEN
            RAISE EXCEPTION 'event re-enable requires lowercase SHA-256 knowledge fingerprint'
                USING ERRCODE = '23514';
        END IF;
        IF p_scope_kind = 'CPI_DOMAIN'
           AND (p_verification_ref IS NULL OR BTRIM(p_verification_ref) = '') THEN
            RAISE EXCEPTION 'domain re-enable requires verification reference'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    INSERT INTO economic_serving_control_decisions (
        control_decision_id, scope_kind, event_occurrence_id,
        expected_control_version, control_version, state, reason_code,
        applied_at, actor_subject, case_ref, verified_knowledge_fingerprint
    ) VALUES (
        decision_id, p_scope_kind, p_event_occurrence_id,
        current_version, current_version + 1, p_state, p_reason_code,
        applied_time, p_actor_subject, p_case_ref, p_verified_knowledge_fingerprint
    );

    INSERT INTO business_audit_events (
        audit_event_id, action_kind, actor_subject, control_decision_id,
        case_ref, verification_ref, event_payload, occurred_at
    ) VALUES (
        gen_random_uuid(), 'ECONOMIC_SERVING_CONTROL_APPLIED',
        p_actor_subject, decision_id, p_case_ref, p_verification_ref,
        jsonb_build_object(
            'scope_kind', p_scope_kind,
            'event_occurrence_id', p_event_occurrence_id,
            'control_version', current_version + 1,
            'state', p_state,
            'reason_code', p_reason_code,
            'verified_knowledge_fingerprint', p_verified_knowledge_fingerprint
        ),
        applied_time
    );

    RETURN decision_id;
END;
$$;

DROP TRIGGER IF EXISTS interpretation_requests_immutable ON interpretation_requests;
CREATE TRIGGER interpretation_requests_immutable
    BEFORE UPDATE OR DELETE ON interpretation_requests
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS promotion_release_evidence_snapshots_immutable
    ON promotion_release_evidence_snapshots;
CREATE TRIGGER promotion_release_evidence_snapshots_immutable
    BEFORE UPDATE OR DELETE ON promotion_release_evidence_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS promotion_release_authorization_materials_immutable
    ON promotion_release_authorization_materials;
CREATE TRIGGER promotion_release_authorization_materials_immutable
    BEFORE UPDATE OR DELETE ON promotion_release_authorization_materials
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS promotion_release_authorizations_immutable
    ON promotion_release_authorizations;
CREATE TRIGGER promotion_release_authorizations_immutable
    BEFORE UPDATE OR DELETE ON promotion_release_authorizations
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS promotion_release_control_decisions_immutable
    ON promotion_release_control_decisions;
CREATE TRIGGER promotion_release_control_decisions_immutable
    BEFORE UPDATE OR DELETE ON promotion_release_control_decisions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS interpretation_approvals_immutable ON interpretation_approvals;
CREATE TRIGGER interpretation_approvals_immutable
    BEFORE UPDATE OR DELETE ON interpretation_approvals
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS interpretation_decisions_immutable ON interpretation_decisions;
CREATE TRIGGER interpretation_decisions_immutable
    BEFORE UPDATE OR DELETE ON interpretation_decisions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS business_audit_events_immutable ON business_audit_events;
CREATE TRIGGER business_audit_events_immutable
    BEFORE UPDATE OR DELETE ON business_audit_events
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS economic_serving_control_decisions_immutable
    ON economic_serving_control_decisions;
CREATE TRIGGER economic_serving_control_decisions_immutable
    BEFORE UPDATE OR DELETE ON economic_serving_control_decisions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();
