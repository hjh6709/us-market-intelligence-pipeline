-- CPI W1 interpretation governance and economic serving-control history.
-- Additive above migration 012; production IAM/GRANT topology is intentionally deferred.

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
            'ECONOMIC_SERVING_CONTROL_APPLIED'
        )
    ),
    actor_subject TEXT NOT NULL CHECK (
        BTRIM(actor_subject) <> '' AND actor_subject = BTRIM(actor_subject)
    ),
    interpretation_decision_id UUID
        REFERENCES interpretation_decisions(interpretation_decision_id),
    control_decision_id UUID
        REFERENCES economic_serving_control_decisions(control_decision_id),
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
        )
        OR
        (
            action_kind = 'ECONOMIC_SERVING_CONTROL_APPLIED'
            AND interpretation_decision_id IS NULL
            AND control_decision_id IS NOT NULL
        )
    )
);

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
