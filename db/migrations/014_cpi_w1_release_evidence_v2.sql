-- R1 additive migration. No policy, gate, review or authorization seed rows.
-- Tasks 6-8 are a connected unit: intermediate revisions are not deployable.
-- Inspect JSON (not JSONB) first so duplicate keys cannot disappear.
CREATE FUNCTION cpi_evidence_canonical_json(value JSON) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    kind TEXT := json_typeof(value);
    rendered TEXT;
    key_count BIGINT;
    distinct_count BIGINT;
BEGIN
    IF kind = 'object' THEN
        SELECT count(*), count(DISTINCT key COLLATE "C")
          INTO key_count, distinct_count FROM json_each(value);
        IF key_count <> distinct_count THEN
            RAISE EXCEPTION 'duplicate evidence JSON key' USING ERRCODE = '23514';
        END IF;
        SELECT '{' || coalesce(string_agg(to_json(key)::TEXT || ':' ||
                   cpi_evidence_canonical_json(child), ',' ORDER BY key COLLATE "C"), '') || '}'
          INTO rendered FROM json_each(value) AS fields(key, child);
    ELSIF kind = 'array' THEN
        SELECT '[' || coalesce(string_agg(cpi_evidence_canonical_json(child), ',' ORDER BY position), '') || ']'
          INTO rendered FROM json_array_elements(value) WITH ORDINALITY AS items(child, position);
    ELSIF kind = 'string' THEN
        rendered := to_json(value #>> '{}')::TEXT;
    ELSIF kind = 'number' THEN
        rendered := value::TEXT;
        IF rendered !~ '^(0|-?[1-9][0-9]*)$' THEN
            RAISE EXCEPTION 'evidence numbers must be canonical integers' USING ERRCODE = '23514';
        END IF;
    ELSIF kind IN ('boolean', 'null') THEN
        rendered := value::TEXT;
    ELSE
        RAISE EXCEPTION 'unsupported evidence JSON type' USING ERRCODE = '23514';
    END IF;
    RETURN rendered;
END;
$$;

CREATE FUNCTION validate_cpi_evidence_canonical_bytes(raw_bytes BYTEA) RETURNS JSONB
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    raw_json JSON := convert_from(raw_bytes, 'UTF8')::JSON;
BEGIN
    IF convert_to(cpi_evidence_canonical_json(raw_json), 'UTF8') <> raw_bytes THEN
        RAISE EXCEPTION 'noncanonical evidence bytes' USING ERRCODE = '23514';
    END IF;
    RETURN raw_json::JSONB;
END;
$$;

CREATE TABLE promotion_capability_evidence_policies (
    policy_digest TEXT PRIMARY KEY CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    release_subject_digest TEXT NOT NULL CHECK (release_subject_digest ~ '^[0-9a-f]{64}$'),
    promotion_capability_id TEXT NOT NULL CHECK (promotion_capability_id <> ''),
    schema_version TEXT NOT NULL CHECK (schema_version = 'cpi-w1-capability-evidence-policy-v1'),
    policy_version TEXT NOT NULL CHECK (policy_version = 'cpi-w1-capability-evidence-v1'),
    complete BOOLEAN NOT NULL,
    canonical_payload BYTEA NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (policy_digest, release_subject_digest, promotion_capability_id),
    CHECK (encode(sha256(canonical_payload), 'hex') = policy_digest),
    CHECK (validate_cpi_evidence_canonical_bytes(canonical_payload) = payload),
    CHECK (payload->>'schema' = schema_version),
    CHECK (payload->>'policy_version' = policy_version),
    CHECK (payload->>'release_subject_digest' = release_subject_digest),
    CHECK (payload->>'promotion_capability_id' = promotion_capability_id),
    CHECK (payload->'complete' = to_jsonb(complete)),
    CHECK (payload->'zero_official_allowed' = 'false'::JSONB),
    CHECK (jsonb_typeof(payload) = 'object' AND payload ?& ARRAY[
        'schema','policy_version','promotion_capability_id','release_subject_digest',
        'coverage_mode','coverage_rule','required_official_evidence_classes',
        'required_conformance_classes','required_exceptional_cases',
        'zero_official_allowed','complete','incomplete_reasons']
        AND payload - ARRAY['schema','policy_version','promotion_capability_id','release_subject_digest',
        'coverage_mode','coverage_rule','required_official_evidence_classes',
        'required_conformance_classes','required_exceptional_cases',
        'zero_official_allowed','complete','incomplete_reasons'] = '{}'::JSONB)
);

CREATE TABLE promotion_capability_evidence_policy_registrations (
    registration_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    release_subject_digest TEXT NOT NULL,
    promotion_capability_id TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    expected_registration_version BIGINT NOT NULL CHECK (expected_registration_version >= 0),
    registration_version BIGINT NOT NULL CHECK (registration_version = expected_registration_version + 1),
    actor TEXT NOT NULL CHECK (actor <> '' AND actor = btrim(actor)),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (release_subject_digest, registration_version),
    FOREIGN KEY (policy_digest, release_subject_digest, promotion_capability_id)
        REFERENCES promotion_capability_evidence_policies
            (policy_digest, release_subject_digest, promotion_capability_id)
);

CREATE TABLE promotion_release_review_artifacts (
    review_ref TEXT NOT NULL,
    review_digest TEXT NOT NULL CHECK (review_digest ~ '^[0-9a-f]{64}$'),
    raw_bytes BYTEA NOT NULL,
    payload JSONB NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = 'cpi-w1-review-artifact-v1'),
    purpose TEXT NOT NULL CHECK (purpose IN ('PROMOTION_AUTHORIZATION', 'EXPECTED_DIFF')),
    bindings JSONB NOT NULL,
    PRIMARY KEY (review_ref, review_digest),
    CHECK (encode(sha256(raw_bytes), 'hex') = review_digest),
    CHECK (validate_cpi_evidence_canonical_bytes(raw_bytes) = payload),
    CHECK (payload->>'schema' = schema_version),
    CHECK (payload->>'purpose' = purpose),
    CHECK (payload->'bindings' = bindings),
    CHECK (jsonb_typeof(payload) = 'object' AND payload ?& ARRAY['schema','purpose','bindings']
           AND payload - ARRAY['schema','purpose','bindings'] = '{}'::JSONB)
);

CREATE FUNCTION enforce_cpi_evidence_policy_payload() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    item TEXT;
    list_value JSONB;
    rule JSONB := NEW.payload->'coverage_rule';
    coverage TEXT := NEW.payload->>'coverage_mode';
    normalized JSONB;
BEGIN
    IF NEW.payload->>'schema' IS DISTINCT FROM NEW.schema_version
       OR NEW.payload->>'policy_version' IS DISTINCT FROM NEW.policy_version
       OR NEW.payload->>'release_subject_digest' IS DISTINCT FROM NEW.release_subject_digest
       OR NEW.payload->>'promotion_capability_id' IS DISTINCT FROM NEW.promotion_capability_id
       OR NEW.payload->'complete' IS DISTINCT FROM to_jsonb(NEW.complete)
       OR NEW.payload->'zero_official_allowed' IS DISTINCT FROM 'false'::JSONB THEN
        RAISE EXCEPTION 'policy columns must exactly match payload' USING ERRCODE='23514';
    END IF;
    FOREACH item IN ARRAY ARRAY['required_official_evidence_classes','required_conformance_classes',
                                'required_exceptional_cases','incomplete_reasons'] LOOP
        list_value := NEW.payload->item;
        IF jsonb_typeof(list_value) IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'policy obligations must be arrays' USING ERRCODE='23514';
        END IF;
        IF EXISTS (SELECT 1 FROM jsonb_array_elements(list_value) e
                    WHERE jsonb_typeof(e) <> 'string' OR e #>> '{}' = ''
                       OR e #>> '{}' <> btrim(e #>> '{}')) THEN
            RAISE EXCEPTION 'policy obligations must be canonical strings' USING ERRCODE='23514';
        END IF;
        SELECT coalesce(jsonb_agg(v ORDER BY v COLLATE "C"),'[]'::JSONB) INTO normalized
            FROM (SELECT DISTINCT e #>> '{}' AS v FROM jsonb_array_elements(list_value) e) sorted;
        IF normalized IS DISTINCT FROM list_value THEN
            RAISE EXCEPTION 'policy obligation sets must be sorted and unique' USING ERRCODE='23514';
        END IF;
        IF item IN ('required_official_evidence_classes','required_conformance_classes') AND list_value='[]'::JSONB THEN
            RAISE EXCEPTION 'empty evidence obligations' USING ERRCODE='23514';
        END IF;
    END LOOP;
    IF NEW.complete = (NEW.payload->'incomplete_reasons' <> '[]'::JSONB) THEN
        RAISE EXCEPTION 'policy completeness/reasons disagree' USING ERRCODE='23514';
    END IF;
    IF coverage='HISTORICAL_MONTHLY_BASELINE' THEN
        IF jsonb_typeof(rule) IS DISTINCT FROM 'object'
           OR NOT rule ?& ARRAY['start_reference_month','end_reference_month','verified_cancellation_required']
           OR rule - ARRAY['start_reference_month','end_reference_month','verified_cancellation_required'] <> '{}'::JSONB
           OR coalesce(rule->>'start_reference_month','') !~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
           OR coalesce(rule->>'end_reference_month','') !~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
           OR rule->>'start_reference_month' > rule->>'end_reference_month'
           OR rule->'verified_cancellation_required' IS DISTINCT FROM 'true'::JSONB THEN
            RAISE EXCEPTION 'invalid historical policy rule' USING ERRCODE='23514';
        END IF;
    ELSIF coverage='EXPLICIT_HISTORICAL_EXCEPTIONS' THEN
        IF rule IS DISTINCT FROM '{"reference_months":["2025-10"]}'::JSONB THEN
            RAISE EXCEPTION 'invalid exception policy rule' USING ERRCODE='23514';
        END IF;
    ELSIF coverage='UNFROZEN_SURFACE_COVERAGE' THEN
        IF NEW.complete OR jsonb_typeof(rule) IS DISTINCT FROM 'object'
           OR NOT rule ? 'surface' OR rule - 'surface' <> '{}'::JSONB
           OR coalesce(rule->>'surface','') = '' THEN
            RAISE EXCEPTION 'invalid unfrozen surface rule' USING ERRCODE='23514';
        END IF;
    ELSE
        RAISE EXCEPTION 'unsupported policy coverage mode' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER cpi_evidence_policy_payload_guard BEFORE INSERT ON promotion_capability_evidence_policies
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_evidence_policy_payload();

CREATE FUNCTION enforce_cpi_review_artifact_payload() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    required_keys TEXT[];
    item RECORD;
BEGIN
    IF NEW.payload->>'schema' IS DISTINCT FROM NEW.schema_version
       OR NEW.payload->>'purpose' IS DISTINCT FROM NEW.purpose
       OR NEW.payload->'bindings' IS DISTINCT FROM NEW.bindings
       OR NEW.review_ref = '' OR NEW.review_ref <> btrim(NEW.review_ref)
       OR NEW.review_ref LIKE '/%' OR NEW.review_ref LIKE '%\%'
       OR EXISTS (SELECT 1 FROM unnest(string_to_array(NEW.review_ref,'/')) p WHERE p IN ('','.','..')) THEN
        RAISE EXCEPTION 'invalid review columns/reference' USING ERRCODE='23514';
    END IF;
    IF NEW.purpose='PROMOTION_AUTHORIZATION' THEN
        required_keys := ARRAY['release_subject_digest','promotion_capability_id',
            'capability_evidence_policy_digest','evidence_snapshot_digest','source_contract_digest',
            'tested_source_content_digest','tested_executor_source_revision','tested_workload_artifact_digest',
            'tested_job_contract_version','gate_policy_version'];
    ELSE
        required_keys := ARRAY['release_subject_digest','promotion_capability_id','artifact_sha256',
            'extractor_contract_version','expected_semantics_sha256','actual_semantics_sha256'];
    END IF;
    IF jsonb_typeof(NEW.bindings) IS DISTINCT FROM 'object'
       OR NOT NEW.bindings ?& required_keys OR NEW.bindings - required_keys <> '{}'::JSONB THEN
        RAISE EXCEPTION 'review requires exact binding keys' USING ERRCODE='23514';
    END IF;
    FOR item IN SELECT key,value FROM jsonb_each(NEW.bindings) LOOP
        IF jsonb_typeof(item.value) <> 'string' OR item.value #>> '{}' = ''
           OR item.value #>> '{}' <> btrim(item.value #>> '{}')
           OR ((item.key LIKE '%digest' OR item.key LIKE '%sha256') AND item.value #>> '{}' !~ '^[0-9a-f]{64}$') THEN
            RAISE EXCEPTION 'invalid review binding value' USING ERRCODE='23514';
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;
CREATE TRIGGER cpi_review_artifact_payload_guard BEFORE INSERT ON promotion_release_review_artifacts
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_review_artifact_payload();

CREATE FUNCTION enforce_cpi_capability_policy_registration() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    current_version BIGINT;
BEGIN
    PERFORM lock_cpi_domain_shared();
    PERFORM pg_advisory_xact_lock(hashtextextended('CPI_RELEASE_SUBJECT:' || NEW.release_subject_digest, 0));
    SELECT coalesce(max(registration_version), 0) INTO current_version
      FROM promotion_capability_evidence_policy_registrations
     WHERE release_subject_digest = NEW.release_subject_digest;
    IF NEW.expected_registration_version <> current_version
       OR NEW.registration_version <> current_version + 1 THEN
        RAISE EXCEPTION 'stale evidence policy registration version' USING ERRCODE = '40001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER promotion_capability_policy_registration_guard
    BEFORE INSERT ON promotion_capability_evidence_policy_registrations
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_capability_policy_registration();

CREATE FUNCTION register_cpi_capability_evidence_policy(
    subject TEXT, capability TEXT, digest TEXT, expected_version BIGINT, registration_actor TEXT
) RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
    inserted_id UUID;
BEGIN
    INSERT INTO promotion_capability_evidence_policy_registrations
        (release_subject_digest, promotion_capability_id, policy_digest,
         expected_registration_version, registration_version, actor)
    VALUES (subject, capability, digest, expected_version, expected_version + 1, registration_actor)
    RETURNING registration_id INTO inserted_id;
    RETURN inserted_id;
END;
$$;

CREATE TRIGGER promotion_capability_evidence_policies_immutable
    BEFORE UPDATE OR DELETE ON promotion_capability_evidence_policies
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();
CREATE TRIGGER promotion_capability_evidence_policy_registrations_immutable
    BEFORE UPDATE OR DELETE ON promotion_capability_evidence_policy_registrations
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();
CREATE TRIGGER promotion_release_review_artifacts_immutable
    BEFORE UPDATE OR DELETE ON promotion_release_review_artifacts
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

-- PostgreSQL truncates generated constraint names; identify these two original
-- single-column version checks by attnum rather than guessing their spelling.
DO $$
DECLARE constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid=c.conrelid AND c.conkey=ARRAY[a.attnum]::SMALLINT[]
        WHERE c.conrelid='promotion_release_evidence_snapshots'::regclass
          AND c.contype='c' AND a.attname IN ('schema_version','evidence_policy_version')
    LOOP
        EXECUTE format('ALTER TABLE promotion_release_evidence_snapshots DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE promotion_release_evidence_snapshots
    ALTER COLUMN corpus_snapshot_digest DROP NOT NULL,
    ALTER COLUMN tested_source_revision DROP NOT NULL,
    ADD COLUMN promotion_capability_id TEXT,
    ADD COLUMN capability_evidence_policy_digest TEXT CHECK (capability_evidence_policy_digest ~ '^[0-9a-f]{64}$'),
    ADD COLUMN capability_corpus_snapshot_digest TEXT CHECK (capability_corpus_snapshot_digest ~ '^[0-9a-f]{64}$'),
    ADD COLUMN source_contract_digest TEXT CHECK (source_contract_digest ~ '^[0-9a-f]{64}$'),
    ADD COLUMN tested_source_content_digest TEXT CHECK (tested_source_content_digest ~ '^[0-9a-f]{64}$'),
    ADD COLUMN tested_executor_source_revision TEXT CHECK (
        tested_executor_source_revision <> '' AND tested_executor_source_revision = btrim(tested_executor_source_revision)),
    ADD COLUMN canonical_payload BYTEA,
    ADD CONSTRAINT cpi_evidence_schema_boundary CHECK (
        (schema_version = 'cpi-w1-promotion-evidence-snapshot-v1'
         AND evidence_policy_version = 'cpi-w1-evidence-v1'
         AND corpus_snapshot_digest IS NOT NULL AND tested_source_revision IS NOT NULL
         AND promotion_capability_id IS NULL AND capability_evidence_policy_digest IS NULL
         AND capability_corpus_snapshot_digest IS NULL AND source_contract_digest IS NULL
         AND tested_source_content_digest IS NULL AND tested_executor_source_revision IS NULL
         AND canonical_payload IS NULL)
        OR
        (schema_version = 'cpi-w1-promotion-evidence-snapshot-v2'
         AND evidence_policy_version = 'cpi-w1-evidence-v2'
         AND corpus_snapshot_digest IS NULL AND tested_source_revision IS NULL
         AND promotion_capability_id IS NOT NULL AND promotion_capability_id <> ''
         AND capability_evidence_policy_digest IS NOT NULL
         AND capability_corpus_snapshot_digest IS NOT NULL AND source_contract_digest IS NOT NULL
         AND tested_source_content_digest IS NOT NULL AND canonical_payload IS NOT NULL)),
    ADD CONSTRAINT cpi_evidence_exact_policy_fk FOREIGN KEY
        (capability_evidence_policy_digest, release_subject_digest, promotion_capability_id)
        REFERENCES promotion_capability_evidence_policies
            (policy_digest, release_subject_digest, promotion_capability_id);

CREATE FUNCTION enforce_cpi_v2_evidence_payload() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    expected JSONB;
BEGIN
    IF NEW.schema_version = 'cpi-w1-promotion-evidence-snapshot-v2' THEN
        expected := jsonb_build_object(
            'schema', NEW.schema_version, 'evidence_policy_version', NEW.evidence_policy_version,
            'promotion_capability_id', NEW.promotion_capability_id,
            'release_subject_digest', NEW.release_subject_digest,
            'capability_evidence_policy_digest', NEW.capability_evidence_policy_digest,
            'capability_corpus_snapshot_digest', NEW.capability_corpus_snapshot_digest,
            'expected_diff_approvals_digest', NEW.expected_diff_approvals_digest,
            'replay_result_digest', NEW.replay_result_digest,
            'source_contract_digest', NEW.source_contract_digest,
            'tested_source_content_digest', NEW.tested_source_content_digest,
            'tested_executor_source_revision', NEW.tested_executor_source_revision,
            'tested_workload_artifact_digest', NEW.tested_workload_artifact_digest,
            'tested_job_contract_version', NEW.tested_job_contract_version);
        IF validate_cpi_evidence_canonical_bytes(NEW.canonical_payload) IS DISTINCT FROM expected
           OR encode(sha256(NEW.canonical_payload), 'hex') IS DISTINCT FROM NEW.evidence_snapshot_digest THEN
            RAISE EXCEPTION 'V2 evidence payload/column/hash mismatch' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER cpi_v2_evidence_payload_guard BEFORE INSERT ON promotion_release_evidence_snapshots
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_v2_evidence_payload();
