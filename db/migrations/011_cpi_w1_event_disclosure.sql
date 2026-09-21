-- CPI W1 event, schedule, disclosure, and marker evidence.
-- Additive above migration 010; legacy migration 009 remains unchanged.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'source_artifacts_artifact_source_identity'
           AND conrelid = 'source_artifacts'::regclass
    ) THEN
        ALTER TABLE source_artifacts
            ADD CONSTRAINT source_artifacts_artifact_source_identity
            UNIQUE (artifact_id, source_code);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS core_event_occurrences (
    event_occurrence_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type = 'CPI'),
    reference_month DATE NOT NULL,
    created_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT core_event_occurrences_reference_month_valid
        CHECK (EXTRACT(DAY FROM reference_month) = 1),
    CONSTRAINT core_event_occurrences_business_identity
        UNIQUE (event_type, reference_month)
);

CREATE TABLE IF NOT EXISTS event_schedule_assertions (
    schedule_assertion_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL DEFAULT 'SCHEDULE_ASSERTION'
        CHECK (subject_type = 'SCHEDULE_ASSERTION'),
    event_occurrence_id UUID NOT NULL
        REFERENCES core_event_occurrences(event_occurrence_id),
    schedule_status TEXT NOT NULL
        CHECK (schedule_status IN ('SCHEDULED', 'DATE_PENDING', 'CANCELED')),
    scheduled_date DATE,
    scheduled_at TIMESTAMPTZ,
    schedule_timezone TEXT,
    time_precision TEXT CHECK (
        time_precision IS NULL OR time_precision IN ('EXACT', 'DATE_ONLY')
    ),
    source_effective_date DATE,
    source_effective_at TIMESTAMPTZ,
    source_effective_precision TEXT CHECK (
        source_effective_precision IS NULL
        OR source_effective_precision IN ('EXACT', 'DATE_ONLY')
    ),
    source_code TEXT NOT NULL,
    source_artifact_id UUID NOT NULL,
    extractor_contract_version TEXT NOT NULL,
    accepted_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    accepted_at TIMESTAMPTZ NOT NULL,
    material_fingerprint TEXT NOT NULL CHECK (
        material_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_schedule_assertions_subject_fk
        FOREIGN KEY (schedule_assertion_id, subject_type)
        REFERENCES interpretation_subjects(subject_id, subject_type),
    CONSTRAINT event_schedule_assertions_artifact_source_fk
        FOREIGN KEY (source_artifact_id, source_code)
        REFERENCES source_artifacts(artifact_id, source_code),
    CONSTRAINT event_schedule_assertions_state_fields_valid CHECK (
        (
            schedule_status = 'SCHEDULED'
            AND (
                (
                    time_precision = 'EXACT'
                    AND scheduled_date IS NOT NULL
                    AND scheduled_at IS NOT NULL
                    AND NULLIF(BTRIM(schedule_timezone), '') IS NOT NULL
                )
                OR
                (
                    time_precision = 'DATE_ONLY'
                    AND scheduled_date IS NOT NULL
                    AND scheduled_at IS NULL
                    AND NULLIF(BTRIM(schedule_timezone), '') IS NOT NULL
                )
            )
        )
        OR
        (
            schedule_status IN ('DATE_PENDING', 'CANCELED')
            AND scheduled_date IS NULL
            AND scheduled_at IS NULL
            AND schedule_timezone IS NULL
            AND time_precision IS NULL
        )
    ),
    CONSTRAINT event_schedule_assertions_source_effective_valid CHECK (
        (
            source_effective_precision = 'EXACT'
            AND source_effective_date IS NOT NULL
            AND source_effective_at IS NOT NULL
        )
        OR
        (
            source_effective_precision = 'DATE_ONLY'
            AND source_effective_date IS NOT NULL
            AND source_effective_at IS NULL
        )
        OR
        (
            source_effective_precision IS NULL
            AND source_effective_date IS NULL
            AND source_effective_at IS NULL
        )
    ),
    CONSTRAINT event_schedule_assertions_parse_identity
        UNIQUE (event_occurrence_id, source_artifact_id, extractor_contract_version)
);

CREATE TABLE IF NOT EXISTS event_disclosures (
    disclosure_id UUID PRIMARY KEY,
    source_code TEXT NOT NULL REFERENCES data_sources(source_code),
    canonical_disclosure_key TEXT NOT NULL,
    disclosure_kind TEXT NOT NULL CHECK (disclosure_kind = 'DATA_RELEASE'),
    established_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_disclosures_business_identity
        UNIQUE (source_code, canonical_disclosure_key)
);

CREATE TABLE IF NOT EXISTS event_disclosure_links (
    disclosure_link_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL DEFAULT 'EVENT_DISCLOSURE_LINK'
        CHECK (subject_type = 'EVENT_DISCLOSURE_LINK'),
    event_occurrence_id UUID NOT NULL
        REFERENCES core_event_occurrences(event_occurrence_id),
    disclosure_id UUID NOT NULL REFERENCES event_disclosures(disclosure_id),
    relation_kind TEXT NOT NULL CHECK (
        relation_kind IN ('EVENT_RELEASE', 'SUPPLEMENTAL_DISCLOSURE')
    ),
    accepted_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    accepted_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_disclosure_links_subject_fk
        FOREIGN KEY (disclosure_link_id, subject_type)
        REFERENCES interpretation_subjects(subject_id, subject_type),
    CONSTRAINT event_disclosure_links_material_identity
        UNIQUE (event_occurrence_id, disclosure_id, relation_kind),
    CONSTRAINT event_disclosure_links_reference_identity
        UNIQUE (disclosure_link_id, event_occurrence_id, disclosure_id, relation_kind)
);

CREATE TABLE IF NOT EXISTS event_disclosure_artifacts (
    disclosure_artifact_link_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL DEFAULT 'DISCLOSURE_ARTIFACT_LINK'
        CHECK (subject_type = 'DISCLOSURE_ARTIFACT_LINK'),
    disclosure_id UUID NOT NULL REFERENCES event_disclosures(disclosure_id),
    artifact_id UUID NOT NULL REFERENCES source_artifacts(artifact_id),
    relation_kind TEXT NOT NULL CHECK (
        relation_kind IN (
            'RELEASE_REPRESENTATION',
            'CORROBORATING_REPRESENTATION',
            'CORRECTION_NOTICE'
        )
    ),
    accepted_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    accepted_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_disclosure_artifacts_subject_fk
        FOREIGN KEY (disclosure_artifact_link_id, subject_type)
        REFERENCES interpretation_subjects(subject_id, subject_type),
    CONSTRAINT event_disclosure_artifacts_material_identity
        UNIQUE (disclosure_id, artifact_id, relation_kind),
    CONSTRAINT event_disclosure_artifacts_reference_identity
        UNIQUE (disclosure_artifact_link_id, disclosure_id, artifact_id, relation_kind),
    CONSTRAINT event_disclosure_artifacts_marker_reference
        UNIQUE (disclosure_artifact_link_id, disclosure_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS disclosure_marker_assertions (
    marker_assertion_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL DEFAULT 'DISCLOSURE_MARKER_ASSERTION'
        CHECK (subject_type = 'DISCLOSURE_MARKER_ASSERTION'),
    disclosure_id UUID NOT NULL REFERENCES event_disclosures(disclosure_id),
    disclosure_artifact_link_id UUID NOT NULL,
    marker_semantics TEXT NOT NULL CHECK (marker_semantics = 'EMBARGO_LIFT'),
    marker_date DATE,
    marker_at TIMESTAMPTZ,
    marker_timezone TEXT,
    time_precision TEXT NOT NULL CHECK (
        time_precision IN ('EXACT', 'DATE_ONLY')
    ),
    source_code TEXT NOT NULL,
    source_artifact_id UUID NOT NULL,
    extractor_contract_version TEXT NOT NULL,
    accepted_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    accepted_at TIMESTAMPTZ NOT NULL,
    material_fingerprint TEXT NOT NULL CHECK (
        material_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT disclosure_marker_assertions_subject_fk
        FOREIGN KEY (marker_assertion_id, subject_type)
        REFERENCES interpretation_subjects(subject_id, subject_type),
    CONSTRAINT disclosure_marker_assertions_artifact_source_fk
        FOREIGN KEY (source_artifact_id, source_code)
        REFERENCES source_artifacts(artifact_id, source_code),
    CONSTRAINT disclosure_marker_assertions_provenance_fk
        FOREIGN KEY (
            disclosure_artifact_link_id, disclosure_id, source_artifact_id
        ) REFERENCES event_disclosure_artifacts(
            disclosure_artifact_link_id, disclosure_id, artifact_id
        ),
    CONSTRAINT disclosure_marker_assertions_precision_valid CHECK (
        (time_precision = 'EXACT'
            AND marker_date IS NOT NULL
            AND marker_at IS NOT NULL
            AND NULLIF(BTRIM(marker_timezone), '') IS NOT NULL)
        OR (time_precision = 'DATE_ONLY'
            AND marker_date IS NOT NULL
            AND marker_at IS NULL
            AND NULLIF(BTRIM(marker_timezone), '') IS NOT NULL)
    ),
    CONSTRAINT disclosure_marker_assertions_parse_identity
        UNIQUE (
            disclosure_artifact_link_id,
            marker_semantics,
            extractor_contract_version
        )
);


CREATE OR REPLACE FUNCTION enforce_cpi_w1_promoter_attempt()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $promoter$
DECLARE
    lineage_attempt UUID;
    lineage_scope TEXT;
    lineage_domain TEXT;
    lineage_field TEXT;
    row_payload JSONB;
BEGIN
    IF TG_NARGS <> 1 THEN
        RAISE EXCEPTION 'promoter lineage guard requires one lineage-field argument'
            USING ERRCODE = '23514';
    END IF;

    lineage_field := TG_ARGV[0];
    IF lineage_field NOT IN (
        'created_by_attempt_id',
        'established_by_attempt_id',
        'accepted_by_attempt_id'
    ) THEN
        RAISE EXCEPTION 'unsupported promoter lineage field: %', lineage_field
            USING ERRCODE = '23514';
    END IF;

    row_payload := to_jsonb(NEW);
    IF NOT row_payload ? lineage_field THEN
        RAISE EXCEPTION 'promoter lineage field % is absent from %',
            lineage_field, TG_TABLE_NAME
            USING ERRCODE = '23514';
    END IF;

    lineage_attempt := NULLIF(row_payload ->> lineage_field, '')::UUID;
    IF lineage_attempt IS NULL THEN
        RAISE EXCEPTION 'canonical CPI evidence requires promoter attempt lineage'
            USING ERRCODE = '23514';
    END IF;

    SELECT execution_scope, data_domain
      INTO STRICT lineage_scope, lineage_domain
      FROM ingestion_attempts
     WHERE attempt_id = lineage_attempt;

    IF lineage_scope <> 'ECONOMIC_PROMOTE' OR lineage_domain <> 'ECONOMIC' THEN
        RAISE EXCEPTION 'canonical CPI evidence requires ECONOMIC_PROMOTE attempt lineage'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$promoter$;

CREATE OR REPLACE FUNCTION enforce_cpi_w1_disclosure_artifact_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    disclosure_source TEXT;
    artifact_source TEXT;
BEGIN
    SELECT source_code INTO STRICT disclosure_source
      FROM event_disclosures
     WHERE disclosure_id = NEW.disclosure_id;

    SELECT source_code INTO STRICT artifact_source
      FROM source_artifacts
     WHERE artifact_id = NEW.artifact_id;

    IF disclosure_source <> artifact_source THEN
        RAISE EXCEPTION 'disclosure and artifact source mismatch'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS event_disclosure_artifacts_source_guard
    ON event_disclosure_artifacts;
CREATE TRIGGER event_disclosure_artifacts_source_guard
    BEFORE INSERT ON event_disclosure_artifacts
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_disclosure_artifact_source();

CREATE OR REPLACE FUNCTION reject_cpi_w1_immutable_evidence_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is immutable CPI W1 evidence', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS core_event_occurrences_immutable ON core_event_occurrences;
CREATE TRIGGER core_event_occurrences_immutable
    BEFORE UPDATE OR DELETE ON core_event_occurrences
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS event_schedule_assertions_immutable ON event_schedule_assertions;
CREATE TRIGGER event_schedule_assertions_immutable
    BEFORE UPDATE OR DELETE ON event_schedule_assertions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS event_disclosures_immutable ON event_disclosures;
CREATE TRIGGER event_disclosures_immutable
    BEFORE UPDATE OR DELETE ON event_disclosures
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS event_disclosure_links_immutable ON event_disclosure_links;
CREATE TRIGGER event_disclosure_links_immutable
    BEFORE UPDATE OR DELETE ON event_disclosure_links
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS event_disclosure_artifacts_immutable ON event_disclosure_artifacts;
CREATE TRIGGER event_disclosure_artifacts_immutable
    BEFORE UPDATE OR DELETE ON event_disclosure_artifacts
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS disclosure_marker_assertions_immutable
    ON disclosure_marker_assertions;
CREATE TRIGGER disclosure_marker_assertions_immutable
    BEFORE UPDATE OR DELETE ON disclosure_marker_assertions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();

DROP TRIGGER IF EXISTS core_event_occurrences_promoter_lineage_guard ON core_event_occurrences;
CREATE TRIGGER core_event_occurrences_promoter_lineage_guard
    BEFORE INSERT ON core_event_occurrences
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('created_by_attempt_id');

DROP TRIGGER IF EXISTS event_schedule_assertions_promoter_lineage_guard ON event_schedule_assertions;
CREATE TRIGGER event_schedule_assertions_promoter_lineage_guard
    BEFORE INSERT ON event_schedule_assertions
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('accepted_by_attempt_id');

DROP TRIGGER IF EXISTS event_disclosures_promoter_lineage_guard ON event_disclosures;
CREATE TRIGGER event_disclosures_promoter_lineage_guard
    BEFORE INSERT ON event_disclosures
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('established_by_attempt_id');

DROP TRIGGER IF EXISTS event_disclosure_links_promoter_lineage_guard ON event_disclosure_links;
CREATE TRIGGER event_disclosure_links_promoter_lineage_guard
    BEFORE INSERT ON event_disclosure_links
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('accepted_by_attempt_id');

DROP TRIGGER IF EXISTS event_disclosure_artifacts_promoter_lineage_guard ON event_disclosure_artifacts;
CREATE TRIGGER event_disclosure_artifacts_promoter_lineage_guard
    BEFORE INSERT ON event_disclosure_artifacts
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('accepted_by_attempt_id');

DROP TRIGGER IF EXISTS disclosure_marker_assertions_promoter_lineage_guard ON disclosure_marker_assertions;
CREATE TRIGGER disclosure_marker_assertions_promoter_lineage_guard
    BEFORE INSERT ON disclosure_marker_assertions
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('accepted_by_attempt_id');



-- System-known visibility is owned by the database transaction clock.
-- Callers may not backdate or future-date accepted_at.
CREATE OR REPLACE FUNCTION set_cpi_w1_accepted_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $accepted_at$
BEGIN
    NEW.accepted_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$accepted_at$;

DROP TRIGGER IF EXISTS event_schedule_assertions_accepted_at_guard
    ON event_schedule_assertions;
CREATE TRIGGER event_schedule_assertions_accepted_at_guard
    BEFORE INSERT ON event_schedule_assertions
    FOR EACH ROW EXECUTE FUNCTION set_cpi_w1_accepted_at();

DROP TRIGGER IF EXISTS event_disclosure_links_accepted_at_guard
    ON event_disclosure_links;
CREATE TRIGGER event_disclosure_links_accepted_at_guard
    BEFORE INSERT ON event_disclosure_links
    FOR EACH ROW EXECUTE FUNCTION set_cpi_w1_accepted_at();

DROP TRIGGER IF EXISTS event_disclosure_artifacts_accepted_at_guard
    ON event_disclosure_artifacts;
CREATE TRIGGER event_disclosure_artifacts_accepted_at_guard
    BEFORE INSERT ON event_disclosure_artifacts
    FOR EACH ROW EXECUTE FUNCTION set_cpi_w1_accepted_at();

DROP TRIGGER IF EXISTS disclosure_marker_assertions_accepted_at_guard
    ON disclosure_marker_assertions;
CREATE TRIGGER disclosure_marker_assertions_accepted_at_guard
    BEFORE INSERT ON disclosure_marker_assertions
    FOR EACH ROW EXECUTE FUNCTION set_cpi_w1_accepted_at();
