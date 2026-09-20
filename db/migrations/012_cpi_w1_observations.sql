-- CPI W1 observation definitions and immutable official observation assertions.
-- Additive above migration 011; legacy migrations 001-009 remain unchanged.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'core_event_occurrences_observation_identity'
           AND conrelid = 'core_event_occurrences'::regclass
    ) THEN
        ALTER TABLE core_event_occurrences
            ADD CONSTRAINT core_event_occurrences_observation_identity
            UNIQUE (event_occurrence_id, event_type);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'event_disclosure_links_observation_reference'
           AND conrelid = 'event_disclosure_links'::regclass
    ) THEN
        ALTER TABLE event_disclosure_links
            ADD CONSTRAINT event_disclosure_links_observation_reference
            UNIQUE (disclosure_link_id, event_occurrence_id, disclosure_id);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS observation_definitions (
    observation_code TEXT PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type = 'CPI'),
    canonical_unit TEXT NOT NULL CHECK (canonical_unit = 'PERCENT'),
    display_name TEXT NOT NULL,
    CONSTRAINT observation_definitions_event_identity
        UNIQUE (observation_code, event_type)
);

INSERT INTO observation_definitions (
    observation_code, event_type, canonical_unit, display_name
) VALUES
    ('CPI_HEADLINE_MOM', 'CPI', 'PERCENT', 'CPI headline month-over-month'),
    ('CPI_HEADLINE_YOY', 'CPI', 'PERCENT', 'CPI headline year-over-year'),
    ('CPI_CORE_MOM', 'CPI', 'PERCENT', 'CPI core month-over-month'),
    ('CPI_CORE_YOY', 'CPI', 'PERCENT', 'CPI core year-over-year')
ON CONFLICT (observation_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS official_observation_assertions (
    assertion_id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL DEFAULT 'OFFICIAL_OBSERVATION_ASSERTION'
        CHECK (subject_type = 'OFFICIAL_OBSERVATION_ASSERTION'),
    event_occurrence_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'CPI'),
    disclosure_id UUID NOT NULL REFERENCES event_disclosures(disclosure_id),
    disclosure_link_id UUID NOT NULL,
    disclosure_artifact_link_id UUID NOT NULL,
    observation_code TEXT NOT NULL,
    assertion_state TEXT NOT NULL CHECK (
        assertion_state IN ('VALUE', 'EXPLICIT_UNAVAILABLE')
    ),
    normalized_value NUMERIC,
    source_value_text TEXT,
    source_reason_text TEXT,
    source_code TEXT NOT NULL,
    source_artifact_id UUID NOT NULL,
    extractor_contract_version TEXT NOT NULL,
    accepted_by_attempt_id UUID NOT NULL REFERENCES ingestion_attempts(attempt_id),
    accepted_at TIMESTAMPTZ NOT NULL,
    material_fingerprint TEXT NOT NULL CHECK (
        material_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT official_observation_assertions_subject_fk
        FOREIGN KEY (assertion_id, subject_type)
        REFERENCES interpretation_subjects(subject_id, subject_type),
    CONSTRAINT official_observation_assertions_event_fk
        FOREIGN KEY (event_occurrence_id, event_type)
        REFERENCES core_event_occurrences(event_occurrence_id, event_type),
    CONSTRAINT official_observation_assertions_definition_fk
        FOREIGN KEY (observation_code, event_type)
        REFERENCES observation_definitions(observation_code, event_type),
    CONSTRAINT official_observation_assertions_disclosure_provenance_fk
        FOREIGN KEY (
            disclosure_link_id, event_occurrence_id, disclosure_id
        ) REFERENCES event_disclosure_links(
            disclosure_link_id, event_occurrence_id, disclosure_id
        ),
    CONSTRAINT official_observation_assertions_artifact_provenance_fk
        FOREIGN KEY (
            disclosure_artifact_link_id, disclosure_id, source_artifact_id
        ) REFERENCES event_disclosure_artifacts(
            disclosure_artifact_link_id, disclosure_id, artifact_id
        ),
    CONSTRAINT official_observation_assertions_artifact_source_fk
        FOREIGN KEY (source_artifact_id, source_code)
        REFERENCES source_artifacts(artifact_id, source_code),
    CONSTRAINT official_observation_assertions_state_value_valid CHECK (
        (assertion_state = 'VALUE' AND normalized_value IS NOT NULL)
        OR
        (
            assertion_state = 'EXPLICIT_UNAVAILABLE'
            AND normalized_value IS NULL
            AND NULLIF(BTRIM(source_reason_text), '') IS NOT NULL
        )
    ),
    CONSTRAINT official_observation_assertions_parse_identity
        UNIQUE (
            disclosure_link_id,
            disclosure_artifact_link_id,
            observation_code,
            extractor_contract_version
        )
);

DROP TRIGGER IF EXISTS official_observation_assertions_immutable
    ON official_observation_assertions;
CREATE TRIGGER official_observation_assertions_immutable
    BEFORE UPDATE OR DELETE ON official_observation_assertions
    FOR EACH ROW EXECUTE FUNCTION reject_cpi_w1_immutable_evidence_mutation();


DROP TRIGGER IF EXISTS official_observation_assertions_promoter_lineage_guard
    ON official_observation_assertions;
CREATE TRIGGER official_observation_assertions_promoter_lineage_guard
    BEFORE INSERT ON official_observation_assertions
    FOR EACH ROW EXECUTE FUNCTION enforce_cpi_w1_promoter_attempt('accepted_by_attempt_id');
