"""Atomic CPI W1 release-envelope and Core 4 promotion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID, UUID as UUIDType, uuid5

from src.cpi_w1_contracts import (
    DisclosureArtifactRelationKind,
    DisclosureRelationKind,
    ObservationState,
    PromotionFamily,
    material_fingerprint,
)
from src.cpi_w1_release import (
    CorroboratingRepresentationCandidate,
    ObservationBundleCandidate,
    ReleaseEnvelopeCandidate,
)
from src.cpi_w1_repository import (
    Claim,
    CpiW1Repository,
    RepositoryInvariantError,
)


_NAMESPACE = UUIDType("f345f13c-72b0-4e59-9fb2-2af2ab235a20")
_CORE4 = {
    "CPI_HEADLINE_MOM",
    "CPI_HEADLINE_YOY",
    "CPI_CORE_MOM",
    "CPI_CORE_YOY",
}

_ALLOWED_EXTRACTORS_BY_ARTIFACT = {
    "CPI_RELEASE_HTML": {"bls-cpi-release-html-v1"},
    "CPI_TABLE1_XLSX": {"bls-cpi-table1-xlsx-v1"},
}


class PromotionInvariantError(RuntimeError):
    pass


class PromotionDeterminismError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromotionResult:
    event_occurrence_id: UUID | None
    disclosure_id: UUID | None
    disclosure_link_id: UUID | None
    disclosure_artifact_link_id: UUID | None
    verified_observation_count: int = 0


def _stable_uuid(kind: str, *parts: object) -> UUID:
    return uuid5(_NAMESPACE, "|".join([kind, *(str(part) for part in parts)]))


def canonical_release_disclosure_key(reference_month) -> str:
    return f"BLS:CPI:{reference_month.isoformat()}:DATA_RELEASE"


def promotion_work_key(
    family: PromotionFamily,
    artifact_id: UUID,
    extractor_contract_version: str,
) -> str:
    if (
        not extractor_contract_version
        or extractor_contract_version != extractor_contract_version.strip()
        or any(
            char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
            for char in extractor_contract_version
        )
    ):
        raise ValueError("extractor_contract_version must be a canonical token")
    return f"{family.value}:{artifact_id}:{extractor_contract_version}"


class CpiW1Promoter:
    def __init__(self, repository: CpiW1Repository | None = None) -> None:
        self.repository = repository or CpiW1Repository()

    def _verify_input_artifact(
        self,
        connection: Any,
        claim: Claim,
        artifact_id: UUID,
        *,
        allowed_kinds: set[str],
        artifact_content_sha256: str,
        extractor_contract_version: str,
    ) -> tuple[str, str]:
        _run_id, input_artifact_id = self.repository.assert_current_claim(
            connection,
            claim,
        )
        if claim.execution_scope != "ECONOMIC_PROMOTE":
            raise PromotionInvariantError("canonical promotion requires ECONOMIC_PROMOTE")
        if input_artifact_id != artifact_id or claim.input_artifact_id != artifact_id:
            raise PromotionInvariantError("promotion claim input artifact mismatch")

        row = connection.execute(
            """
            SELECT source_code, artifact_contract_kind, source_contract_version,
                   content_sha256
              FROM source_artifacts
             WHERE artifact_id=%s
               AND data_domain='ECONOMIC'
            """,
            (artifact_id,),
        ).fetchone()
        if row is None:
            raise PromotionInvariantError("promotion artifact does not exist")
        source_code, artifact_kind, source_contract_version, content_sha256 = row
        if source_code != "BLS":
            raise PromotionInvariantError("CPI W1 promotion requires BLS artifact")
        if artifact_kind not in allowed_kinds:
            raise PromotionInvariantError("artifact contract kind is not eligible")
        if source_contract_version != "bls-cpi-source-v1":
            raise PromotionInvariantError("unsupported BLS CPI source contract version")
        if content_sha256 != artifact_content_sha256:
            raise PromotionInvariantError(
                "parsed candidate does not match input artifact content hash"
            )
        allowed_extractors = _ALLOWED_EXTRACTORS_BY_ARTIFACT.get(artifact_kind, set())
        if extractor_contract_version not in allowed_extractors:
            raise PromotionInvariantError(
                "extractor contract version is not approved for artifact kind"
            )
        return artifact_kind, source_contract_version

    @staticmethod
    def _ensure_subject(connection: Any, subject_id: UUID, subject_type: str) -> None:
        connection.execute(
            """
            INSERT INTO interpretation_subjects (subject_id, subject_type)
            VALUES (%s, %s)
            ON CONFLICT (subject_id) DO NOTHING
            """,
            (subject_id, subject_type),
        )
        row = connection.execute(
            """
            SELECT subject_type
              FROM interpretation_subjects
             WHERE subject_id=%s
            """,
            (subject_id,),
        ).fetchone()
        if row != (subject_type,):
            raise PromotionDeterminismError("stable subject UUID resolved to wrong type")

    def promote_release_envelope(
        self,
        connection: Any,
        claim: Claim,
        *,
        artifact_id: UUID,
        candidate: ReleaseEnvelopeCandidate,
    ) -> PromotionResult:
        if candidate.event_type != "CPI":
            raise PromotionInvariantError("release candidate is not CPI")
        expected_work_key = promotion_work_key(
            PromotionFamily.CPI_RELEASE_ENVELOPE_PROMOTE,
            artifact_id,
            candidate.extractor_contract_version,
        )
        if claim.work_key != expected_work_key:
            raise PromotionInvariantError("claim is not release-envelope promotion work")

        with connection.transaction():
            self._verify_input_artifact(
                connection,
                claim,
                artifact_id,
                allowed_kinds={"CPI_RELEASE_HTML"},
                artifact_content_sha256=candidate.artifact_content_sha256,
                extractor_contract_version=candidate.extractor_contract_version,
            )

            event_id = _stable_uuid(
                "event",
                candidate.event_type,
                candidate.reference_month.isoformat(),
            )
            connection.execute(
                """
                INSERT INTO core_event_occurrences (
                    event_occurrence_id, event_type, reference_month,
                    created_by_attempt_id
                ) VALUES (%s, 'CPI', %s, %s)
                ON CONFLICT (event_type, reference_month) DO NOTHING
                """,
                (event_id, candidate.reference_month, claim.attempt_id),
            )
            event_row = connection.execute(
                """
                SELECT event_occurrence_id
                  FROM core_event_occurrences
                 WHERE event_type='CPI' AND reference_month=%s
                """,
                (candidate.reference_month,),
            ).fetchone()
            if event_row is None:
                raise PromotionInvariantError("CPI event occurrence was not established")
            event_id = event_row[0]
            self.repository.lock_cpi_event(connection, event_id)

            disclosure_key = canonical_release_disclosure_key(candidate.reference_month)
            disclosure_id = _stable_uuid("disclosure", disclosure_key)
            connection.execute(
                """
                INSERT INTO event_disclosures (
                    disclosure_id, source_code, canonical_disclosure_key,
                    disclosure_kind, established_by_attempt_id
                ) VALUES (%s, 'BLS', %s, 'DATA_RELEASE', %s)
                ON CONFLICT (source_code, canonical_disclosure_key) DO NOTHING
                """,
                (disclosure_id, disclosure_key, claim.attempt_id),
            )
            disclosure_row = connection.execute(
                """
                SELECT disclosure_id, disclosure_kind
                  FROM event_disclosures
                 WHERE source_code='BLS' AND canonical_disclosure_key=%s
                """,
                (disclosure_key,),
            ).fetchone()
            if disclosure_row is None or disclosure_row[1] != "DATA_RELEASE":
                raise PromotionDeterminismError("disclosure identity did not converge")
            disclosure_id = disclosure_row[0]

            link_row = connection.execute(
                """
                SELECT disclosure_link_id
                  FROM event_disclosure_links
                 WHERE event_occurrence_id=%s
                   AND disclosure_id=%s
                   AND relation_kind='EVENT_RELEASE'
                """,
                (event_id, disclosure_id),
            ).fetchone()
            if link_row is None:
                disclosure_link_id = _stable_uuid(
                    "event-release-link",
                    event_id,
                    disclosure_id,
                )
                self._ensure_subject(
                    connection,
                    disclosure_link_id,
                    "EVENT_DISCLOSURE_LINK",
                )
                connection.execute(
                    """
                    INSERT INTO event_disclosure_links (
                        disclosure_link_id, event_occurrence_id, disclosure_id,
                        relation_kind, accepted_by_attempt_id, accepted_at
                    ) VALUES (
                        %s, %s, %s, 'EVENT_RELEASE', %s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (event_occurrence_id, disclosure_id, relation_kind)
                    DO NOTHING
                    """,
                    (
                        disclosure_link_id,
                        event_id,
                        disclosure_id,
                        claim.attempt_id,
                    ),
                )
                link_row = connection.execute(
                    """
                    SELECT disclosure_link_id
                      FROM event_disclosure_links
                     WHERE event_occurrence_id=%s
                       AND disclosure_id=%s
                       AND relation_kind='EVENT_RELEASE'
                    """,
                    (event_id, disclosure_id),
                ).fetchone()
            if link_row is None:
                raise PromotionInvariantError("EVENT_RELEASE relation was not established")
            disclosure_link_id = link_row[0]

            artifact_link_row = connection.execute(
                """
                SELECT disclosure_artifact_link_id
                  FROM event_disclosure_artifacts
                 WHERE disclosure_id=%s
                   AND artifact_id=%s
                   AND relation_kind='RELEASE_REPRESENTATION'
                """,
                (disclosure_id, artifact_id),
            ).fetchone()
            if artifact_link_row is None:
                artifact_link_id = _stable_uuid(
                    "release-artifact-link",
                    disclosure_id,
                    artifact_id,
                    DisclosureArtifactRelationKind.RELEASE_REPRESENTATION.value,
                )
                self._ensure_subject(
                    connection,
                    artifact_link_id,
                    "DISCLOSURE_ARTIFACT_LINK",
                )
                connection.execute(
                    """
                    INSERT INTO event_disclosure_artifacts (
                        disclosure_artifact_link_id, disclosure_id, artifact_id,
                        relation_kind, accepted_by_attempt_id, accepted_at
                    ) VALUES (
                        %s, %s, %s, 'RELEASE_REPRESENTATION', %s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (disclosure_id, artifact_id, relation_kind)
                    DO NOTHING
                    """,
                    (
                        artifact_link_id,
                        disclosure_id,
                        artifact_id,
                        claim.attempt_id,
                    ),
                )
                artifact_link_row = connection.execute(
                    """
                    SELECT disclosure_artifact_link_id
                      FROM event_disclosure_artifacts
                     WHERE disclosure_id=%s
                       AND artifact_id=%s
                       AND relation_kind='RELEASE_REPRESENTATION'
                    """,
                    (disclosure_id, artifact_id),
                ).fetchone()
            if artifact_link_row is None:
                raise PromotionInvariantError(
                    "release representation relation was not established"
                )
            artifact_link_id = artifact_link_row[0]

            marker_fp = material_fingerprint(
                {
                    "marker_semantics": candidate.marker_semantics,
                    "marker_date": candidate.marker_date,
                    "marker_at": candidate.marker_at,
                    "marker_timezone": candidate.marker_timezone,
                    "time_precision": candidate.time_precision,
                }
            )
            marker_row = connection.execute(
                """
                SELECT marker_assertion_id, material_fingerprint, marker_date,
                       marker_at, marker_timezone, time_precision
                  FROM disclosure_marker_assertions
                 WHERE disclosure_artifact_link_id=%s
                   AND marker_semantics=%s
                   AND extractor_contract_version=%s
                """,
                (
                    artifact_link_id,
                    candidate.marker_semantics,
                    candidate.extractor_contract_version,
                ),
            ).fetchone()
            if marker_row is None:
                marker_id = _stable_uuid(
                    "marker",
                    artifact_link_id,
                    candidate.marker_semantics,
                    candidate.extractor_contract_version,
                )
                self._ensure_subject(
                    connection,
                    marker_id,
                    "DISCLOSURE_MARKER_ASSERTION",
                )
                connection.execute(
                    """
                    INSERT INTO disclosure_marker_assertions (
                        marker_assertion_id, disclosure_id,
                        disclosure_artifact_link_id, marker_semantics,
                        marker_date, marker_at, marker_timezone, time_precision,
                        source_code, source_artifact_id,
                        extractor_contract_version, accepted_by_attempt_id,
                        accepted_at, material_fingerprint
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        'BLS', %s, %s, %s, CURRENT_TIMESTAMP, %s
                    )
                    ON CONFLICT (
                        disclosure_artifact_link_id,
                        marker_semantics,
                        extractor_contract_version
                    ) DO NOTHING
                    """,
                    (
                        marker_id,
                        disclosure_id,
                        artifact_link_id,
                        candidate.marker_semantics,
                        candidate.marker_date,
                        candidate.marker_at,
                        candidate.marker_timezone,
                        candidate.time_precision.value,
                        artifact_id,
                        candidate.extractor_contract_version,
                        claim.attempt_id,
                        marker_fp,
                    ),
                )
            marker_row = connection.execute(
                """
                SELECT material_fingerprint, marker_date, marker_at,
                       marker_timezone, time_precision
                  FROM disclosure_marker_assertions
                 WHERE disclosure_artifact_link_id=%s
                   AND marker_semantics=%s
                   AND extractor_contract_version=%s
                """,
                (
                    artifact_link_id,
                    candidate.marker_semantics,
                    candidate.extractor_contract_version,
                ),
            ).fetchone()
            if marker_row is None or marker_row[0] != marker_fp:
                raise PromotionDeterminismError(
                    "same release marker parse identity produced different material"
                )

            self.repository.terminalize_claim_in_transaction(
                connection,
                claim,
                outcome="SUCCEEDED",
            )

            return PromotionResult(
                event_occurrence_id=event_id,
                disclosure_id=disclosure_id,
                disclosure_link_id=disclosure_link_id,
                disclosure_artifact_link_id=artifact_link_id,
            )

    def promote_corroborating_representation(
        self,
        connection: Any,
        claim: Claim,
        *,
        artifact_id: UUID,
        candidate: CorroboratingRepresentationCandidate,
    ) -> PromotionResult:
        if candidate.event_type != "CPI":
            raise PromotionInvariantError("corroborating representation is not CPI")
        expected_work_key = promotion_work_key(
            PromotionFamily.CPI_CORROBORATING_REPRESENTATION_PROMOTE,
            artifact_id,
            candidate.extractor_contract_version,
        )
        if claim.work_key != expected_work_key:
            raise PromotionInvariantError(
                "claim is not corroborating-representation promotion work"
            )

        with connection.transaction():
            self._verify_input_artifact(
                connection,
                claim,
                artifact_id,
                allowed_kinds={"CPI_TABLE1_XLSX"},
                artifact_content_sha256=candidate.artifact_content_sha256,
                extractor_contract_version=candidate.extractor_contract_version,
            )

            rows = connection.execute(
                """
                SELECT e.event_occurrence_id, d.disclosure_id, l.disclosure_link_id
                  FROM core_event_occurrences e
                  JOIN event_disclosure_links l
                    ON l.event_occurrence_id = e.event_occurrence_id
                   AND l.relation_kind = 'EVENT_RELEASE'
                  JOIN event_disclosures d
                    ON d.disclosure_id = l.disclosure_id
                   AND d.source_code = 'BLS'
                  LEFT JOIN LATERAL (
                        SELECT decision_state
                          FROM interpretation_decisions
                         WHERE subject_id = l.disclosure_link_id
                         ORDER BY decision_version DESC
                         LIMIT 1
                  ) link_decision ON TRUE
                 WHERE e.event_type='CPI'
                   AND e.reference_month=%s
                   AND COALESCE(link_decision.decision_state, 'VALID')='VALID'
                """,
                (candidate.reference_month,),
            ).fetchall()
            if len(rows) != 1:
                raise PromotionInvariantError(
                    "corroborating representation requires one valid EVENT_RELEASE"
                )

            event_id, disclosure_id, disclosure_link_id = rows[0]
            self.repository.lock_cpi_event(connection, event_id)
            existing = connection.execute(
                """
                SELECT disclosure_artifact_link_id
                  FROM event_disclosure_artifacts
                 WHERE disclosure_id=%s
                   AND artifact_id=%s
                   AND relation_kind='CORROBORATING_REPRESENTATION'
                """,
                (disclosure_id, artifact_id),
            ).fetchone()
            if existing is None:
                artifact_link_id = _stable_uuid(
                    "corroborating-artifact-link",
                    disclosure_id,
                    artifact_id,
                    DisclosureArtifactRelationKind.CORROBORATING_REPRESENTATION.value,
                )
                self._ensure_subject(
                    connection,
                    artifact_link_id,
                    "DISCLOSURE_ARTIFACT_LINK",
                )
                connection.execute(
                    """
                    INSERT INTO event_disclosure_artifacts (
                        disclosure_artifact_link_id, disclosure_id, artifact_id,
                        relation_kind, accepted_by_attempt_id, accepted_at
                    ) VALUES (
                        %s, %s, %s, 'CORROBORATING_REPRESENTATION',
                        %s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (disclosure_id, artifact_id, relation_kind)
                    DO NOTHING
                    """,
                    (
                        artifact_link_id,
                        disclosure_id,
                        artifact_id,
                        claim.attempt_id,
                    ),
                )
                existing = connection.execute(
                    """
                    SELECT disclosure_artifact_link_id
                      FROM event_disclosure_artifacts
                     WHERE disclosure_id=%s
                       AND artifact_id=%s
                       AND relation_kind='CORROBORATING_REPRESENTATION'
                    """,
                    (disclosure_id, artifact_id),
                ).fetchone()
            if existing is None:
                raise PromotionInvariantError(
                    "corroborating representation relation was not established"
                )
            artifact_link_id = existing[0]

            self.repository.terminalize_claim_in_transaction(
                connection,
                claim,
                outcome="SUCCEEDED",
            )
            return PromotionResult(
                event_occurrence_id=event_id,
                disclosure_id=disclosure_id,
                disclosure_link_id=disclosure_link_id,
                disclosure_artifact_link_id=artifact_link_id,
            )

    def _current_valid_observation_topology(
        self,
        connection: Any,
        *,
        artifact_id: UUID,
        reference_month,
    ) -> tuple[UUID, UUID, UUID, UUID]:
        rows = connection.execute(
            """
            SELECT
                e.event_occurrence_id,
                d.disclosure_id,
                l.disclosure_link_id,
                da.disclosure_artifact_link_id
              FROM source_artifacts a
              JOIN event_disclosure_artifacts da
                ON da.artifact_id = a.artifact_id
              JOIN event_disclosures d
                ON d.disclosure_id = da.disclosure_id
              JOIN event_disclosure_links l
                ON l.disclosure_id = d.disclosure_id
               AND l.relation_kind = 'EVENT_RELEASE'
              JOIN core_event_occurrences e
                ON e.event_occurrence_id = l.event_occurrence_id
              LEFT JOIN LATERAL (
                    SELECT decision_state
                      FROM interpretation_decisions
                     WHERE subject_id = l.disclosure_link_id
                     ORDER BY decision_version DESC
                     LIMIT 1
              ) link_decision ON TRUE
              LEFT JOIN LATERAL (
                    SELECT decision_state
                      FROM interpretation_decisions
                     WHERE subject_id = da.disclosure_artifact_link_id
                     ORDER BY decision_version DESC
                     LIMIT 1
              ) artifact_decision ON TRUE
             WHERE a.artifact_id=%s
               AND a.source_code='BLS'
               AND e.event_type='CPI'
               AND e.reference_month=%s
               AND da.relation_kind IN (
                    'RELEASE_REPRESENTATION',
                    'CORROBORATING_REPRESENTATION'
               )
               AND COALESCE(link_decision.decision_state, 'VALID')='VALID'
               AND COALESCE(artifact_decision.decision_state, 'VALID')='VALID'
            """,
            (artifact_id, reference_month),
        ).fetchall()
        if len(rows) != 1:
            raise PromotionInvariantError(
                "observation artifact must resolve to one valid EVENT_RELEASE topology"
            )
        return rows[0]

    def promote_observation_bundle(
        self,
        connection: Any,
        claim: Claim,
        *,
        artifact_id: UUID,
        candidate: ObservationBundleCandidate,
    ) -> PromotionResult:
        codes = [item.observation_code for item in candidate.observations]
        if len(codes) != 4 or set(codes) != _CORE4:
            raise PromotionInvariantError("Core 4 bundle must resolve exactly four semantics")
        expected_work_key = promotion_work_key(
            PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE,
            artifact_id,
            candidate.extractor_contract_version,
        )
        if claim.work_key != expected_work_key:
            raise PromotionInvariantError("claim is not observation-bundle promotion work")

        with connection.transaction():
            self._verify_input_artifact(
                connection,
                claim,
                artifact_id,
                allowed_kinds={"CPI_RELEASE_HTML", "CPI_TABLE1_XLSX"},
                artifact_content_sha256=candidate.artifact_content_sha256,
                extractor_contract_version=candidate.extractor_contract_version,
            )
            event_id, disclosure_id, disclosure_link_id, artifact_link_id = (
                self._current_valid_observation_topology(
                    connection,
                    artifact_id=artifact_id,
                    reference_month=candidate.reference_month,
                )
            )
            self.repository.lock_cpi_event(connection, event_id)

            inserted = 0
            for item in candidate.observations:
                existing_assertion = connection.execute(
                    """
                    SELECT assertion_id, material_fingerprint, assertion_state,
                           normalized_value, source_value_text, source_reason_text
                      FROM official_observation_assertions
                     WHERE disclosure_link_id=%s
                       AND disclosure_artifact_link_id=%s
                       AND observation_code=%s
                       AND extractor_contract_version=%s
                    """,
                    (
                        disclosure_link_id,
                        artifact_link_id,
                        item.observation_code,
                        candidate.extractor_contract_version,
                    ),
                ).fetchone()
                assertion_id = (
                    existing_assertion[0]
                    if existing_assertion is not None
                    else _stable_uuid(
                        "observation",
                        disclosure_link_id,
                        artifact_link_id,
                        item.observation_code,
                        candidate.extractor_contract_version,
                    )
                )
                if existing_assertion is None:
                    self._ensure_subject(
                        connection,
                        assertion_id,
                        "OFFICIAL_OBSERVATION_ASSERTION",
                    )

                connection.execute(
                    """
                    INSERT INTO official_observation_assertions (
                        assertion_id, event_occurrence_id, event_type,
                        disclosure_id, disclosure_link_id,
                        disclosure_artifact_link_id, observation_code,
                        assertion_state, normalized_value, source_value_text,
                        source_reason_text, source_code, source_artifact_id,
                        extractor_contract_version, accepted_by_attempt_id,
                        accepted_at, material_fingerprint
                    ) VALUES (
                        %s, %s, 'CPI', %s, %s, %s, %s,
                        %s, %s, %s, %s, 'BLS', %s, %s, %s,
                        CURRENT_TIMESTAMP, %s
                    )
                    ON CONFLICT (
                        disclosure_link_id,
                        disclosure_artifact_link_id,
                        observation_code,
                        extractor_contract_version
                    ) DO NOTHING
                    """,
                    (
                        assertion_id,
                        event_id,
                        disclosure_id,
                        disclosure_link_id,
                        artifact_link_id,
                        item.observation_code,
                        item.assertion_state.value,
                        item.normalized_value,
                        item.source_value_text,
                        item.source_reason_text,
                        artifact_id,
                        candidate.extractor_contract_version,
                        claim.attempt_id,
                        item.material_fingerprint,
                    ),
                )
                row = connection.execute(
                    """
                    SELECT material_fingerprint, assertion_state,
                           normalized_value, source_value_text, source_reason_text
                      FROM official_observation_assertions
                     WHERE disclosure_link_id=%s
                       AND disclosure_artifact_link_id=%s
                       AND observation_code=%s
                       AND extractor_contract_version=%s
                    """,
                    (
                        disclosure_link_id,
                        artifact_link_id,
                        item.observation_code,
                        candidate.extractor_contract_version,
                    ),
                ).fetchone()
                if row is None or row[0] != item.material_fingerprint:
                    raise PromotionDeterminismError(
                        "same observation parse identity produced different material"
                    )
                if row[1] != item.assertion_state.value:
                    raise PromotionDeterminismError(
                        "observation assertion state did not converge"
                    )
                if row[3] != item.source_value_text or row[4] != item.source_reason_text:
                    raise PromotionDeterminismError(
                        "same observation parse identity changed immutable source provenance"
                    )
                inserted += 1

            self.repository.terminalize_claim_in_transaction(
                connection,
                claim,
                outcome="SUCCEEDED",
            )
            return PromotionResult(
                event_occurrence_id=event_id,
                disclosure_id=disclosure_id,
                disclosure_link_id=disclosure_link_id,
                disclosure_artifact_link_id=artifact_link_id,
                verified_observation_count=inserted,
            )

    def quarantine_observation_work(
        self,
        connection: Any,
        claim: Claim,
        *,
        reason_code: str,
    ) -> None:
        prefix = PromotionFamily.CPI_OBSERVATION_BUNDLE_PROMOTE.value + ":"
        if not claim.work_key.startswith(prefix):
            raise PromotionInvariantError("claim is not observation-bundle promotion work")
        self.repository.terminalize_claim(
            connection,
            claim,
            outcome="QUARANTINED",
            reason_code=reason_code,
        )
