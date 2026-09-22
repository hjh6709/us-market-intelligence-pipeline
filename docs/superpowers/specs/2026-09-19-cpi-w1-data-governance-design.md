STATUS: TARGET_CANONICAL

# CPI W1 Data & Governance Design

Date: 2026-09-19
Repository baseline: main @ 83e0259f2d587a50e5b670dc521e9a9b75283442
Scope class: Architectural
Target stage: CPI W1 Internal Alpha foundation

## 1. Purpose

This specification defines the CPI W1 data and governance vertical slice for the U.S. Market Intelligence product.

It is intentionally narrower than the full product architecture. It freezes the semantics needed to implement CPI source collection, immutable evidence, event and disclosure identity, official observation assertions, interpretation governance, point-in-time reconstruction, emergency serving containment, and the selector contract consumed by later product-serving work.

The goal is not to reproduce the existing migration-009 model. The goal is to create an additive, migration-safe target that can coexist with current legacy serving while removing known semantic weaknesses such as release-time event identity, consecutive source revision ordinals, primary-marker source facts, silent latest-wins selection, and conflation of operational state with knowledge state.

## 2. Success criteria

This design is successful when an implementer can build the CPI W1 data/governance foundation without inventing any of the following during implementation:

- what a CPI event occurrence is;
- which BLS surfaces are authoritative for schedule, release, and corroboration;
- how mutable source URLs differ from stable evidence identity;
- how schedule changes, cancellations, partial releases, corrections, and replay are represented;
- how official values, explicit unavailability, unresolved state, and conflict differ;
- how parser failure differs from disagreement between valid official evidences;
- how source chronology differs from capture, acceptance, governance, and serving time;
- how current source reconstruction differs from system-known point-in-time reconstruction;
- how a wrong interpretation is corrected without rewriting immutable evidence;
- how emergency withholding differs from truth correction;
- how ingestion retries remain idempotent under duplicate scheduling, worker crashes, and ambiguous commits.

## 3. Explicit non-goals

This specification does not freeze the full platform.

Out of scope:

- FOMC stable occurrence identity and source-specific semantics;
- Employment and PCE source-specific adapters and exceptional cases;
- market-price series, company, SEC, earnings, and news data models;
- consensus-provider contracts and surprise calculations beyond the lineage seam;
- public-route aliases and SEO production routing;
- aggressive CDN caching of governed numeric data;
- billing, entitlement, ads, B2B tenancy, and paid API products;
- final production IAM, Cloud Run deployment topology, Cloud SQL HA/DR, and workforce RBAC;
- implementation of the Spring Product API and Next.js product pages.

Those concerns receive separate specifications. Nothing in those later specifications may redefine CPI evidence semantics defined here.

## 4. Current implementation boundary

Current implementation remains the existing repository behavior.

At the baseline commit:

- the repository still serves the current FastAPI/legacy data model;
- migration 009 is a normalized foundation but is not populated by the production CPI adapter;
- current CPI ingestion uses the checked-in release manifest and FRED/ALFRED-oriented legacy flow;
- current pipeline telemetry vocabulary remains a legacy executable contract;
- migrations 001 through 009 are historical and must not be rewritten.

This specification is target design, not a claim that the target already exists.

## 5. Core invariants

The implementation must preserve all of the following.

1. Core CPI event identity is the statistical occurrence, not the publication timestamp.
2. For CPI, reference_month is the first day of the reference month and is required.
3. Schedule evidence and release evidence are different domains.
4. Event and disclosure are many-to-many.
5. Source artifacts are immutable evidence where rights permit retention.
6. Artifact metadata survives raw-byte expiration or non-retention.
7. Source URL is not artifact identity.
8. A mutable current source URL is never a permanent product identity.
9. Source facts never contain a product-level PRIMARY marker.
10. Missing source evidence is not equivalent to canceled, unavailable, or not published.
11. Official value and consensus are different truth domains.
12. Parser success is not canonical acceptance.
13. Parser failure and valid-official-evidence disagreement are different states.
14. Official assertions are append-only.
15. A retry repeats execution, never business identity.
16. Identical immutable identity plus identical material converges.
17. Identical immutable identity plus different material is a determinism failure.
18. Different valid official evidences with different material are retained and produce conflict; no latest-wins rule exists.
19. Operational state is not knowledge state.
20. Evidence is immutable; interpretation may change through governed decisions.
21. Emergency serving containment changes serving, not evidence validity.
22. Backfill and replay do not create live customer side effects by default.
23. A timestamp is stored only with known semantics and precision.
24. Replay must never make old source evidence appear newly published.
25. Source reconstruction and system-known PIT are different contracts.
26. No LLM inference or fuzzy matching participates in canonical CPI extraction.
27. Official economic decimals are represented exactly; binary floating-point is not canonical storage or API representation.
28. Existing migrations 001-009 remain immutable.

## 6. Authority model

This specification is the semantic authority for CPI W1 data/governance once approved and activated through the repository authority migration.

Concern-specific authorities:

| Concern | Authority |
| --- | --- |
| CPI source acquisition and surface roles | BLS CPI Source Contract defined here |
| CPI event/disclosure semantics | this specification |
| CPI observation vocabulary | this specification plus migration-owned reference rows |
| parser/extractor semantics | versioned extractor contract conforming to this specification |
| evidence validity overrides | interpretation governance defined here |
| current source reconstruction | CPI selector contract defined here |
| system-known PIT | CPI selector contract defined here |
| emergency public withholding | economic serving-control decisions defined here |
| current implementation behavior | executable repository at the commit being inspected |
| production runtime security | separate Runtime/Security/Readiness specification |
| public API and UI semantics | separate CPI Product Serving specification |

A status ledger, ADR, test, cache, support case, marketing copy, or operational log is never CPI truth authority.

## 7. BLS CPI Source Contract v1

### 7.1 Schedule surfaces

Surface roles are contractual, not inferred from arrival time.

| Surface | Role |
| --- | --- |
| BLS CPI-specific release schedule page | AUTHORITATIVE schedule evidence |
| BLS global online calendar / ICS | FALLBACK_CORROBORATION |
| checked-in CPI release manifest | FIXTURE_ONLY |
| BLS News Release HTML | release evidence, not schedule authority |
| BLS Table 1 XLSX | observation corroboration, not schedule authority |

An authoritative CPI-specific schedule assertion is not displaced merely because an ICS artifact is captured later.

Schedule source role is derived from the versioned source contract using the evidence
artifact's `source_contract_version + artifact_contract_kind`; it is never inferred
from locator URL, capture order, or accepted_at. CPI W1 does not duplicate this role
as mutable row state. For the v1 CPI schedule HTML and global ICS surfaces,
capture-time chronology is explicitly disabled.

When the authoritative schedule surface is unavailable, an approved fallback surface may be used according to the source contract. A mismatch from FALLBACK_CORROBORATION while authoritative evidence is valid is an internal corroboration mismatch, not automatically a public schedule conflict.

Schedule changes are represented by new schedule assertions. No RESCHEDULED source state is invented.

Cancellation requires explicit authoritative evidence. Row absence, HTTP failure, or missing publication does not imply cancellation.

Explicit cancellation/nonpublication evidence is not assumed to live only in the
regular CPI schedule table. An official CPI archive entry, program-specific
exception notice, or other versioned BLS exception/archival source surface may
establish nonpublication when it explicitly names the affected reference period.
Such a surface must have its own source/extractor contract and provenance; it may not
be synthesized from a missing schedule row, HTTP 404, stale current-release page, or
collector failure.

### 7.2 Release surfaces

Preferred live extraction surface:

- BLS CPI News Release HTML, current edition.

Secondary official representation:

- same-release BLS News Release Table 1 XLSX, when captured contemporaneously and validated.

Historical reconstruction:

- dated archived News Release HTML is the primary as-released evidence;
- a later-retrieved XLSX is not assumed to preserve as-released seasonal-adjusted values;
- BLS Public Data API/current database is a latest-revised comparison source, not release-moment authority.

The current-edition URL is a mutable locator. Its content may change from one CPI release to the next.

### 7.3 Live release acceptance

HTTP success does not prove a new release exists.

A live release artifact must pass an envelope check including:

- program identity is CPI;
- reference month matches the expected occurrence;
- release heading is recognized;
- official release marker semantics are recognized;
- Table 1 semantic surface is recognized before observation extraction.

If the current URL still represents the previous release, the ingestion result is DATA_NOT_AVAILABLE / EXPECTED_RELEASE_NOT_VISIBLE. No official assertion is inserted.

### 7.4 Robot and retrieval behavior

Retrieval is schedule-driven and bounded.

The collector must not use sub-second hammering, uncontrolled fan-out, proxy rotation, or unofficial mirrors as automatic bypasses for BLS throttling or blocking.

429, 403, timeout, DNS, or source-reachability failures are operational failures. They do not manufacture economic facts.

### 7.5 Rights and retention

BLS official data/documents used by this CPI source contract are treated according to the documented source-rights assessment. Retained raw artifacts preserve source attribution. BLS branding/emblem use is not implied by data reuse.

Rights policy is operationalized through source contract metadata and legal evidence references; Git is not the legal contract authority.

## 8. Time semantics

The following clocks are distinct and must never be substituted.

| Time | Meaning |
| --- | --- |
| source_effective_* | chronology stated by the source for schedule evidence |
| official release marker | official release/embargo marker used for event analysis |
| source_artifact.captured_at | time this platform successfully captured this artifact instance |
| assertion.accepted_at | time the platform accepted a typed interpretation into durable evidence |
| interpretation decision applied_at | time a governance interpretation decision was applied |
| serving-control decision applied_at | time public-serving policy changed |
| knowledge.asOf | latest knowledge cutoff that affected the source-reconstructed representation |
| representationAsOf | latest time the public semantic representation changed |
| request time | observability only |

Source chronology selection rules:

1. Apply source-contract surface roles before comparing schedule materials. AUTHORITATIVE CPI-schedule evidence is evaluated first; FALLBACK_CORROBORATION never displaces valid authoritative evidence merely because it was captured later.
2. If all eligible schedule materials at the active authority tier agree, the schedule is resolved.
3. If different eligible materials at that tier have comparable authoritative source-effective chronology, the later applicable source chronology may supersede earlier schedule evidence.
4. If source-effective chronology is unavailable but eligible artifacts were captured live, captured_at may order live acquisition evidence only when the source contract explicitly permits it.
5. Unknown, date-only, or historical evidences that cannot be safely ordered remain in conflict rather than receiving synthetic times.
6. accepted_at controls system-known visibility, not historical source chronology.
7. A replayed old artifact keeps its original captured_at; replay accepted_at must not make it look newly published.

## 9. Physical data model

The target uses one PostgreSQL public schema for CPI W1. Schema names are not used as security boundaries.

The candidate foundation contains 19 physical tables.

### 9.1 Reference and ingestion

1. data_sources
2. ingestion_runs
3. ingestion_work_items
4. ingestion_attempts
5. source_artifacts

### 9.2 Event and disclosure

6. core_event_occurrences
7. event_schedule_assertions
8. event_disclosures
9. event_disclosure_links
10. event_disclosure_artifacts
11. disclosure_marker_assertions

### 9.3 Official observations

12. observation_definitions
13. official_observation_assertions

### 9.4 Interpretation governance

14. interpretation_subjects
15. interpretation_requests
16. interpretation_approvals
17. interpretation_decisions
18. business_audit_events

### 9.5 Runtime serving control

19. economic_serving_control_decisions

The table count is not a product goal. It is the current minimal physical model after removing generic frameworks that have no W1 problem.

## 10. Reference data

### 10.1 data_sources

Purpose: stable source vocabulary only.

Minimum fields:

- source_code primary key;
- display_name.

Reference rows are migration-owned. Runtime collectors and promoters have read-only access.

### 10.2 observation_definitions

Purpose: relational vocabulary required for DB integrity.

Minimum fields:

- observation_code primary key;
- event_type;
- canonical_unit;
- display_name.

CPI W1 observation codes:

- CPI_HEADLINE_MOM
- CPI_HEADLINE_YOY
- CPI_CORE_MOM
- CPI_CORE_YOY

Full semantic meaning such as CPI-U, seasonal adjustment, geography, and transformation belongs to the versioned data/extractor contract, not an overgrown ontology table.

## 11. Ingestion workflow model

The ingestion tables are data-ingestion-plane state. They are not a generic platform queue for billing, notifications, or unrelated async jobs.

### 11.1 ingestion_runs

Required concepts:

- run_id;
- execution_scope;
- data_domain;
- job_type;
- trigger_type;
- run_mode: LIVE, BACKFILL, REPLAY;
- trigger_idempotency_key when scheduled;
- scheduled_for for scheduled triggers;
- replay_of_run_id for REPLAY;
- source_revision;
- workload_artifact_digest;
- job_contract_version;
- config_fingerprint;
- state;
- outcome;
- created_at, started_at, finished_at.

Run state and outcome remain separate.

W1 run states:

- CREATED
- RUNNING
- TERMINAL

Allowed run transitions:

- CREATED -> RUNNING;
- CREATED -> TERMINAL only for a pre-execution NO_WORK or terminal orchestration failure;
- RUNNING -> TERMINAL;
- TERMINAL has no outgoing transition.

Run state/timestamp invariants:

- CREATED has outcome=NULL, started_at=NULL, finished_at=NULL;
- RUNNING has outcome=NULL, started_at non-null, finished_at=NULL;
- TERMINAL has outcome non-null and finished_at non-null;
- a TERMINAL run may have started_at=NULL only when it terminalized directly from CREATED.

Run outcomes:

- SUCCEEDED
- PARTIAL
- FAILED
- NO_WORK

A run may terminalize only after every existing work item is TERMINAL.

A reconciliation path must finalize stranded non-terminal runs whose existing work
items are all TERMINAL. The final outcome is derived from durable work outcomes under
the same database aggregation contract; an orchestrator restart must not leave a
completed run permanently RUNNING.

Run outcome aggregation is deterministic after executable work planning:

- a direct CREATED -> TERMINAL orchestration failure before any work item is created may use FAILED;
- otherwise NO_WORK when no work item exists or every work item is SKIPPED;
- SUCCEEDED when at least one non-SKIPPED work item exists and every non-SKIPPED outcome is SUCCEEDED or DATA_NOT_AVAILABLE;
- FAILED when every non-SKIPPED work item is FAILED;
- PARTIAL for every other terminal mix, including any QUARANTINED outcome or a mixture of successful/DATA_NOT_AVAILABLE and FAILED outcomes.

DATA_NOT_AVAILABLE therefore means the work executed safely but the expected source fact was not available; it does not by itself make the run fail.

A scheduled logical invocation converges on one run identity even when scheduler delivery is duplicated. A non-null trigger_idempotency_key is unique within data_domain + execution_scope + job_type + trigger_type.

### 11.2 ingestion_work_items

Required concepts:

- work_item_id;
- run_id;
- execution_scope;
- data_domain;
- work_key;
- input_artifact_id when the work is artifact promotion;
- state;
- outcome;
- reason_code;
- next_claim_at;
- claim_generation;
- claim_token;
- lease_until;
- created_at, updated_at.

updated_at is database-owned work-state metadata. Every successful UPDATE of an
ingestion_work_items row refreshes updated_at from the database clock; callers do not
supply or preserve an arbitrary value. It is operational metadata only and must never
be substituted for evidence accepted_at, captured_at, or source chronology.

Unique identity:

- run_id + work_key.

W1 work states:

- PENDING
- CLAIMED
- PAUSED
- TERMINAL

PAUSED is an operational containment state, not an economic-data or knowledge state.
It exists because automatic promotion work has one durable business identity across
runs. A recoverable infrastructure failure must not be made permanently unretryable
merely because an automatic retry budget was exhausted.

Allowed work behavior:

- new executable work starts PENDING;
- a claim moves PENDING -> CLAIMED and increments claim_generation;
- a worker holding the current claim may terminalize CLAIMED -> TERMINAL;
- a retryable attempt may return CLAIMED -> PENDING with next_claim_at and without changing business identity;
- that retry transition first terminalizes the current attempt with a durable
  operational failure reason, then clears claim ownership and returns the work to
  PENDING; attempt history is never rewritten;
- W1 retry scheduling uses a policy-owned relative delay against the database clock,
  not a caller-selected absolute timestamp. The initial operational policy bounds one
  retry delay to 1 second through 24 hours;
- when automatic retry policy is exhausted or explicit operator investigation is
  required, the current RUNNING attempt is terminalized FAILED and the work moves
  CLAIMED -> PAUSED with a durable canonical reason. PAUSED has no claim token, no
  lease, no next_claim_at, and is not claimable;
- PAUSED -> PENDING is allowed only through an audited operational-resume boundary.
  Resume clears the pause reason from mutable work state, schedules the work from the
  database clock, preserves all prior attempt history, and does not change
  claim_generation until the next claim;
- a parent run containing PAUSED work remains non-terminal. Reconciliation surfaces
  the pause but must not invent a terminal run outcome;
- TERMINAL/FAILED is reserved for an explicit irreversible abandonment of that exact
  work business identity. Because automatic promotion identity is globally unique for
  artifact + family + extractor, terminal FAILED cannot be used as a temporary
  dead-letter state that an orchestrator later recreates.
- an expired CLAIMED lease may be reclaimed atomically by a new executor by advancing claim_generation and attempt_number; the stale claimant is fenced and cannot terminalize the work;
- reclaim closes the previous still-RUNNING attempt as FAILED with durable
  `LEASE_EXPIRED_RECLAIM` operational reason before creating the new generation, so a
  terminal run cannot retain an indefinitely RUNNING abandoned attempt;
- claim_token is immutable within one ownership generation; lease renewal cannot silently replace the owner token;
- An active owner may renew its lease using the same work_item_id, claim_generation,
  claim_token, and RUNNING attempt. Renewal uses the database clock, cannot resurrect
  an already expired claim, cannot change ownership generation/token, and must not
  shorten an existing active lease;
- an expired-lease reclaim must advance claim_generation and use a new claim_token;
- the orchestrator may terminalize PENDING -> TERMINAL with outcome=SKIPPED before any attempt exists when the work is no longer applicable;
- once a parent run is TERMINAL, no new work item may be attached to that run;
- TERMINAL has no outgoing transition.

Work state/timestamp/claim invariants:

- PENDING has outcome=NULL, reason_code=NULL, claim_token=NULL, lease_until=NULL;
- CLAIMED has outcome=NULL, reason_code=NULL, claim_token non-null, lease_until non-null;
- PAUSED has outcome=NULL, a non-empty canonical reason_code, claim_token=NULL,
  lease_until=NULL, and next_claim_at=NULL;
- TERMINAL has outcome non-null, claim_token=NULL, lease_until=NULL;
- SKIPPED is valid only for a PENDING -> TERMINAL orchestrator transition with no execution attempt;
- DATA_NOT_AVAILABLE, QUARANTINED, FAILED, and SUCCEEDED require an execution attempt.
- for an executed terminal work item, the terminal work outcome must match the current generation's terminal attempt outcome;
- SKIPPED requires that no execution attempt exists for the work item.

SKIP LOCKED or equivalent claim SQL is only a queue primitive. It does not define business-data consistency.

Work outcomes:

- SUCCEEDED
- FAILED
- QUARANTINED
- DATA_NOT_AVAILABLE
- SKIPPED

QUARANTINED means execution completed but platform interpretation could not safely be accepted. It is not the same as official-evidence CONFLICT.

Abnormal terminal work outcomes require a durable reason_code. For work items,
FAILED, QUARANTINED, DATA_NOT_AVAILABLE, and SKIPPED require a non-empty canonical
uppercase reason token. SUCCEEDED must not carry a reason_code. PENDING and CLAIMED
work items do not carry stale terminal reasons; retry-attempt diagnostics remain in
the immutable attempt history.

### 11.3 ingestion_attempts

Required concepts:

- attempt_id;
- work_item_id;
- execution_scope;
- data_domain;
- attempt_number;
- state;
- outcome;
- reason_code;
- started_at;
- finished_at.

Unique identity:

- work_item_id + attempt_number.

W1 attempt states:

- RUNNING
- TERMINAL

Attempt outcomes when TERMINAL:

- SUCCEEDED
- FAILED
- QUARANTINED
- DATA_NOT_AVAILABLE

Attempt state/timestamp invariants:

- RUNNING has outcome=NULL and finished_at=NULL;
- TERMINAL has outcome non-null and finished_at non-null.

SKIPPED is a work-level terminal outcome and does not require manufacturing an execution attempt when no execution occurred.

For attempts, FAILED, QUARANTINED, and DATA_NOT_AVAILABLE require a non-empty
canonical uppercase reason token. SUCCEEDED must not carry a reason_code, and RUNNING
attempts do not carry a terminal reason. The reason vocabulary is a bounded
application/extractor contract, while the database enforces token shape and
outcome/reason consistency.

claim_generation and attempt_number advance together when a new execution ownership generation starts. An attempt row may be created only while its parent work item is CLAIMED, and its attempt_number must equal that work item's current claim_generation. This is database-enforced so a caller cannot manufacture an execution attempt without ownership or attach an attempt to a stale generation. A stale attempt can finish logging locally, but it cannot mutate work state or canonical evidence after losing the current claim generation. No business truth depends on attempt number.

Run, work-item, and attempt identity/lineage fields are immutable after insert.
State-machine updates may change only the explicitly mutable operational state fields;
they may not rewrite run identity, work business identity, artifact input lineage, or
attempt generation history.

### 11.4 Scope and domain integrity

execution_scope answers who may execute a workflow.

data_domain answers which data world the workflow belongs to.

Both are propagated through run, work, attempt, and artifact lineage.

Child rows must use composite foreign keys that prevent cross-scope and cross-domain attachment.

A promotion work item with input_artifact_id must reference an artifact in the same data_domain.

## 12. Source artifacts

source_artifacts preserves acquisition evidence.

Required concepts:

- artifact_id;
- data_domain;
- source_code;
- artifact_contract_kind;
- source_contract_version;
- locator_key;
- retrieval_url when credential-free and useful for forensic provenance;
- content_sha256;
- content_type;
- captured_at;
- created_by_attempt_id;
- created_by_execution_scope;
- content_state;
- storage_uri when retained;
- storage_generation when retained on a versioned object store;
- created_at.

A source_artifact row represents one successful capture event, not a globally deduplicated content object. Its durable idempotency identity is scoped to the collector attempt and captured locator/content, for example created_by_attempt_id + locator_key + content_sha256. Repeated captures by different attempts may therefore create distinct artifact rows even when bytes are identical.

Within that same capture identity, an idempotent metadata retry must match all immutable
capture metadata: source, artifact/source contract kinds and versions, locator/URL,
content hash/type, captured_at, retention state, and retained-object identity. The
original captured_at and object generation are preserved; a retry may not silently
reinterpret the capture.

This is intentional: captured_at must remain the time of that specific successful capture and must not become an imprecise "first observed" timestamp caused by cross-attempt deduplication. Semantic selectors already converge identical material from multiple artifacts, so CPI W1 does not need global artifact-row deduplication.

For W1, raw-object storage does not deduplicate across distinct source_artifact rows. Each retained artifact receives its own immutable physical object identity, with a key that incorporates durable artifact/attempt identity and the content hash. This deliberately spends a small amount of storage to keep capture provenance, retention, deletion, and forensic ownership independent.

content_sha256 remains the byte-integrity identity and may be used for comparison, but two artifact rows with the same hash do not share a deletable physical object in W1.

retrieval_url is forensic provenance only. It is not automatically product-safe or stable.

W1 content states:

- RETAINED
- NOT_RETAINED
- DELETED_BY_POLICY

Metadata survives raw-content deletion.

The collector may create evidence but does not own evidence-deletion authority.

Object storage and PostgreSQL are not one transaction. The safe write direction is:

1. fetch bytes;
2. compute hash;
3. create object conditionally;
4. verify immutable physical version/generation;
5. insert source_artifact metadata.

An orphan object is recoverable operational debris. A RETAINED DB row pointing to missing or different bytes is an integrity violation.

## 13. CPI event occurrence identity

core_event_occurrences represents the statistical occurrence.

Required concepts:

- event_occurrence_id UUID;
- event_type;
- reference_month;
- created_by_attempt_id;
- created_at.

For CPI:

- event_type = CPI;
- reference_month is required;
- reference_month is the first calendar day of the statistical reference month.

A regular monthly CPI occurrence is unique by event_type + reference_month.

Release date/time is not part of event identity.

## 14. Schedule assertions

event_schedule_assertions is append-only evidence.

Stored source states:

- SCHEDULED
- DATE_PENDING
- CANCELED

RESCHEDULED is not stored. A reschedule is multiple schedule assertions.

Required concepts:

- schedule_assertion_id;
- event_occurrence_id;
- schedule_status;
- scheduled_date;
- scheduled_at;
- schedule_timezone;
- time_precision;
- source_effective_date;
- source_effective_at;
- source_effective_precision;
- source_code;
- source_artifact_id;
- extractor_contract_version;
- accepted_by_attempt_id;
- accepted_at;
- material_fingerprint.

No schedule assertion row means unresolved, not DATE_PENDING.

Synthetic midnight is forbidden.

Schedule field combinations are closed, not best-effort:

- A SCHEDULED assertion has exactly one of two precision forms:
  - EXACT: scheduled_date, scheduled_at, schedule_timezone are present;
  - DATE_ONLY: scheduled_date and schedule_timezone are present, scheduled_at is absent.
- DATE_PENDING carries no scheduled_date, scheduled_at, schedule_timezone, or
  time_precision. It is an explicit source assertion that a date is pending.
- CANCELED carries no scheduled_date, scheduled_at, schedule_timezone, or
  time_precision. Any previously announced date remains preserved in the earlier
  SCHEDULED assertion; cancellation does not rewrite or duplicate that schedule.
- source_effective_precision follows the same precision discipline: EXACT requires
  source_effective_date and source_effective_at; DATE_ONLY requires
  source_effective_date and no source_effective_at; NULL precision requires both
  source-effective fields to be NULL.

Parse identity prevents the same artifact/event/extractor contract from producing different rows silently.

Schedule assertions are governable interpretation subjects.

## 15. Disclosure identity

event_disclosures represents official public disclosure occurrences.

Required concepts:

- disclosure_id;
- source_code;
- canonical_disclosure_key;
- disclosure_kind;
- established_by_attempt_id;
- created_at.

For CPI W1 the relevant kind is DATA_RELEASE.

canonical_disclosure_key is a platform-owned stable identity produced by the versioned disclosure-identity contract. It is not represented as a source-native identifier unless the source actually supplies one.

The artifact that established a disclosure is represented only through event_disclosure_artifacts. The disclosure row does not duplicate an established_by_artifact_id foreign key. When a disclosure is first established from an artifact, the disclosure row and its initial disclosure-artifact relation are inserted in the same promotion transaction.

Release timestamp is not disclosure identity.

## 16. Event-disclosure relations

event_disclosure_links is append-only topology evidence.

Required concepts:

- disclosure_link_id UUID primary key;
- event_occurrence_id;
- disclosure_id;
- relation_kind;
- accepted_by_attempt_id;
- accepted_at;
- created_at.

accepted_at is the platform knowledge time for this topology interpretation. created_at is storage metadata and must not be substituted for PIT visibility.

For every W1 governable evidence type, `accepted_at` is assigned by the guarded
promotion transaction at the moment the platform accepts that interpretation. It is
not a parser field, source timestamp, replay timestamp, or caller-selectable value.
Promotion APIs must not accept arbitrary `accepted_at` input. Backfill/replay may
accept old source evidence later, but its system-known visibility begins at the real
acceptance time; replay never backdates platform knowledge.

The database insert boundary overwrites accepted_at from the PostgreSQL transaction
clock for governable evidence rows. This is defense in depth against a privileged
application caller accidentally or deliberately supplying a backdated/future
knowledge timestamp. Rows accepted in one promotion transaction therefore share the
same transaction-time knowledge boundary.

CPI W1 relation kinds:

- EVENT_RELEASE
- SUPPLEMENTAL_DISCLOSURE

Unique material identity includes event_occurrence_id + disclosure_id + relation_kind.

Only EVENT_RELEASE participates in CPI release-anchor selection.

A later publication containing information about a canceled CPI occurrence may be SUPPLEMENTAL_DISCLOSURE without becoming that event's release.

Each link is a governable interpretation subject.

## 17. Disclosure-artifact relations

event_disclosure_artifacts links one disclosure to one or more official representations.

Required concepts:

- disclosure_artifact_link_id UUID primary key;
- disclosure_id;
- artifact_id;
- relation_kind;
- accepted_by_attempt_id;
- accepted_at;
- created_at.

Unique material identity includes disclosure_id + artifact_id + relation_kind.

accepted_at is the platform knowledge time for this representation relation.

Initial relation kinds:

- RELEASE_REPRESENTATION
- CORROBORATING_REPRESENTATION
- CORRECTION_NOTICE

CORRECTION_NOTICE is used only when the official notice is actually scoped to that disclosure. A BLS notice affecting an unrelated database series, geography, or representation remains source evidence but must not be linked to a CPI disclosure merely because it is a CPI-program notice.

Different official representations may have different bytes while yielding the same semantic material.

A valid official disagreement is evidence conflict, not parser failure.

Each link is a governable interpretation subject.

## 18. Disclosure marker assertions

disclosure_marker_assertions records official analysis markers such as release or embargo-lift time.

Required concepts:

- marker_assertion_id;
- disclosure_id;
- disclosure_artifact_link_id;
- marker_semantics;
- marker_date;
- marker_at;
- marker_timezone;
- time_precision;
- source_code;
- source_artifact_id;
- extractor_contract_version;
- accepted_by_attempt_id;
- accepted_at;
- material_fingerprint.

The marker must pin the exact disclosure-artifact relation from which it was interpreted. DB integrity or promotion validation must ensure disclosure_id, disclosure_artifact_link_id, and source_artifact_id describe one consistent provenance chain.

The marker is not artifact capture time.

CPI W1 uses the official release/embargo marker as the analysis anchor when exact evidence exists.

For the BLS CPI release-envelope contract v1, the source phrase "embargoed until"
is normalized to marker_semantics = EMBARGO_LIFT. Promoters do not invent
RELEASE_TIME or other aliases for the same source statement. Any additional marker
semantic requires an explicit extractor-contract revision and selector review.

Marker assertions are governable interpretation subjects.

## 19. Extraction, validation, and quarantine

Canonical promotion is not:

    Artifact -> parser -> assertion

It is:

    Artifact
      -> deterministic extraction
      -> typed candidate
      -> structural validation
      -> semantic mapping validation
      -> domain-contract validation
      -> permitted official corroboration
      -> canonical promotion

Blocking interpretation failures include:

- required semantic heading missing;
- ambiguous row or column mapping;
- reference-period mismatch;
- unit mismatch;
- source text and normalized-decimal mismatch;
- required observation unresolved for the artifact contract;
- same parse identity producing different material.

A disagreement between two successfully validated official representations is not a parser failure and does not cause either evidence item to be discarded. Both evidences are retained; the selector produces CONFLICT for the affected material, and operational policy may withhold serving while the disagreement is investigated.

Advisory anomalies do not automatically replace or reject official source truth.

Examples:

- unusual CPI magnitude;
- large deviation from consensus;
- later current BLS API value differs from historical as-released value;
- external provider differs.

Consensus and external providers never vote on official truth.

No fuzzy matching or LLM interpretation is allowed in the canonical extraction path.

## 20. Official observation assertions

official_observation_assertions is append-only official evidence.

Required concepts:

- assertion_id;
- event_occurrence_id;
- event_type;
- disclosure_id;
- disclosure_link_id;
- disclosure_artifact_link_id;
- observation_code;
- assertion_state;
- normalized_value;
- source_value_text;
- source_reason_text;
- source_code;
- source_artifact_id;
- extractor_contract_version;
- accepted_by_attempt_id;
- accepted_at;
- material_fingerprint.

Stored assertion states:

- VALUE
- EXPLICIT_UNAVAILABLE

UNRESOLVED and CONFLICT are selector results, not stored source assertions.

VALUE requires normalized_value.

EXPLICIT_UNAVAILABLE requires normalized_value to be null and positive source-context evidence that the value was unavailable. Punctuation alone, including a dash, is not sufficient.

CPI percent values are stored as exact decimal values with canonical unit PERCENT. For example, 2.9 percent is represented as decimal 2.9, not 0.029 and not a binary floating approximation.

DB integrity must prevent cross-event-type observation codes and source/artifact provenance mismatches.

Every official observation assertion must pin the exact event-disclosure relation and disclosure-artifact relation that support it. The pinned disclosure_link_id must resolve to the same event_occurrence_id and disclosure_id carried by the assertion. The pinned disclosure_artifact_link_id must resolve to the same disclosure_id and source_artifact_id. This prevents an observation from being attached to an unrelated disclosure or artifact merely because the high-level source is the same.

For CPI W1 Core 4 actuals, the pinned event-disclosure relation must be EVENT_RELEASE. A SUPPLEMENTAL_DISCLOSURE may preserve provenance and historical context but may not manufacture or fill the event's four canonical actuals. This prevents a later publication from silently converting a canceled or missing release into an as-released value.

A CORRECTION_NOTICE representation may yield corrected CPI Core 4 assertions only through an explicit correction extractor contract tied to the same EVENT_RELEASE disclosure. Such corrected evidence does not silently supersede prior valid material; conflicting material remains CONFLICT until the governed interpretation process resolves the platform's eligibility view.

Parse identity is separate from semantic material identity. Different artifacts may legitimately support the same material.

Each observation assertion is a governable interpretation subject.

## 21. CPI expected observation bundle

CPI W1 expects exactly four observation semantics:

- headline MoM;
- headline YoY;
- core MoM;
- core YoY.

The expected set is owned by the versioned CPI observation-set contract, not an additional DB table.

W1 EVENT_RELEASE promotion is correctness-first. For the CPI release adapter, a release bundle is accepted only when each expected observation is safely resolved as VALUE or EXPLICIT_UNAVAILABLE.

This four-observation bundle rule applies to the CPI EVENT_RELEASE contract. It does not require every SUPPLEMENTAL_DISCLOSURE or future correction representation to manufacture all four values; those flows must obey their own explicitly versioned source/extractor contract and may not silently masquerade as the event release.

The November-2025 exceptional release is a required acceptance fixture because complete source resolution can coexist with partial numeric availability.

## 22. Interpretation governance

### 22.1 Governable subjects

interpretation_subjects provides a stable governance target identity for exactly these W1 evidence types:

- SCHEDULE_ASSERTION
- EVENT_DISCLOSURE_LINK
- DISCLOSURE_ARTIFACT_LINK
- DISCLOSURE_MARKER_ASSERTION
- OFFICIAL_OBSERVATION_ASSERTION

Identity nodes such as core_event_occurrences and event_disclosures are not directly invalidated. Incorrect topology or interpretation is removed from serving through governed evidence relations.

For every W1 governable evidence row, the evidence row UUID is the interpretation_subjects.subject_id. No second governance UUID is created. subject_type disambiguates the governed evidence type and composite integrity rules bind the subject to the corresponding typed row.

### 22.2 Requests

interpretation_requests is append-only proposal evidence.

Required concepts:

- request_id;
- subject_id;
- requested_state: VALID or INVALID;
- expected_decision_version;
- reason_code;
- case_ref;
- governance_policy_version;
- proposer_subject;
- requested_at;
- expires_at.

For W1 interpretation decisions, governance policy `cpi-governance-v1` requires at least one independent APPROVE from a subject other than the proposer and no REJECT. Additional independent APPROVE records do not invalidate an otherwise valid request. A later governance-policy version may change the minimum threshold only through an explicit contract change; the proposer cannot choose the approval threshold per request.

For `cpi-governance-v1`, request validity is exactly 24 hours from requested_at. expires_at is generated under the governance policy and is not a client-selected bypass.

No mutable APPROVED/APPLIED status is required.

### 22.3 Approvals

interpretation_approvals is append-only.

Required concepts:

- approval_id;
- request_id;
- proposer_subject;
- approver_subject;
- approval_decision: APPROVE or REJECT;
- decided_at.

Self-approval by the same workforce subject is structurally denied.

A single approver may not vote twice on the same request.

Corporate IdP/HR remains authority for whether two subjects represent distinct authorized people.

Workforce subject fields are security principals, not client-authored labels. In
production, `proposer_subject`, `approver_subject`, serving-control
`actor_subject`, and activation actor identity must be derived server-side from the
authenticated corporate IdP principal using one stable immutable subject identifier.
Display names, email aliases, request-body strings, headers supplied by untrusted
clients, or caller-selected arbitrary text are not valid identity authority.

The application boundary must ignore/reject caller attempts to choose another
workforce subject. Identity normalization at the database layer is only defense in
depth; it does not replace IdP-backed person identity or authorization.

### 22.4 Effective decisions

interpretation_decisions is append-only.

Required concepts:

- interpretation_decision_id;
- subject_id;
- decision_version;
- decision_state: VALID or INVALID;
- request_id;
- applied_at.

No decision means VALID by default.

A wrong invalidation is corrected by a later VALID decision, never by deleting history.

Activation rejects a no-op request whose requested_state is already the subject's current effective state. Case closure or reaffirmation belongs in audit/case evidence rather than meaningless interpretation versions.

Only an effective decision changes selector eligibility. Requests and approvals alone do not change product truth.

For a subject with no prior decision, expected_decision_version is 0 and the first applied decision receives decision_version 1. Competing requests based on the same expected version cannot both become effective.

Application-level two-person convention is insufficient. Production mutation must use a guarded activation boundary that verifies request, policy-derived approval requirement, expiry, approvals, rejection state, expected version, decision insert, and business audit atomically. The exact DB privilege implementation belongs to the Runtime/Security specification.

## 23. Business audit

business_audit_events records material privileged business actions, not every HTTP request.

For interpretation activation, the effective decision and its audit event commit atomically.

For economic serving-control activation, the serving-control decision and its business-audit event also commit atomically. A serving-control state change without its corresponding material-action audit record is not a valid successful activation.

The audit table is append-only to normal runtime identities.

Failed activation attempts belong in security/application logs, not as successful business-audit facts.

The DB audit row is not claimed to be tamper-proof against a fully compromised migration/owner identity. Independent centralized audit retention is a runtime-readiness concern.

## 24. Economic serving-control decisions

economic_serving_control_decisions is append-only operational policy history.

Purpose: immediately withhold public serving without rewriting evidence validity.

Required concepts:

- control_decision_id;
- scope_kind;
- event_occurrence_id when event-scoped;
- expected_control_version;
- control_version;
- state: ENABLED or WITHHELD;
- reason_code;
- applied_at;
- actor_subject;
- case_ref;
- verified_knowledge_fingerprint when required for re-enable.

Initial scopes:

- CPI_DOMAIN
- EVENT_OCCURRENCE

Scope consistency is structural: CPI_DOMAIN has no event_occurrence_id; EVENT_OCCURRENCE requires one CPI event_occurrence_id.

No decision means ENABLED.

For a scope with no prior decision, expected_control_version is 0 and the first applied decision receives control_version 1. Concurrent control changes based on the same expected version cannot both become effective. A control activation that would leave the scope in the same effective state is rejected as a no-op. The activation boundary must serialize one scope's version transition and atomically write the business-audit event. The concrete locking mechanism belongs to the Runtime/Security implementation plan; no client may choose control_version arbitrarily.

applied_at is the activation transaction's recorded application time, not a claim of exact database commit timestamp.

Effective policy is deny-overrides:

- a CPI_DOMAIN WITHHELD decision cannot be bypassed by an event-level ENABLED decision;
- domain re-enable requires a later CPI_DOMAIN ENABLED decision.

Emergency containment may be faster than truth correction.

Truth correction remains the governed interpretation workflow.

For W1 Internal Alpha, an authorized operator may apply WITHHELD immediately. Re-enabling an EVENT_OCCURRENCE after a prior WITHHELD requires a non-empty case_ref and the current selector knowledge fingerprint recorded as verified_knowledge_fingerprint; the application boundary must refuse re-enable while the event remains CONFLICT or UNRESOLVED. Re-enabling CPI_DOMAIN requires a non-empty case_ref plus a verification_ref recorded in the business audit because one event fingerprint cannot prove domain-wide recovery.

The event-level selector knowledge fingerprint is a lowercase SHA-256 digest of canonical
JSON owned by the selector contract. It represents the governed knowledge state before
the serving overlay, not request time, cache state, or serving-control history. The
payload must include the selector-contract version, event occurrence identity, release
projection state, and each Core 4 observation's resolved state plus the sorted distinct
eligible semantic material fingerprints that produced that state. Duplicate provenance
that yields the same semantic material does not change the fingerprint. Request/as-of
timestamps are excluded so an unchanged knowledge state remains stable across reads.
Task 11 owns the exact canonical payload builder; Task 12 must recompute it immediately
before event re-enable and compare it to the operator-verified value.

Event-scoped promotion, interpretation activation, and event re-enable share a
transaction-scoped event advisory fence. Re-enable acquires the fence before
recomputing current knowledge and holds it through control activation. Promotion
acquires it before validating mutable governance-dependent topology and writing
event-scoped evidence. Interpretation activation acquires the same fence for every
event affected by the governed subject. This prevents check/use races between
fingerprint verification, new official evidence, and validity decisions.

These are correctness guards, not a complete production authorization model. Independent production re-enable approval, break-glass policy, and workforce role enforcement remain mandatory before Public Beta under the Runtime/Security/Readiness contract.

## 25. Selector contract

### 25.1 Knowledge modes

The system supports two semantic modes.

OFFICIAL_SOURCE_RECONSTRUCTION:

- answers what the current validated interpretation of official historical source evidence says;
- may include evidence ingested later through backfill;
- excludes evidence currently invalidated by interpretation governance.

SYSTEM_KNOWN_PIT(T):

- includes only governable evidence whose platform knowledge time is <= T;
- applies interpretation decisions with applied_at <= T;
- reconstructs what the system considered valid by the decision time;
- is for internal research/forward-validation use, not a casual public query switch.

Platform knowledge time is explicit per evidence type:

- schedule assertion -> accepted_at;
- event-disclosure link -> accepted_at;
- disclosure-artifact link -> accepted_at;
- disclosure marker assertion -> accepted_at;
- official observation assertion -> accepted_at.

Storage created_at is never substituted for this contract.

### 25.2 Current validity

For each governable subject:

- no decision -> VALID;
- latest applicable decision -> VALID or INVALID.

Current reconstruction excludes subjects whose latest decision is INVALID.

Eligibility is transitive across the provenance graph. A typed assertion is usable only when its own interpretation subject is valid and every governable relation required to justify that assertion is also visible and valid in the selected knowledge mode.

In particular:

- an official observation requires its pinned event-disclosure link and disclosure-artifact link to be visible and valid;
- a disclosure marker requires its pinned disclosure-artifact link to be visible and valid;
- invalidating a relation does not delete dependent evidence, but dependent evidence becomes ineligible until a new valid supporting relation is explicitly established and new interpretation evidence is accepted.

### 25.3 Semantic material identity

material_fingerprint is a deterministic hash of typed semantic material only. It must not include source URL, artifact id, parser version, accepted_at, capture time, operator identity, or other provenance.

For CPI W1:

- schedule material includes schedule_status, scheduled_date, scheduled_at, schedule_timezone, and time_precision; source-effective chronology remains separate ordering metadata;
- disclosure-marker material includes marker_semantics, marker_date, marker_at, marker_timezone, and time_precision;
- observation material includes observation_code, assertion_state, and normalized_value under the observation definition's canonical unit; source_value_text and source_reason_text are provenance and are excluded.

The same semantic material from different artifacts or parser versions therefore converges at selector time without erasing its independent evidence lineage.

### 25.4 Observation resolution

For one event + observation code, after validity and knowledge-mode filtering:

- zero distinct valid materials -> UNRESOLVED;
- one VALUE material -> VALUE;
- one EXPLICIT_UNAVAILABLE material -> EXPLICIT_UNAVAILABLE;
- more than one distinct valid material -> CONFLICT.

No majority vote and no latest-wins rule exists.

### 25.5 Release selection

Each selector invocation owns one PostgreSQL `REPEATABLE READ, READ ONLY` snapshot.
All evidence, governance decisions, source-role inputs, and the projection evaluation
clock used for one result are read within that snapshot. A selector must not silently
join statement-level READ COMMITTED snapshots because concurrent promotion or
invalidation could otherwise produce a knowledge result that never existed at one
database point in time. The selector rejects reuse inside an already-active caller
transaction in W1 rather than inheriting unknown isolation semantics.

Selector projection evaluation time is distinct from evidence time and request
observability metadata.

- For SYSTEM_KNOWN_PIT(T), T is both the knowledge cutoff and the release-projection
  evaluation instant. This preserves deterministic historical reconstruction.
- For OFFICIAL_SOURCE_RECONSTRUCTION, the selector captures one PostgreSQL transaction
  timestamp at the start of the selection and uses that single instant only to decide
  whether an exact/date-only schedule is before, on, or after due.
- The evaluation instant is not accepted_at, captured_at, source chronology,
  knowledge.asOf, or representationAsOf, and is not inserted into evidence.
- The evaluation instant is excluded from the knowledge fingerprint directly. If time
  passage changes the derived release projection state, the fingerprint may change
  because the semantic release state changed, not because a timestamp was hashed.

CPI release selection considers valid EVENT_RELEASE links only.

For one CPI event occurrence:
- zero distinct valid EVENT_RELEASE disclosures means there is no validated release anchor;
- exactly one distinct valid EVENT_RELEASE disclosure is the release anchor;
- more than one distinct valid EVENT_RELEASE disclosure is release-topology CONFLICT.
  The selector must not collapse multiple valid release disclosures to a boolean
  "released" state or choose one by accepted_at, created_at, capture time, or row order.

SUPPLEMENTAL_DISCLOSURE never becomes the release anchor.

Projection states:

- NOT_YET_DUE
- DUE_DATE_UNTIMED
- AWAITING_CONFIRMATION
- DISCLOSED
- NO_RELEASE_EXPECTED
- UNRESOLVED
- CONFLICT

Exact-time schedule:

- before scheduled_at -> NOT_YET_DUE;
- at/after scheduled_at without validated release -> AWAITING_CONFIRMATION.

Date-only schedule:

- before source-local scheduled date -> NOT_YET_DUE;
- on the scheduled date without validated release -> DUE_DATE_UNTIMED;
- after the scheduled date without validated release -> AWAITING_CONFIRMATION.

DUE_DATE_UNTIMED means the official date is known but the contract does not know whether the due time has passed. It must not be collapsed into NOT_YET_DUE or AWAITING_CONFIRMATION.

A canceled applicable schedule with no EVENT_RELEASE gives NO_RELEASE_EXPECTED.

If the currently selected applicable schedule is CANCELED while a valid EVENT_RELEASE also exists and no newer authoritative schedule evidence safely resolves that contradiction, the release projection is CONFLICT rather than silently choosing either cancellation or disclosure.

### 25.6 Serving and decision-eligibility overlay

Knowledge selection and consumer eligibility are separate.

OFFICIAL_SOURCE_RECONSTRUCTION and SYSTEM_KNOWN_PIT reconstruct knowledge. economic_serving_control_decisions do not rewrite those knowledge results and are not themselves part of historical source truth.

Live governed consumers then apply the effective serving-control overlay:

- public Product API/UI must obey it;
- forward/Paper decision paths must obey it before using CPI values for a live or simulated-forward decision;
- alert/notification generation must obey it before emitting a numeric CPI value;
- authorized offline research may inspect the underlying knowledge result only in an explicit diagnostic/research mode that cannot create live customer or trading side effects.

A governed live numeric observation is consumer-eligible only when all of the
following hold:
- the release projection is DISCLOSED;
- the observation resolution is VALUE;
- the effective serving-control state is ENABLED.

If release knowledge is CONFLICT, UNRESOLVED, NOT_YET_DUE, DUE_DATE_UNTIMED,
AWAITING_CONFIRMATION, or NO_RELEASE_EXPECTED, the governed numeric value is absent
even if an isolated observation assertion happens to resolve to VALUE. This is
knowledge eligibility, not a serving-control decision, and must not rewrite either
the evidence or control history.

If the knowledge result is VALUE but the effective control is WITHHELD:

- governed consumer value availability becomes WITHHELD;
- the numeric value is absent from governed consumer payloads and side effects;
- underlying evidence remains VALID unless separately governed;
- the reason is a serving/governance reason, not a fabricated source state.

A live governed consumer must not combine knowledge and control snapshots from
different database states. The canonical live path selects current knowledge and
effective serving controls inside one selector-owned REPEATABLE READ snapshot.

If a lower-level caller supplies a previously selected knowledge object for overlay,
the selector recomputes the current knowledge fingerprint inside the serving snapshot
and rejects the request if the fingerprint changed. This prevents stale VALUE
knowledge from being served after newly accepted conflicting evidence or governance
invalidation.

Live W1 governed-serving APIs must not accept a caller-selected serving-control time.
They capture the effective control cutoff from the database clock so a caller cannot
request an earlier decision point to bypass a current WITHHELD state.

Historical user-visible representation replay, if later required, must apply serving-control decisions as-of the requested representation time. That capability is not required for W1 Internal Alpha.

## 26. Representation timestamps and fingerprint

The later Product Serving contract will expose at least:

knowledge.asOf:

- cutoff of source/interpretation knowledge that affected the represented facts.

representationAsOf:

- latest semantic change to the public representation, including knowledge change, serving-control decision, public correction notice, or stable public-source-link change.

Neither is the request timestamp.

Representation identity must be derived from served semantics, not server instance, latency, trace ID, or request time.

## 27. Promotion families, idempotency, and transaction boundaries

Fetch, parse, and semantic validation occur outside short canonical promotion transactions.

Typed parser candidates are cryptographically bound to the exact source bytes by the
artifact content SHA-256. Promotion re-reads the durable source_artifacts hash and
rejects a candidate whose hash does not match the claimed input artifact. Promotion
also accepts only extractor-contract versions approved for that artifact contract kind.
This prevents cross-wiring a valid candidate from one artifact into another same-period
artifact.

One artifact may safely establish that a release occurred even when its numeric observation surface cannot yet be interpreted. CPI W1 therefore does not use one all-or-nothing promotion outcome for release envelope and observation bundle.

Two promotion families are required for the primary CPI release artifact:

1. CPI_RELEASE_ENVELOPE_PROMOTE
   - validates program identity, reference month, disclosure identity, EVENT_RELEASE topology, RELEASE_REPRESENTATION provenance, and official marker semantics;
   - may SUCCEED even when observation extraction later quarantines;
   - atomically inserts or verifies the disclosure, its EVENT_RELEASE link, the disclosure-artifact relation, and marker assertions.

2. CPI_OBSERVATION_BUNDLE_PROMOTE
   - requires a valid, visible EVENT_RELEASE relation and matching disclosure-artifact relation for the input artifact;
   - validates and promotes the four CPI W1 observation semantics atomically;

Secondary official representations require a separate topology-only family:

3. CPI_CORROBORATING_REPRESENTATION_PROMOTE
   - is used for a validated same-release secondary representation such as Table 1 XLSX;
   - requires exactly one valid BLS EVENT_RELEASE for the same CPI reference month;
   - links the artifact to that disclosure as CORROBORATING_REPRESENTATION;
   - inserts no official observation assertion;
   - may SUCCEED even if later XLSX Core 4 extraction quarantines.

The separate topology family is necessary for the same reason release-envelope and
observation promotion are split: a valid official representation relationship must
not disappear merely because its numeric surface cannot yet be interpreted safely.

Explicit official corrections use two additional families:

4. CPI_CORRECTION_NOTICE_PROMOTE
   - accepts only a source artifact parsed by an explicitly versioned correction-notice
     extractor contract;
   - requires the notice to be scoped to exactly one existing valid CPI EVENT_RELEASE
     disclosure for the same reference month;
   - links the correction artifact to that disclosure as CORRECTION_NOTICE;
   - inserts no corrected observation assertion.

5. CPI_CORRECTION_OBSERVATION_PROMOTE
   - requires a valid EVENT_RELEASE relation and the matching valid CORRECTION_NOTICE
     disclosure-artifact link for the exact input artifact;
   - accepts a non-empty unique subset of the CPI W1 Core 4 semantics, never unrelated
     observations;
   - atomically promotes exactly the corrected semantics explicitly established by the
     correction extractor contract;
   - does not manufacture unaffected Core 4 values and does not invalidate or supersede
     prior evidence automatically.

A correction artifact that cannot be safely scoped to the release is QUARANTINED and
must not be linked merely because it is a BLS/CPI-program notice. If corrected material
differs from still-valid prior material, selector output remains CONFLICT until governed
interpretation decisions change eligibility. Correction topology and corrected values
are split so a valid scoped notice can be preserved even when numeric correction
extraction remains unresolved.
   - may QUARANTINE without rolling back already accepted release-envelope evidence.

This split is a semantic boundary, not a microservice requirement. Both work families may run in the same promoter codebase and under the same economic-promoter logical writer capability.

For an official corroborating representation such as an eligible same-release XLSX, the representation-provenance step and its observation-bundle promotion are separately identifiable work. A valid conflicting representation may succeed operationally and cause selector CONFLICT; it is not converted into a parser failure.

Each promotion transaction:

1. verifies current work-item fencing/lease ownership;
2. verifies the artifact, execution scope, data domain, source contract, and extractor contract expected by that work family;
3. inserts or verifies only that family's immutable evidence;
4. detects idempotence or same-parse determinism failure;
5. terminalizes its attempt/work state as appropriate;
6. commits atomically.

The envelope transaction never inserts partial observation values. The observation-bundle transaction never manufactures missing disclosure topology.

If the worker loses the connection after COMMIT and cannot know the outcome, it does not guess. Retry resolves the outcome using durable identities and existing terminal state.

No exactly-once claim is made. The target is at-least-once execution with durable idempotency.

## 28. Collector-to-promoter handoff

Collector and promoter are distinct trust seams.

Collector:

- retrieves approved source content;
- creates raw evidence;
- cannot directly mutate canonical CPI facts.

Promoter:

- interprets validated artifacts;
- cannot manufacture missing evidence by falling back to direct internet retrieval.

Handoff uses a control-plane/orchestrator path. The collector itself does not need cross-scope mutation rights to create promoter work.

Fast path:

- artifact commit -> authenticated handoff -> orchestrator creates the required promotion-family work items.

Artifact collection does not imply parser readiness. Bounded official evidence may be
captured and retained while a candidate extractor is blocked by corpus/conformance
review. The orchestrator must consult the extractor release gate before creating
automatic canonical promotion work. A blocked parser is an operational readiness
condition, not an economic fact and not DATA_NOT_AVAILABLE.

Recovery path:

- reconciliation detects committed artifacts lacking expected promotion-family work and safely creates the missing work.

For the primary CPI release HTML, expected automatic work includes both CPI_RELEASE_ENVELOPE_PROMOTE and CPI_OBSERVATION_BUNDLE_PROMOTE.

Automatic promotion-work identity includes artifact identity + promotion-family contract + extractor contract version. Duplicate handoffs therefore converge, while a new extractor version creates an explicit replayable interpretation attempt.
The database enforces this automatic-promotion identity across runs for work with a
non-null input artifact. `run_id + work_key` remains the general work identity, but
automatic ECONOMIC_PROMOTE work additionally has one global
`input_artifact_id + work_key` identity. Two orchestrator runs may race, but they
cannot create two business work items for the same artifact/family/extractor. A losing
run may legitimately finalize NO_WORK after observing that the promotion work already
exists elsewhere.

A run may finish PARTIAL when envelope promotion succeeds but observation promotion is QUARANTINED or FAILED. Product release state and observation-resolution state remain independent axes.

## 29. Ingestion transition authority

Immutable evidence has one logical writer capability.

Operational PAUSED/resume actions are append-only audited with the authenticated
workload/workforce actor, work_item_id, reason/case context, and database-owned
occurred_at. Mutable work state alone is not sufficient forensic evidence because a
resume clears the active pause reason.

Mutable ingestion workflow state follows an explicit transition matrix.

Conceptual authorities:

| Transition | Authority |
| --- | --- |
| create run/work | ingestion_orchestrator |
| claim/heartbeat/attempt lifecycle | scoped ingestion_executor |
| retry execution-state transition | scoped ingestion_executor |
| work terminalization | scoped ingestion_executor |
| run terminal aggregation | ingestion_orchestrator |

A terminal run may not receive new work.

The Runtime/Security specification will map these logical capabilities to concrete DB roles, workload identities, RLS policies, and grants.

## 30. Legacy coexistence and migration

Migrations 001-009 are immutable historical migration evidence.

The implementation must not reinterpret existing legacy rows into new identities in place.

Rules:

- new ingestion_* objects are additive and distinct from legacy pipeline_* telemetry;
- new target tables coexist with legacy serving during transition;
- no dual-write is required until a separately planned migration step explicitly introduces it;
- existing legacy WorkItemOutcome vocabulary is not silently redefined to include QUARANTINED;
- the first target migration is additive above the historical baseline;
- exact migration file numbering and rollout sequence belong to the implementation plan;
- eventual legacy deprecation requires evidence that new adapters, selector, serving, and correction workflows are operating correctly.

## 31. Required conformance fixtures

### 31.1 Source corpus

The baseline approved CPI source corpus covers reference months 2022-01 through 2026-08, matching the design-review window available on 2026-09-19. The corpus may be extended forward without changing this semantic contract. It must cover every available release artifact in that baseline window, with explicit inclusion of:

- ordinary releases;
- negative monthly changes;
- zero values;
- annual January seasonal-revision periods;
- 2025-10 canceled release;
- 2025-11 partial numeric availability;
- post-shutdown recovery;
- current/latest release;
- any discovered same-locator/different-byte cases;
- any official correction notices relevant to the corpus.

Each corpus entry pins artifact provenance such as source URL/locator, captured_at, content hash, reference month, source-contract version, and retained object identity where applicable.

### 31.2 Parser differential replay

URL inventory is not equivalent to a golden replay corpus.
Synthetic conformance fixtures are also not official corpus materialization. Reduced,
hand-authored, or adversarial HTML fixtures may be SHA-256 pinned and replayed to
protect parser behavior, but they must live in a separately labeled conformance set
and never satisfy an official-artifact materialization gate. Only bytes captured from
the approved official locator (or an approved immutable archive of those exact bytes)
may count as materialized official corpus evidence. Each replay-required
baseline artifact must be materialized as exact bytes, pinned by SHA-256, and bound to
the extractor contract that interprets that artifact kind before the parser release
gate can be considered complete. An entry may remain REMOTE_ONLY during corpus
construction, but such an entry keeps the release gate incomplete rather than being
silently skipped as success.

Extractor contract version is entry-scoped because CPI release HTML, Table 1 XLSX,
schedule/exception surfaces, and correction evidence do not share one parser identity.


A candidate extractor version is run against the entire approved corpus.

Expected-diff approval is cryptographically scoped to the exact corpus artifact hash,
extractor-contract version, prior expected semantic digest, and new actual semantic
digest, plus a durable review reference. A broad "expected change" flag is forbidden.
Parser failures remain hard-blocking in W1 because free-text exception identity is not
a stable semantic contract.

The diff classifies:

- semantic unchanged;
- expected changed;
- unexpected changed;
- newly failed;
- newly accepted.

Expected semantic changes must be declared in the parser change review. Unexpected semantic changes block promotion.

### 31.3 Selector vectors

Canonical selector conformance vectors must include at least:

- no schedule assertion -> schedule UNRESOLVED;
- exact schedule before due -> NOT_YET_DUE;
- exact schedule after due without release -> AWAITING_CONFIRMATION;
- valid release envelope with quarantined observation bundle -> release DISCLOSED while observations remain UNRESOLVED/WITHHELD by consumer policy;
- date-only schedule on its source-local date -> DUE_DATE_UNTIMED;
- canceled schedule -> NO_RELEASE_EXPECTED;
- canceled selected schedule plus unresolved valid EVENT_RELEASE contradiction -> CONFLICT;
- valid VALUE;
- EXPLICIT_UNAVAILABLE;
- different valid materials -> CONFLICT;
- same material from multiple artifacts -> one resolved material;
- invalidated evidence excluded from current reconstruction;
- evidence visible in SYSTEM_KNOWN_PIT before its later invalidation;
- backfilled assertion absent from PIT before accepted_at;
- event-disclosure link absent from PIT before its accepted_at;
- invalidating an event-disclosure link makes observations pinned to that link ineligible without deleting them;
- invalidating a disclosure-artifact link makes markers/observations pinned to it ineligible without deleting them;
- identical semantic material from different artifacts/parser versions has the same material identity while preserving separate evidence rows;
- multiple valid EVENT_RELEASE disclosures -> release CONFLICT;
- EVENT_RELEASE versus SUPPLEMENTAL_DISCLOSURE behavior;
- SUPPLEMENTAL_DISCLOSURE cannot contribute CPI W1 Core 4 canonical actuals;
- correction representation creates conflict rather than silent supersession when prior valid material differs;
- serving-control WITHHELD overlay leaves knowledge reconstruction intact while withholding governed consumers;
- domain WITHHELD overriding event ENABLED.

Java and Python selector implementations, if both exist, must conform to the same vectors.

## 32. Failure semantics

Examples:

| Condition | Operational result | Knowledge/public implication |
| --- | --- | --- |
| provider timeout | FAILED / provider reason | no new fact; existing canonical data remains usable |
| expected live release not visible yet | DATA_NOT_AVAILABLE | AWAITING_CONFIRMATION when due |
| parser source structure unknown | QUARANTINED | no new assertion |
| semantic mapping ambiguous | QUARANTINED | no new assertion |
| two valid official representations disagree | ingestion may succeed | CONFLICT; numeric value unavailable |
| same parse identity gives different material | determinism failure | no silent overwrite |
| canonical DB unavailable | infrastructure failure | later serving layer fails closed for governed numeric values |
| official correction detected but semantics unresolved | containment/review | WITHHELD if material risk exists; no automatic latest-wins |
| backfill/replay inserts old evidence | valid historical ingest | no live customer notification side effect |

## 33. Source correction policy

Automatic source supersession is intentionally deferred.

When an explicit BLS correction is detected:

1. preserve all official evidence;
2. determine the notice scope;
3. withhold affected public serving when integrity is uncertain;
4. investigate whether the correction affects the CPI W1 observation set or only another representation/series;
5. use governed interpretation decisions for platform-interpretation errors;
6. ingest new official evidence when appropriate;
7. verify the selector;
8. re-enable serving through a new serving-control decision.

Latest artifact, latest capture, or latest parser never wins automatically.

## 34. Security boundary assumptions for this spec

This specification defines semantic mutation boundaries but does not claim full Byzantine resistance.

Assumptions:

- a fully compromised Tier-0 migration/owner identity can damage the database and is handled by the separate Runtime/Security design;
- a compromised collector must not directly mutate canonical CPI evidence;
- a compromised promoter is trusted within the economic canonical-write domain but is prevented from mutating user state, governance decisions, and unrelated data domains;
- product-serving readers do not receive raw-artifact storage credentials or ingestion lease metadata;
- governance activation and serving-control activation require guarded mutation boundaries and business audit.

## 35. Readiness boundary

Architecture freeze is not production readiness.

This specification becomes implementation-ready when approved, but Internal Alpha additionally requires evidence for:

- migration equivalence;
- runtime privilege-negative tests;
- source corpus and differential parser tests;
- selector conformance;
- data-correction rehearsal;
- source outage and DB failure runbooks;
- backup/restore verification;
- workload identity and secret isolation;
- business-audit path;
- named operational ownership.

Those readiness controls are owned by the separate Runtime/Security/Readiness specification.

## 36. Repository authority status

This document is the active `TARGET_CANONICAL` authority for CPI W1 Data & Governance semantics. Canonical target status does not imply implementation completion.

The repository authority migration must keep the following routing true:

- docs/architecture/AUTHORITY.md routes CPI W1 Data & Governance here;
- docs/architecture/platform-contract.md is superseded historical target context;
- docs/architecture/data-contracts.md is migration-009 foundation/historical context;
- docs/architecture/operations-and-deployment.md cannot define production ingestion authority;
- docs/engineering/api-contracts.md is current/legacy FastAPI compatibility only;
- docs/engineering/current-vs-target.md is status-ledger-only;
- docs/configuration/README.md cannot create target semantics;
- docs/README.md routes readers through the authority index;
- current-system.md is a verified implementation snapshot, not executable authority;
- conflicting 2026-09-10 Superpowers target specs remain visibly historical/superseded.

Executable implementation truth remains code + migrations + tests at the commit being inspected. Any future repository change that makes these routing statements false is an authority regression.

## 37. Implementation sequence after spec approval

This section states dependency order only; detailed tasks belong to the implementation plan.

1. Authority/status migration prerequisites for the CPI data-governance subproject.
2. Additive DB foundation above migration 009.
3. Reference data and source-contract fixtures.
4. BLS schedule/release collector path.
5. deterministic extraction, semantic validation, and corpus replay.
6. promoter and selector conformance.
7. interpretation governance and serving-control mutation boundaries.
8. operational reconciliation and failure-path tests.
9. legacy coexistence verification.
10. readiness handoff to the Product Serving and Runtime/Security specifications.

No Spring Product API, Next.js public page, billing, or broad platform refactor is part of this implementation plan.

## 38. Final design position

CPI W1 is modeled as an evidence-first, governed interpretation system.

Official source artifacts remain evidence.
Typed assertions remain immutable.
Source disagreement is preserved rather than silently resolved.
Platform mistakes are corrected by interpretation decisions rather than rewriting history.
Emergency serving policy is recorded separately from knowledge validity.
Point-in-time reconstruction is based on what the system had accepted and considered valid at the decision time.
The public product consumes governed projections; it does not redefine financial truth.

Complexity is introduced only where an observed W1 failure mode requires it. Generic event-sourcing frameworks, global workflow buses, LLM-based extraction, automatic latest-wins correction, and microservice decomposition are explicitly avoided.
