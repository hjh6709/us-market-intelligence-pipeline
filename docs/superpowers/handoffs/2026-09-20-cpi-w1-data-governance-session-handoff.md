# CPI W1 Data & Governance — Design Reasoning, Decision Log, and Session Handoff

Status: `EVIDENCE_ONLY / HANDOFF`

Date compiled: 2026-09-20  
Repository: `hjh6709/us-market-intelligence-pipeline`  
Working branch: `docs/cpi-w1-data-governance-2026-09-19`  
Branch HEAD at compilation: `f0d5667d806fae31d13e5121cb7f08a4dba6338a`

> This document is intentionally **not** semantic authority. The active CPI W1 semantic authority is the approved design spec, and implementation order is owned by the implementation plan. This file preserves the reviewable reasoning record: design goals, alternatives considered, rejected approaches, red-team findings, decision evolution, implementation handoff state, and resume instructions.
>
> It does **not** attempt to reproduce private model chain-of-thought verbatim. Instead, it captures the complete shareable rationale and decision history needed for another engineer or a future session to continue without reconstructing the design from scratch.

## 1. Why this document exists

The CPI W1 work was developed across repeated architecture-review passes rather than one-shot design.

The working rule throughout the design was:

- assume a hidden contradiction exists until disproved;
- treat this as a commercial service that must survive review by senior engineering, PM, data, SRE/operations, security/legal, CS, marketing, and organizational owners;
- distinguish what is implemented from what is merely designed;
- never use documentation status or a test name to promote a target into current implementation truth;
- prefer the smallest architecture that preserves correctness, evidence, governance, and operational accountability;
- avoid introducing generic frameworks where a concrete W1 failure mode does not justify them.

The result was an evidence-first CPI data/governance slice rather than a rewrite of the existing platform.

## 2. Repository and authority context

Original baseline before CPI W1 work:

`main @ 83e0259f2d587a50e5b670dc521e9a9b75283442`

At that baseline:

- the current application was Python/FastAPI/browser based;
- existing research and serving still depended on legacy tables;
- migrations `001` through `009` existed and were already part of repository history;
- migration `009_event_session_foundations.sql` introduced a normalized event/session foundation but was not the production CPI ingestion path;
- the existing CPI loader used `config/cpi_releases.json` plus FRED/ALFRED-oriented observations;
- legacy pipeline telemetry and current serving semantics remained executable contracts;
- multiple 2026-09-10 documents still described an older target using consecutive lifecycle revisions, current-primary markers, and Airflow-centric future operations.

The architecture authority migration therefore became a prerequisite instead of an optional documentation cleanup.

The intended authority direction is:

- **CURRENT_IMPLEMENTATION** — executable code, migrations, and tests at the commit being inspected;
- **TARGET_CANONICAL** — the active CPI W1 design spec for CPI data/governance semantics;
- **STATUS_LEDGER** — implementation progress only;
- **HISTORICAL_SUPERSEDED** — previous design/rationale retained for compatibility/history;
- **EVIDENCE_ONLY** — dated verification or this handoff document; never semantic authority.

## 3. Primary design documents

The current work is anchored by:

- `docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md`
- `docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md`
- `docs/architecture/AUTHORITY.md`

A future session must read those files before interpreting this handoff.

This handoff is explanatory. If it disagrees with the active spec after a later change, the spec wins.

## 4. Core problem that forced a new CPI W1 model

The old normalized foundation solved real problems but still encoded assumptions that were unsafe for the desired product:

- event lifecycle revision numbers looked like source-native truth even though many official sources do not publish such revision ordinals;
- the "current primary marker" concept mixed source evidence with product selection policy;
- a release timestamp was too close to event identity;
- old target text encouraged treating current/latest projections as semantic truth;
- historical backfill, live acquisition, and system-known point-in-time knowledge were not separated strongly enough;
- source artifact identity and product source-link identity were not sufficiently distinct;
- operational job state, economic knowledge state, and public-serving state could be collapsed too easily;
- correction handling could drift toward latest-wins behavior;
- one release could contain evidence for another occurrence, which breaks one-event/one-disclosure assumptions;
- release existence and successful observation parsing were too easy to treat as one result.

The CPI W1 redesign was therefore additive and isolated from the legacy implementation rather than an in-place reinterpretation of migration 009.

## 5. Non-negotiable semantic invariants

The design converged on the following invariants.

### Event, disclosure, and source semantics

1. CPI event identity is the statistical occurrence, not the publication timestamp.
2. CPI reference month is required and normalized to the first day of that month.
3. Schedule evidence and release evidence are different domains.
4. Event and disclosure are distinct identities.
5. Event/disclosure is many-to-many rather than forced one-to-one.
6. A later publication can be supplemental evidence for an older occurrence without becoming that occurrence's release.
7. Missing source evidence does not mean canceled, not published, or unavailable.
8. Cancellation requires explicit authoritative evidence.
9. No product-level `PRIMARY` marker is stored as an official source fact.
10. Source URL is provenance, not artifact identity and not canonical event identity.
11. A mutable current-release URL cannot be treated as a permanent event-specific public source link.

### Evidence and observation semantics

12. Raw artifacts are immutable evidence where retention rights allow.
13. Artifact metadata survives raw-content deletion or non-retention.
14. Parser success is not equivalent to canonical acceptance.
15. Parser failure differs from disagreement between two valid official evidences.
16. Valid official disagreement is preserved and resolves to `CONFLICT`; no latest-wins rule applies.
17. Official value and consensus/provider expectation are different truth domains.
18. An explicit unavailable value is different from unresolved.
19. A dash or blank cell alone is not sufficient evidence for `EXPLICIT_UNAVAILABLE`.
20. Official decimal values use exact decimal semantics; binary floating point is not canonical representation.
21. No fuzzy or LLM inference participates in canonical extraction.

### Time and PIT semantics

22. Source chronology, platform capture time, accepted knowledge time, governance application time, serving-control time, and request time are different clocks.
23. Timestamps are stored only with known semantics and precision.
24. Date-only evidence must not receive synthetic midnight.
25. Replay of old evidence must not make it appear newly published.
26. `OFFICIAL_SOURCE_RECONSTRUCTION` and `SYSTEM_KNOWN_PIT(T)` are separate read contracts.
27. Backfilled evidence may appear in current source reconstruction while remaining invisible in system-known PIT before its `accepted_at`.
28. Storage `created_at` is never substituted for knowledge time.

### Governance and serving semantics

29. Evidence is immutable; platform interpretation may change through append-only decisions.
30. Emergency withholding changes serving eligibility, not source truth.
31. Serving controls are append-only decisions, not mutable truth rows.
32. No decision means VALID for interpretation and ENABLED for serving-control defaults.
33. Governance no-op state changes should be rejected instead of manufacturing meaningless history.
34. Serving-control no-op changes should likewise be rejected.
35. Domain-wide withholding uses deny-overrides; event-level ENABLED cannot override domain WITHHELD.
36. Public/API/Paper-forward/alert consumers must obey serving controls.
37. Authorized offline diagnostics may inspect underlying knowledge only through an explicitly non-side-effecting path.

### Workflow semantics

38. At-least-once execution plus durable idempotency is the target; no exactly-once claim.
39. Retry repeats execution, never business identity.
40. Worker lease/fencing must prevent a stale claimant from mutating canonical evidence or terminalizing work.
41. `SKIP LOCKED` is a queue primitive, not a data-consistency contract.
42. Run/work/attempt state and outcome are separate.
43. Collector and promoter are distinct trust seams.
44. Promoter may not silently fetch alternate internet evidence when expected input is missing.
45. Backfill/replay does not produce live customer side effects by default.

## 6. BLS CPI source-contract reasoning

A major source-contract decision was to stop treating every official BLS surface as equivalent simply because it is official.

The design assigns roles:

- CPI-specific BLS schedule page — **AUTHORITATIVE** schedule evidence;
- BLS global calendar / ICS — **FALLBACK_CORROBORATION**;
- checked-in release manifest — **FIXTURE_ONLY**;
- CPI News Release HTML — release/disclosure evidence, not schedule authority;
- same-release Table 1 XLSX — official observation representation/corroboration.

Why this matters:

If the CPI-specific schedule page says one date and the global ICS has not yet synchronized, a naive "two official values disagree => public conflict" policy would create false product conflicts from publication-lag between BLS surfaces.

Therefore source-role is applied before chronology comparison.

The design also explicitly rejects:

- using HTTP 200 as proof that a new release exists;
- interpreting an old current-edition page after due time as "no release";
- treating 403/429/timeout as economic facts;
- proxy rotation or aggressive scraping as an automatic operational response;
- using an official provider disagreement as a vote to replace the BLS source.

## 7. Why schedule changes are assertions, not RESCHEDULED source states

The old target had lifecycle-style transition semantics.

For CPI W1, a source-native `RESCHEDULED` state is not invented.

Instead:

- original `SCHEDULED` assertion remains evidence;
- later `SCHEDULED` assertion with new source chronology is additional evidence;
- selector resolves the applicable schedule using authority tier and comparable chronology.

This preserves what the source said rather than creating a synthetic lifecycle event.

Likewise, row absence is never turned into `DATE_PENDING`; `DATE_PENDING` requires actual source evidence.

## 8. Why disclosure identity is separate from event identity

The model uses `core_event_occurrences` and `event_disclosures` separately.

Reasons:

- the same disclosure can contain evidence relevant to more than one occurrence;
- a supplemental publication should not become the release anchor for the event it mentions;
- correction notices have their own publication identity;
- source-native disclosure identifiers may not exist;
- release timestamp is not stable disclosure identity.

A platform-owned deterministic `canonical_disclosure_key` therefore identifies disclosures, while explicit relations describe how they connect to event occurrences and artifacts.

## 9. Event/disclosure relation reasoning

`event_disclosure_links` uses:

- `EVENT_RELEASE`
- `SUPPLEMENTAL_DISCLOSURE`

Only `EVENT_RELEASE` participates in CPI release-anchor selection.

This prevents a later release or supplemental table that references a prior occurrence from silently filling that occurrence's canonical Core 4 actuals.

A related hard rule is:

> CPI W1 Core 4 actual assertions must pin an `EVENT_RELEASE` relation.

Supplemental evidence remains preserved but cannot manufacture as-released actuals.

## 10. Disclosure/artifact relation reasoning

A disclosure can have multiple official representations.

The model therefore separates disclosure identity from representation artifacts using `event_disclosure_artifacts`.

Initial relation kinds:

- `RELEASE_REPRESENTATION`
- `CORROBORATING_REPRESENTATION`
- `CORRECTION_NOTICE`

Important review result:

A correction notice is linked to a CPI disclosure only when its scope actually applies to that disclosure. A generic BLS/CPI notice about another series, geography, or database representation must not be attached merely because it belongs to the CPI program.

## 11. Artifact design evolution

The artifact design changed several times during red-team review.

### Rejected: global row deduplication by source/locator/hash

Why rejected:

If the same bytes are captured by two independent attempts, global artifact-row dedupe collapses independent acquisition events and makes `captured_at` ambiguous.

Final direction:

- one `source_artifact` row represents one successful capture event;
- idempotency is scoped to the same collector attempt + locator + hash;
- another attempt may create another row with the same bytes;
- semantic selectors converge identical material later.

### Rejected: shared deletable raw object across artifact rows

Why rejected:

Retention or deletion policy for one artifact must not silently delete another capture's forensic evidence.

Final W1 direction:

- each retained artifact row owns its own immutable physical object identity;
- content hash remains byte-integrity identity;
- physical object deletion is independent per artifact row.

### Rejected: `public_source_url`

Why rejected:

A URL that is publicly accessible is not automatically safe as the permanent product link.

Final field is forensic `retrieval_url`.

Public stable-link selection belongs to product-serving projection, not raw artifact storage.

## 12. Why source-contract version and extractor-contract version are separate

Three different version domains were identified:

- `source_contract_version` — why/how these bytes were retrieved;
- `extractor_contract_version` — how those bytes were interpreted as typed evidence;
- selector contract version — how multiple valid evidences are resolved into knowledge.

Collapsing these into one generic version would make replay, parser changes, and acquisition-policy changes impossible to distinguish cleanly.

## 13. CPI Core 4 observation model

W1 canonical observation semantics are exactly:

- `CPI_HEADLINE_MOM`
- `CPI_HEADLINE_YOY`
- `CPI_CORE_MOM`
- `CPI_CORE_YOY`

The canonical release contract requires all four semantics to resolve as either:

- `VALUE`
- `EXPLICIT_UNAVAILABLE`

before the Core 4 observation bundle is promoted.

This does not mean every supplemental or correction disclosure must manufacture four values. The atomic four-observation rule belongs specifically to the CPI EVENT_RELEASE Core 4 contract.

The November 2025 exceptional release is a required fixture because full semantic resolution can coexist with partial numeric availability.

## 14. Critical design correction: split release-envelope and observation promotion

One of the last major blockers found during review was the original single-promotion model.

Problem:

A BLS release can be clearly valid as a disclosure even when the Core 4 parser cannot safely map the observation table.

If one transaction handled both release existence and observations, the system would be forced into one of two wrong outcomes:

- hide the actual release because observations quarantined; or
- partially insert observations and weaken atomicity.

Final design:

### `CPI_RELEASE_ENVELOPE_PROMOTE`

Validates and promotes:

- CPI program identity;
- expected reference month;
- disclosure identity;
- `EVENT_RELEASE` relation;
- release representation relation;
- official release/embargo marker.

It may succeed even if observation parsing later quarantines.

### `CPI_OBSERVATION_BUNDLE_PROMOTE`

Requires already-valid matching release topology and atomically promotes the four expected CPI observation semantics.

Therefore this state is valid:

- release = `DISCLOSED`
- observation bundle work = `QUARANTINED`
- observation knowledge = unresolved/unavailable to governed consumers

This split is a durable work-family boundary, not a reason to create another microservice.

## 15. Why valid official disagreement is not QUARANTINED

A semantic mapping that cannot be trusted is our interpretation failure and becomes `QUARANTINED`.

But if:

- HTML validates;
- XLSX validates;
- both are officially scoped to the same release;
- values materially disagree;

then neither evidence item should be discarded.

Both promotion jobs may succeed operationally. The selector sees multiple distinct valid semantic materials and returns `CONFLICT`.

This distinction is central:

- **QUARANTINED** — our candidate interpretation is unsafe;
- **CONFLICT** — multiple valid official evidences disagree.

## 16. Material fingerprint reasoning

The design deliberately excludes provenance from `material_fingerprint`.

A semantic fingerprint may include, depending on type:

- schedule state/date/time/timezone/precision;
- marker semantics/time/timezone/precision;
- observation code + state + canonical decimal value.

It must not include:

- artifact ID;
- URL;
- parser version;
- accepted time;
- capture time;
- operator;
- ingestion attempt.

Why:

Two independent official artifacts or parser versions that produce the same semantic fact should converge at selector time while retaining separate provenance rows.

## 17. Provenance graph and transitive eligibility

A later review found that simply storing event ID, disclosure ID, and artifact ID on an observation was insufficient.

Final observation evidence pins:

- exact `disclosure_link_id`;
- exact `disclosure_artifact_link_id`;
- matching event/disclosure/artifact identities.

This enables transitive governance.

If a relation is later invalidated:

- dependent observation rows are not deleted;
- dependent evidence becomes selector-ineligible because its supporting relation is no longer valid/visible.

The same principle applies to disclosure markers and their artifact relations.

## 18. Governance design

Governable W1 subject types are limited to:

- schedule assertion;
- event-disclosure link;
- disclosure-artifact link;
- disclosure-marker assertion;
- official-observation assertion.

Identity nodes such as event occurrences and disclosures are not directly invalidated.

Reason:

They are graph/identity structures. Serving meaning is controlled by interpretation of relations and assertions, avoiding a generic governance framework that expands to every table.

The evidence UUID itself is reused as `interpretation_subjects.subject_id`; no second governance UUID is created.

## 19. Governance request/approval/decision reasoning

A key security finding was that approval threshold cannot be caller-controlled.

Rejected design:

- request row carries arbitrary `required_approvals` supplied by proposer.

Final W1 policy:

- exactly one independent approval from someone other than proposer;
- no reject;
- policy version owns the threshold;
- request expiry is generated by governance policy;
- proposer cannot choose the threshold or bypass expiration.

Effective decisions are append-only:

- no prior decision => VALID;
- first activation expects version 0, writes version 1;
- competing activations on same expected version cannot both succeed;
- no-op decisions are rejected;
- reversing a prior wrong invalidation means a later VALID decision, never deleting history.

## 20. Serving-control design evolution

### Rejected: mutable current-state serving row

Why rejected:

If WITHHELD is later overwritten by ENABLED, CS/SRE cannot reconstruct what a user saw during the incident.

Final:

`economic_serving_control_decisions` is append-only.

Each scope has monotonic versioning with compare-and-set semantics.

Scopes:

- `CPI_DOMAIN`
- `EVENT_OCCURRENCE`

Rules:

- no decision => ENABLED;
- domain WITHHELD overrides event ENABLED;
- no-op state changes rejected;
- serving-control activation and business audit commit atomically.

## 21. Knowledge versus serving

Another important separation:

`OFFICIAL_SOURCE_RECONSTRUCTION` and `SYSTEM_KNOWN_PIT` reconstruct knowledge.

Serving controls do not rewrite those knowledge results.

A live governed consumer then applies the serving overlay.

Example:

- underlying observation knowledge = VALUE 2.9;
- emergency control = WITHHELD;
- knowledge remains VALUE 2.9;
- Product/Paper-forward/notification consumer receives no numeric value.

This is important for incident recovery and research reproducibility.

## 22. Public representation timestamps

One `knowledge.asOf` field was found insufficient.

A value may remain the same while user-visible representation changes because:

- a serving control withholds it;
- a stable archive source link appears;
- a public correction notice changes the representation.

Therefore later Product Serving design should distinguish:

- `knowledge.asOf` — economic/source/interpretation knowledge cutoff;
- `representationAsOf` — latest semantic change in the served representation.

Request timestamp is neither.

## 23. Ingestion workflow state machine

### Runs

States:

- `CREATED`
- `RUNNING`
- `TERMINAL`

Outcomes:

- `SUCCEEDED`
- `PARTIAL`
- `FAILED`
- `NO_WORK`

Allowed transitions are explicit; terminal has no outgoing transition.

### Work items

States:

- `PENDING`
- `CLAIMED`
- `TERMINAL`

Outcomes:

- `SUCCEEDED`
- `FAILED`
- `QUARANTINED`
- `DATA_NOT_AVAILABLE`
- `SKIPPED`

Retryable work may return from CLAIMED to PENDING without changing business identity.

### Attempts

States:

- `RUNNING`
- `TERMINAL`

Attempt terminal outcomes do not include synthetic SKIPPED attempts if no execution actually occurred.

`claim_generation` and attempt generation fence stale workers.

## 24. Separate collect and promote runs/scopes

A hidden inconsistency was found when run-level `execution_scope` was introduced.

If one run has one scope, collector and promoter work cannot safely coexist inside the same run under different authorities.

Final direction:

- collect runs are scoped to collection;
- promotion runs are scoped to promotion;
- lineage is recovered through the artifact and input-artifact relationship;
- no generic mixed-authority run is needed.

This preserves the trust boundary without inventing a platform-wide orchestration model.

## 25. Database schema choice

An intermediate idea used `app.ingestion_*`.

This was rejected because the prior target explicitly aimed to avoid schema-per-domain complexity in W1, and schema names were starting to be treated as security boundaries.

Final W1 direction:

- use PostgreSQL `public` schema;
- security comes later through object ownership, capability roles, grants, RLS where justified, and workload identity;
- one schema does not imply one security boundary.

## 26. Physical model

The reviewed candidate foundation contains 19 physical tables:

Reference / ingestion:

1. `data_sources`
2. `ingestion_runs`
3. `ingestion_work_items`
4. `ingestion_attempts`
5. `source_artifacts`

Event / disclosure:

6. `core_event_occurrences`
7. `event_schedule_assertions`
8. `event_disclosures`
9. `event_disclosure_links`
10. `event_disclosure_artifacts`
11. `disclosure_marker_assertions`

Official observations:

12. `observation_definitions`
13. `official_observation_assertions`

Interpretation governance:

14. `interpretation_subjects`
15. `interpretation_requests`
16. `interpretation_approvals`
17. `interpretation_decisions`
18. `business_audit_events`

Serving control:

19. `economic_serving_control_decisions`

The number 19 is not a KPI. It is the current result of repeatedly removing tables that did not solve a concrete W1 problem.

## 27. Explicitly rejected architectural alternatives

### One giant system-architecture spec

Rejected because the full product was not actually frozen. Only CPI W1 and minimal platform seams were mature enough.

The architecture was decomposed into separate subprojects:

- CPI W1 Data & Governance;
- CPI W1 Product Serving;
- Platform Runtime / Security / Readiness.

### Reusing migration-009 semantics as the final model

Rejected because revision ordinals, current-primary markers, and lifecycle assumptions would leak historical implementation choices into the new source contract.

### Rewriting migrations 001-009

Rejected because already-applied migration history is evidence and must remain immutable.

### Global "event status"

Rejected because schedule, release, observation resolution, interpretation, and serving are independent axes.

### Silent latest-wins correction

Rejected because later capture time is not necessarily source chronology or truth priority.

### Majority vote between official representations

Rejected because source truth is not an election; disagreement is evidence.

### Generic platform message bus now

Rejected by YAGNI. The design preserves clean seams so one can be introduced later when multiple real consumers justify it.

### Microservices for collector/promoter

Rejected. Logical trust boundary is required; physical service split is not required yet.

### One mutable serving-control row

Rejected because it destroys incident history.

### One promotion result for envelope + Core 4

Rejected because release existence can be known while observation extraction remains unsafe.

### LLM/fuzzy canonical extraction

Rejected because canonical financial facts require deterministic auditable mapping.

## 28. Product/API/UI implications already frozen at the seam

The CPI Data/Governance spec does not implement the Product API, but later product work must preserve these semantics:

- release state and observation availability are separate;
- `WITHHELD` numeric values are absent from API/UI/meta/SEO/notifications/cache;
- null is never zero;
- exact official decimals are serialized without float drift;
- public current historical view is source reconstruction, not arbitrary user-selectable PIT;
- internal forward/Paper validation uses system-known PIT;
- DATE_ONLY schedule needs a distinct state such as `DUE_DATE_UNTIMED`;
- a known bad-data resource can still return HTTP 200 for the resource while withholding the financial value; HTTP server health and data-health are different concepts;
- canonical-authority outage for W1 numeric truth must fail closed rather than serving an unlabelled stale number.

## 29. Runtime/security assumptions deferred to a separate spec

This CPI W1 spec defines mutation and trust seams but does not claim production IAM is implemented.

Target concepts established for the later Runtime/Security specification include:

- runtime workloads never own DB objects;
- permanent object owner is NOLOGIN;
- migration identity is separate from deploy identity and runtime identity;
- collector cannot write canonical facts;
- promoter cannot mutate user state/governance decisions;
- product readers do not receive raw artifact storage credentials;
- governance activation uses a narrow guarded boundary;
- production/nonproduction identities and data must be separated;
- workforce identity differs from workload identity;
- break-glass is separate, time-limited, reasoned, audited, and reviewed;
- build identity differs from deploy identity;
- restore testing is required; backup existence alone is not recoverability.

These are target constraints, not current implementation claims.

## 30. Readiness model

A major organizational principle was:

> Mergeable != Deployable != Launchable.

The design distinguishes:

- architecture freeze;
- implementation;
- verification;
- Internal Alpha readiness;
- Public Beta readiness;
- Paid launch readiness.

A feature may be code-complete and still not launchable.

Readiness evidence is tied to exact artifact/schema/environment/configuration rather than a free-floating green checklist.

## 31. PM / Marketing / CS implications

The design intentionally prevents product claims from outrunning data truth.

Marketing cannot:

- claim a value is current if serving control withholds it;
- expose a mutable current BLS URL as stable historical proof;
- present source conflict as a single canonical number;
- treat latest-revised data as historical as-released data.

CS needs to be able to explain:

- release confirmed but values still under review;
- value temporarily withheld without source history being deleted;
- a correction changed the platform interpretation;
- why a user saw a different state at an earlier time.

This drove append-only serving control and separate `knowledge.asOf` / `representationAsOf` concepts.

## 32. Security/legal implications

Important legal/security choices:

- raw bytes are retained only under approved source-rights policy;
- attribution is preserved;
- product deep-link policy is distinct from retrieval provenance;
- one artifact's retention/deletion must not silently alter another artifact's forensic history;
- canonical parser does not execute active content;
- XLSX handling must use bounded ZIP inspection, read-only/data-only parsing, and reject unsafe workbook features;
- HTML extraction must not execute JS or fetch subresources;
- redirects/hosts/sizes/timeouts are bounded by source contract;
- legal rights status is a launch gate rather than an unverified architectural assumption.

## 33. Authority-cleanup reasoning

The repository originally contained multiple files that appeared active enough to create ambiguity.

The authority migration therefore reclassified:

- `docs/architecture/platform-contract.md`
- `docs/architecture/data-contracts.md`
- `docs/architecture/operations-and-deployment.md`
- old 2026-09-10 Superpowers target specs

as historical/superseded for CPI W1 target semantics.

`docs/engineering/api-contracts.md` remains current FastAPI compatibility/current implementation, not future Product API authority.

`docs/engineering/current-vs-target.md` is a status ledger and cannot create target semantics.

`docs/architecture/current-system.md` is a verified snapshot, not eternal implementation authority.

A later audit found that the new authority index said the CPI spec was `TARGET_CANONICAL` while the spec still said `PROPOSED ... AWAITING USER REVIEW`. That contradiction was corrected in later commits, including:

- `310bb45e3f3c` — reconcile CPI W1 canonical authority status;
- `9b98b823bc79` — remove remaining authority wording drift.

## 34. Implementation plan structure

The implementation plan was intentionally divided into small reviewable tasks rather than "implement the platform".

Planned sequence:

1. repository authority/status migration;
2. ingestion/artifact/governance-subject foundation;
3. event/schedule/disclosure/marker evidence;
4. Core 4 official observation assertions;
5. interpretation governance and serving-control history;
6. Python W1 semantic contracts;
7. safe artifact storage and BLS retrieval;
8. CPI schedule parsing;
9. release envelope + Core 4 extractor;
10. repository claim fencing + split promotion;
11. selector/PIT + serving overlay;
12. governance application boundary;
13. golden corpus differential replay;
14. controlled collector/orchestrator;
15. legacy coexistence + migration-equivalence verification;
16. final verification evidence package.

Every task follows RED -> minimal implementation -> GREEN -> regression -> self-review -> commit.

## 35. Implementation progress visible in Git history at this handoff

Recent branch history includes:

- `0578885899ed` — add CPI W1 data and governance design;
- `149954a90de9` — tighten design after self-review;
- `8bb1fcd99769` — harden capture/serving/release semantics;
- `d5d880d6b2d6` — close provenance and PIT eligibility gaps;
- `372cc7cc592d` — freeze workflow state transitions;
- `8d700ba7a58f` — separate knowledge, serving, correction semantics;
- `72c598ea23d3` — split release and observation promotion outcomes;
- `cddceb3dc270` — add implementation plan;
- `47ee40fef253` — self-review implementation plan;
- `11dbcfca77eb` — establish architecture authority routing;
- `9ee864b5626b` — harden workflow and governance semantics;
- `310bb45e3f3c` — reconcile canonical authority status;
- `9b98b823bc79` — remove remaining authority wording drift;
- `9342e6bbd528` — add CPI W1 ingestion evidence foundation;
- `ce70f262f513` — preserve artifact deletion metadata;
- `414978bc9d6d` — restore migration dollar quoting;
- `1dbd954ce8a9` — correct migration delimiter;
- `f0d5667d806f` — add CPI W1 event disclosure evidence model.

At compilation time the branch HEAD is `f0d5667d806fae31d13e5121cb7f08a4dba6338a`.

This means implementation has progressed beyond the original Task 1 handoff. A future session must inspect the actual branch and plan checklist before assuming the next task number.

## 36. Specific implementation risks to keep red-teaming

These remain the most important classes of hidden errors to look for while implementing later tasks.

### Migration/DB

- composite FK accidentally allows cross-scope or cross-domain attachment;
- retention-state CHECK allows RETAINED without verifiable object identity;
- append-only evidence table has an UPDATE/DELETE path through runtime roles;
- provenance graph can point to mismatched event/disclosure/artifact;
- subject registry can contain a subject with no corresponding typed evidence;
- typed evidence can exist without its required interpretation subject;
- unique constraint accidentally collapses distinct capture events;
- run terminal state still allows adding work;
- retry/reclaim path increments attempt but not claim generation atomically.

### Parser/source

- current-release page accepted without matching expected reference month;
- HTML/XLSX table mapping relies on fixed cell positions;
- dash interpreted as unavailable without source context;
- old current-page content after due treated as no release;
- 403/429 treated as publication state;
- XLSX formula/macro/external-link handling unsafe;
- parser versions change semantic output without corpus differential block.

### Selector/PIT

- accepted time accidentally used as source chronology;
- created_at used as PIT knowledge time;
- invalidated relation does not invalidate dependent assertion eligibility;
- repeated same material creates false conflict;
- conflicting distinct materials collapse via latest-wins;
- supplemental disclosure becomes release anchor;
- correction notice silently supersedes earlier material;
- serving-control history contaminates source-reconstruction truth.

### Governance

- proposer controls approval threshold or expiry;
- self-approval possible through equivalent workforce identity representation;
- competing expected-version requests both activate;
- no-op decisions create fake history;
- serving-control activation is not atomically audited;
- domain WITHHELD can be bypassed by event ENABLED.

### Product seam

- release state inferred from observation availability;
- value null rendered as zero;
- withheld value leaks through SEO/meta/cache/notification;
- mutable current URL exposed as stable source citation;
- stale cache survives correction/withholding;
- HTTP health conflated with financial-data health.

### Operations

- reconciliation creates duplicate promotion business identities;
- collector directly mutates canonical facts;
- promoter fetches from internet as a fallback;
- backfill triggers notifications or forward decisions;
- object write succeeds but DB write fails and orphan cleanup is missing;
- DB says RETAINED but object is absent or wrong generation.

## 37. What must not be changed casually

A future session should stop and explicitly review before changing any of these:

- migrations `001` through `009`;
- CPI event identity;
- separation of event occurrence and disclosure;
- split envelope/observation promotion;
- Core 4 atomic observation-bundle rule;
- EVENT_RELEASE-only Core 4 actual rule;
- source reconstruction vs system-known PIT separation;
- append-only interpretation decisions;
- append-only serving-control decisions;
- evidence preservation under conflict;
- no latest-wins rule;
- no LLM/fuzzy canonical extraction;
- collector/promoter trust separation;
- legacy FastAPI/current serving coexistence until its own Product Serving migration.

## 38. Current repository coexistence principle

Do not confuse new W1 implementation with replacement of the existing product.

The intended coexistence is:

- legacy `src/cpi_ingestion.py` continues to exist for current callers;
- legacy `pipeline_*` telemetry continues with its existing vocabulary;
- migration 009 remains historical executable foundation;
- migration 010+ adds CPI W1 objects;
- current FastAPI continues to serve the existing model until a separate Product Serving migration;
- target CPI W1 modules are not allowed to silently reinterpret legacy enum/status meanings.

## 39. Resume procedure for another ChatGPT/Codex session

At the beginning of a new session, do not paste the entire old chat.

Use this procedure:

1. Open the repository/branch:
   `docs/cpi-w1-data-governance-2026-09-19`
2. Read, in order:
   - `docs/architecture/AUTHORITY.md`
   - `docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md`
   - `docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md`
   - this handoff document
3. Inspect branch HEAD and recent commits.
4. Inspect the implementation-plan task checklist and determine the actual next incomplete task.
5. Run relevant existing tests before modifying the next subsystem.
6. Continue red-team review against the risk list above.
7. If the implementation requires changing a frozen semantic invariant, stop implementation and update/review the spec first.

## 40. Recommended new-session prompt

```text
이전 세션에서 진행하던 us-market-intelligence-pipeline CPI W1 Data & Governance 작업을 이어서 진행한다.

Repository:
https://github.com/hjh6709/us-market-intelligence-pipeline

작업 브랜치:
docs/cpi-w1-data-governance-2026-09-19

반드시 먼저 저장소 현재 HEAD와 git history를 직접 확인하고 아래 문서를 처음부터 읽어.

1. docs/architecture/AUTHORITY.md
2. docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md
3. docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md
4. docs/superpowers/handoffs/2026-09-20-cpi-w1-data-governance-session-handoff.md

4번 문서는 semantic authority가 아니라 이전 세션의 설계 rationale, rejected alternatives, red-team findings, implementation handoff를 보존한 EVIDENCE_ONLY 문서다. 충돌하면 AUTHORITY.md가 가리키는 active spec/실행 코드가 우선한다.

대기업 상용 서비스 수준으로 계속 red-team한다.
시니어 개발자·PM·데이터·SRE/운영·보안/법무·CS·마케팅·조직 운영 관점에서 contradiction/ambiguity를 적극적으로 찾는다.

추측하지 말고 repository current state를 우선한다.
001~009 migration은 수정하지 않는다.
legacy current implementation과 CPI W1 target semantics를 섞지 않는다.
semantic contract 변경이 필요하면 구현을 멈추고 이유와 영향 범위를 먼저 보고한다.

현재 plan에서 실제 다음 미완료 Task를 확인한 뒤 그 Task부터 TDD 방식으로 진행한다.
```

## 41. Final continuity rule

The key lesson from this design process is not a single table name or parser rule.

It is the separation of authorities and time domains:

```text
official source evidence
!= platform interpretation
!= system-known-at-T
!= operational workflow state
!= serving eligibility
!= customer representation
!= marketing claim
```

Every future implementation choice should be checked against that separation before being accepted.

If one field, enum, table, endpoint, or "convenient latest value" starts representing two of those domains at once, treat that as a likely design regression and red-team it before continuing.
