# CPI W1 Data & Governance — Full Session Decision Log and Continuity Record

Status: `EVIDENCE_ONLY / SESSION_CONTINUITY`

Compiled: 2026-09-21  
Repository: `hjh6709/us-market-intelligence-pipeline`  
Working branch: `docs/cpi-w1-data-governance-2026-09-19`  
Branch HEAD at compilation: `b38ff7ea8d7df3ea7af6482468aa16b482feab71`

> This file preserves the complete **shareable** reasoning record from the architecture and implementation-review sessions: goals, assumptions, contradictions found, rejected alternatives, decision changes, review criteria, implementation progress, and continuation instructions.
>
> It is intentionally not semantic authority. `docs/architecture/AUTHORITY.md` routes active authority. The CPI W1 semantic authority is the active design spec, and implementation order is owned by the implementation plan.
>
> This file does not reproduce private model chain-of-thought verbatim. Instead, it records the full reviewable rationale an engineer, reviewer, or future session needs to understand why the current design exists and how it evolved.

## 1. User intent that governed the entire effort

The user repeatedly asked for the repository to be treated as a real commercial service owned jointly by senior engineering, PM, marketing, security/legal, data, CS, SRE/operations, and organizational leadership.

The working instruction was not “find reasons this is okay.” It was:

- assume hidden contradictions remain until actively disproved;
- re-check earlier conclusions instead of trusting prior review passes;
- distinguish implementation truth from design intent;
- prefer evidence and explicit authority over narrative confidence;
- keep the system maintainable by default and scalable by design;
- add complexity only when a concrete failure mode justifies it;
- preserve enough rationale that another session can continue without reconstructing decisions from chat history.

That requirement is why this work accumulated multiple red-team passes before and during implementation planning.

## 2. Original repository state and why it was not enough

The CPI W1 work started from:

`main @ 83e0259f2d587a50e5b670dc521e9a9b75283442`

At that point:

- the live/current product was Python + FastAPI + browser UI;
- legacy serving still read legacy research/provider tables;
- migrations `001` through `009` already existed and were part of repository history;
- migration `009_event_session_foundations.sql` provided a normalized event/session foundation;
- migration 009 was not the production CPI adapter path;
- current CPI ingestion still depended on `config/cpi_releases.json` and FRED/ALFRED-style observations;
- legacy pipeline telemetry remained active;
- several 2026-09-10 documents still looked current enough to mislead implementers even though they encoded older target semantics.

Those older semantics included:

- lifecycle revision chains;
- observation `revision_number`;
- current-primary markers;
- Airflow-centric target operations;
- a product direction that could be misread as the active final target.

The conclusion was not that migration 009 was “bad.” It was that it was a valid historical foundation that should not silently become the semantic authority for the new CPI W1 design.

## 3. Architectural classification and process

This was classified as **Architectural refinement**, not a bounded code change.

The work therefore followed:

1. repository exploration;
2. repeated contradiction review;
3. decomposition into smaller architectural subprojects;
4. CPI W1 Data & Governance written spec;
5. spec self-review;
6. user approval;
7. detailed implementation plan;
8. plan self-review;
9. staged implementation with TDD and per-task commits;
10. continuing red-team review during implementation whenever a hidden semantic blocker was discovered.

The full product architecture was deliberately **not** frozen as one monolithic spec.

The decomposition became:

- CPI W1 Data & Governance;
- CPI W1 Product Serving;
- Platform Runtime / Security / Readiness.

Only the first of these is the subject of this branch.

## 4. Core design principle discovered during review

The most important invariant is the separation of authorities and state domains:

```text
official source evidence
!= platform interpretation
!= system-known-at-T
!= operational workflow state
!= serving eligibility
!= customer representation
!= marketing claim
```

Whenever one table, field, enum, endpoint, or “latest value” starts representing two of those domains at once, treat it as a likely design regression.

## 5. Authority-model correction

A major early problem was repository authority ambiguity.

The repository had current code, dated current-system snapshots, old “canonical” architecture docs, status ledgers, and old target specs that were all close enough in wording to be misread as active authority.

The authority model was therefore formalized:

- `CURRENT_IMPLEMENTATION` — executable code, migrations, and tests at the commit being inspected;
- `TARGET_CANONICAL` — active target semantics for a specific concern;
- `STATUS_LEDGER` — implementation progress only;
- `HISTORICAL_SUPERSEDED` — historical design/rationale preserved but unable to define current target semantics;
- `EVIDENCE_ONLY` — dated verification/handoff evidence.

The following were reclassified for CPI W1:

- `docs/architecture/platform-contract.md` -> historical/superseded target;
- `docs/architecture/data-contracts.md` -> migration-009 historical foundation contract;
- `docs/architecture/operations-and-deployment.md` -> historical target;
- old 2026-09-10 Superpowers target specs -> historical/superseded;
- `docs/engineering/api-contracts.md` -> current FastAPI compatibility/current implementation;
- `docs/engineering/current-vs-target.md` -> status ledger only;
- `docs/architecture/current-system.md` -> verified current snapshot, not eternal executable authority.

A later audit found a contradiction:

- `AUTHORITY.md` routed the CPI W1 spec as `TARGET_CANONICAL`;
- the spec header still said `PROPOSED TARGET SPEC — AWAITING USER REVIEW`.

That contradiction was corrected in later commits.

## 6. Non-negotiable CPI W1 semantic invariants

### Event and disclosure

1. CPI event identity is the statistical occurrence, not publication timestamp.
2. CPI reference month is required and normalized to the first calendar day of the reference month.
3. Schedule evidence and release evidence are separate domains.
4. Event occurrence and disclosure are separate identities.
5. Event/disclosure is many-to-many.
6. A later supplemental publication may refer to an older occurrence without becoming that occurrence’s release.
7. Missing evidence is not cancellation, unavailable, or not published.
8. Cancellation requires explicit authoritative evidence.
9. No product-level `PRIMARY` marker is stored as official source fact.
10. Release timestamp is not disclosure identity.

### Evidence

11. Raw source artifacts are immutable evidence where retention rights allow.
12. Artifact metadata survives raw-byte deletion/non-retention.
13. Source URL is provenance, not artifact identity.
14. Mutable current-release URL is never permanent event identity.
15. Parser success is not canonical acceptance.
16. Parser failure and valid-official disagreement are different states.
17. Valid official disagreement is retained and resolves to `CONFLICT`; no latest-wins.
18. Official actual and consensus are different truth domains.
19. A dash or blank is not by itself `EXPLICIT_UNAVAILABLE`.
20. Official decimal values use exact decimal semantics.
21. No LLM/fuzzy inference participates in canonical extraction.

### Time and PIT

22. Source chronology, capture time, accepted-at, governance-applied-at, serving-control-applied-at, representation time, and request time are different clocks.
23. No synthetic midnight for date-only evidence.
24. Replay must not make old evidence appear newly published.
25. `OFFICIAL_SOURCE_RECONSTRUCTION` and `SYSTEM_KNOWN_PIT(T)` are different contracts.
26. Backfilled evidence may appear in current source reconstruction while remaining invisible in PIT before its `accepted_at`.
27. Storage `created_at` is never substituted for knowledge time.

### Governance and serving

28. Evidence is immutable; interpretation changes through append-only decisions.
29. Emergency withholding changes serving eligibility, not source truth.
30. Serving-control decisions are append-only.
31. No decision defaults to VALID / ENABLED.
32. No-op governance and serving decisions should be rejected.
33. Domain-wide WITHHELD uses deny-overrides.
34. Public/API/Paper-forward/alerts must obey serving controls.
35. Offline diagnostics may inspect underlying knowledge only through non-side-effecting paths.

### Workflow

36. At-least-once execution + durable idempotency; no exactly-once claim.
37. Retry repeats execution, not business identity.
38. Lease/fencing prevents stale workers from mutating canonical evidence or terminalizing work.
39. `SKIP LOCKED` is a queue primitive, not consistency semantics.
40. Run/work/attempt state and outcome are separate.
41. Collector and promoter are separate trust seams.
42. Promoter may not fetch alternate internet evidence when expected input is missing.
43. Backfill/replay does not cause live customer side effects by default.

## 7. BLS source-contract reasoning

The design explicitly stopped treating all official BLS surfaces as equal merely because they are official.

Roles:

- CPI-specific schedule page -> `AUTHORITATIVE`;
- BLS global calendar / ICS -> `FALLBACK_CORROBORATION`;
- checked-in release manifest -> `FIXTURE_ONLY`;
- CPI News Release HTML -> release/disclosure evidence;
- same-release Table 1 XLSX -> official representation/corroboration.

Reason:

A CPI-specific schedule page and global ICS can temporarily disagree because official surfaces synchronize at different times. If all official evidence were treated as equal and “latest capture wins,” internal publication lag could become a false customer-facing conflict.

Therefore authority tier is applied **before** chronology comparison.

Also rejected:

- HTTP 200 as proof of a new release;
- stale current-page content as evidence of “no release”;
- 403/429/timeout as economic facts;
- proxy rotation/hammering as a normal recovery strategy;
- provider disagreement as a vote to replace BLS truth.

## 8. Schedule model evolution

Rejected: source lifecycle states such as synthetic `RESCHEDULED`.

Final:

- each source statement is an assertion;
- original `SCHEDULED` remains evidence;
- later `SCHEDULED` with newer source chronology is additional evidence;
- selector resolves by authority tier + comparable chronology;
- `DATE_PENDING` requires explicit evidence;
- absence means unresolved;
- date-only schedules use `DUE_DATE_UNTIMED` on the scheduled local date.

This avoids inventing lifecycle states the source never actually published.

## 9. Why disclosure identity is separate

The separation exists because:

- one disclosure may contain evidence relevant to multiple occurrences;
- supplemental material must not become the release anchor;
- correction notices have their own publication identity;
- source-native IDs may not exist;
- timestamp is too fragile to serve as disclosure identity.

A platform-owned `canonical_disclosure_key` identifies a disclosure, and explicit relations describe event/disclosure and disclosure/artifact topology.

## 10. Event-disclosure relation rules

Relation kinds:

- `EVENT_RELEASE`
- `SUPPLEMENTAL_DISCLOSURE`

Only `EVENT_RELEASE` participates in CPI release-anchor selection.

Hard rule:

> CPI W1 Core 4 canonical actuals must pin an `EVENT_RELEASE` relation.

A supplemental disclosure may preserve context but may not fill a missing/canceled event’s canonical actuals.

## 11. Disclosure-artifact relation rules

A disclosure can have multiple official representations.

Relations include:

- `RELEASE_REPRESENTATION`
- `CORROBORATING_REPRESENTATION`
- `CORRECTION_NOTICE`

Important red-team result:

A correction notice is linked to a CPI disclosure only when its scope actually applies to that disclosure. A generic CPI/BLS notice about another series, geography, or database representation must not be attached merely because it belongs to the CPI program.

## 12. Artifact design evolution

### Rejected: global artifact-row dedupe

Why:

If two independent capture attempts retrieve the same bytes, global dedupe collapses separate acquisition events and makes `captured_at` ambiguous.

Final:

- one `source_artifact` row = one successful capture event;
- idempotency is scoped to same attempt + locator + hash;
- different attempts may create distinct rows with identical bytes;
- semantic selectors converge identical meaning later.

### Rejected: shared deletable object between artifact rows

Why:

Retention/deletion of one capture must not silently affect another capture’s forensic evidence.

Final W1:

- each retained artifact owns its own immutable physical object identity;
- hash is byte-integrity identity, not deletion ownership;
- deletion remains independently accountable.

### Rejected: `public_source_url`

Why:

Publicly reachable is not the same as stable/safe permanent customer citation.

Final:

- `retrieval_url` is forensic provenance;
- stable product citation is a later serving projection.

## 13. Contract-version separation

Three version domains remain separate:

- `source_contract_version` — how/why bytes were retrieved;
- `extractor_contract_version` — how bytes were interpreted;
- selector contract version — how multiple evidence rows resolve into knowledge.

A single generic version field was rejected because it destroys replay/debug clarity.

## 14. Core 4 model

Canonical observation codes:

- `CPI_HEADLINE_MOM`
- `CPI_HEADLINE_YOY`
- `CPI_CORE_MOM`
- `CPI_CORE_YOY`

For the CPI `EVENT_RELEASE` contract, all four semantics must resolve as:

- `VALUE`, or
- `EXPLICIT_UNAVAILABLE`

before the bundle is promoted.

This rule does not force every supplemental/correction disclosure to manufacture four values.

The 2025 exceptional release behavior is required test coverage because full semantic resolution can coexist with partial numeric availability.

## 15. Major correction: release envelope and observation bundle are separate promotion families

A late red-team pass found a serious flaw in the original “one promotion result” model.

A release can clearly exist while observation extraction remains unsafe.

If one transaction handled both:

- either the release would be hidden because observation parsing quarantined;
- or partial observations would leak through.

Final split:

### `CPI_RELEASE_ENVELOPE_PROMOTE`

Promotes:

- program identity;
- reference month;
- disclosure identity;
- `EVENT_RELEASE` topology;
- release representation;
- official release marker.

It may succeed even if numeric parsing later quarantines.

### `CPI_OBSERVATION_BUNDLE_PROMOTE`

Requires valid matching release topology and atomically promotes Core 4.

Valid final state:

```text
release = DISCLOSED
observation work = QUARANTINED
observation knowledge = UNRESOLVED / withheld from governed consumers
```

This is a durable work-family boundary, not a microservice requirement.

## 16. QUARANTINED versus CONFLICT

The distinction is deliberate.

- `QUARANTINED` -> our interpretation is unsafe.
- `CONFLICT` -> multiple valid official evidences disagree.

Example:

If HTML and XLSX both validate but disagree, both evidence paths can succeed. The selector returns `CONFLICT`. Neither is discarded merely because it arrived second.

## 17. Material fingerprint design

`material_fingerprint` hashes semantic meaning only.

It excludes:

- artifact ID;
- URL;
- parser version;
- accepted time;
- capture time;
- operator identity;
- ingestion attempt.

This allows two independent evidences with identical meaning to converge without erasing provenance.

## 18. Provenance graph and transitive eligibility

A later review found that event ID + disclosure ID + artifact ID were not enough.

Official observations pin:

- exact `disclosure_link_id`;
- exact `disclosure_artifact_link_id`;
- matching event/disclosure/artifact identities.

Markers similarly pin disclosure-artifact relation.

Therefore:

- invalidating a relation does not delete dependent evidence;
- dependent evidence becomes selector-ineligible transitively.

This gives governance a precise graph boundary.

## 19. Governance design

Governable subject types are intentionally limited to:

- schedule assertion;
- event-disclosure link;
- disclosure-artifact link;
- disclosure-marker assertion;
- official-observation assertion.

Identity nodes such as event occurrences and disclosures are not directly invalidated.

The evidence row UUID is reused as `interpretation_subjects.subject_id`; no second governance UUID is created.

## 20. Governance approval policy

A security red-team found that approval threshold must not be request-controlled.

Rejected:

- proposer chooses `required_approvals`.

Final W1 policy:

- exactly one independent APPROVE;
- approver differs from proposer;
- no REJECT;
- policy version owns threshold;
- expiry is policy-generated;
- proposer cannot bypass threshold or expiry.

Effective decisions are append-only and versioned.

## 21. Serving-control reasoning

Rejected:

- one mutable current serving row.

Reason:

It destroys historical incident reconstruction.

Final:

- append-only `economic_serving_control_decisions`;
- versioned per scope;
- no-op changes rejected;
- domain WITHHELD overrides event ENABLED;
- activation is atomically audited;
- serving state is applied after knowledge selection.

Serving control never rewrites the underlying source knowledge result.

## 22. Knowledge versus representation time

Two concepts are required:

- `knowledge.asOf` — source/interpretation knowledge cutoff;
- `representationAsOf` — last semantic change in what a governed consumer would receive.

A number can remain the same while representation changes because:

- serving control withholds it;
- correction notice is surfaced;
- stable source link changes.

Request time is neither.

## 23. Workflow state machine

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

### Attempts

States:

- `RUNNING`
- `TERMINAL`

Stale attempts lose mutation authority when claim generation advances.

## 24. Separate collection and promotion scopes

A run-level `execution_scope` exposed a contradiction:

one run cannot honestly belong to both collector and promoter authority if those are separate trust seams.

Final direction:

- collection runs are collection-scoped;
- promotion runs are promotion-scoped;
- lineage is recovered through artifacts and input-artifact relationships;
- no mixed-authority generic run is introduced.

## 25. Schema decision

An intermediate `app.ingestion_*` proposal was rejected.

Reason:

It contradicted the decision to avoid schema-per-domain complexity and risked turning schema names into fake security boundaries.

Final W1:

- keep PostgreSQL `public` schema;
- security comes from ownership, grants, capability roles, RLS where justified, and workload identities;
- one schema does not mean one security boundary.

## 26. Physical model

The design converged on 19 physical tables:

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

Governance:

14. `interpretation_subjects`
15. `interpretation_requests`
16. `interpretation_approvals`
17. `interpretation_decisions`
18. `business_audit_events`

Serving:

19. `economic_serving_control_decisions`

The number is not a KPI. It is the result after repeatedly removing abstractions without a concrete W1 failure mode.

## 27. Explicitly rejected alternatives

Rejected during review:

- one giant full-platform canonical spec;
- using migration-009 semantics as the final CPI W1 model;
- rewriting migrations 001-009;
- one mega event-status enum;
- silent latest-wins correction;
- majority vote between official representations;
- generic message bus before multiple real consumers exist;
- microservice split merely to express collector/promoter trust;
- mutable serving-control current-state row;
- one promotion result for release existence + observations;
- LLM/fuzzy canonical extraction;
- treating current public URL as permanent source identity;
- treating schedule/release/observation/serving as one lifecycle;
- letting status docs create target semantics.

## 28. Product / API / UI seam already frozen

Later Product Serving work must preserve:

- release state separate from observation availability;
- WITHHELD values absent from API/UI/meta/SEO/notifications/cache;
- null never rendered as zero;
- exact official decimals serialized without float drift;
- public historical view = source reconstruction;
- forward/Paper validation = system-known PIT;
- `DUE_DATE_UNTIMED` for date-only scheduled day;
- HTTP service health != financial-data health;
- canonical-authority outage fails closed for governed CPI numeric truth.

## 29. Runtime/security assumptions deferred

Target constraints already identified for the later Runtime/Security/Readiness spec:

- runtime workloads never own DB objects;
- permanent object owner is NOLOGIN;
- migration identity separate from deploy and runtime identities;
- collector cannot mutate canonical facts;
- promoter cannot mutate user/governance state;
- product readers do not receive raw object credentials;
- governance activation uses narrow guarded mutation boundary;
- prod/nonprod identities and data separated;
- workforce identity != workload identity;
- break-glass is time-limited, reasoned, audited, reviewed;
- restore testing is required; backups alone are not recoverability.

These are target constraints, not current implementation claims.

## 30. Enterprise readiness model

The organizational model became:

```text
Mergeable != Deployable != Launchable
```

Stages:

- Architecture Freeze
- Implementation
- Verification
- Internal Alpha Readiness
- Public Beta
- Paid Launch

A feature can be code-complete and still be unlaunchable.

Readiness evidence must bind to exact artifact/schema/environment/configuration, not a free-floating checklist.

## 31. PM / marketing / CS implications

Marketing cannot:

- claim a value is current while serving control withholds it;
- expose a mutable BLS current URL as stable historical proof;
- flatten source conflict into one canonical number;
- present latest-revised data as historical as-released data.

CS must be able to explain:

- release confirmed but values still under review;
- value temporarily withheld without source history deletion;
- a correction changed platform interpretation;
- why a user saw a different state earlier.

This is why serving-control history and representation timing are first-class.

## 32. Security/legal implications

Important constraints:

- raw bytes retained only under approved source-rights policy;
- attribution preserved;
- retrieval provenance != customer deep-link authority;
- one artifact’s deletion does not alter another artifact’s forensic history;
- HTML extraction executes no JS/subresources;
- XLSX parsing is bounded, inert, read-only/data-only;
- redirects/hosts/size/timeouts are source-contract bounded;
- legal-rights evidence is a launch gate, not an assumed fact.

## 33. Implementation plan

The approved plan is:

1. authority/status migration;
2. ingestion/artifact/governance-subject foundation;
3. event/schedule/disclosure/marker evidence;
4. Core 4 observation assertions;
5. governance + serving-control history;
6. Python semantic contracts;
7. safe artifact storage + BLS retrieval;
8. schedule parsing;
9. release-envelope + Core 4 extractor;
10. claim fencing + split promotion;
11. selectors/PIT + serving overlay;
12. governance application boundary;
13. golden corpus replay;
14. controlled collector/orchestrator;
15. legacy coexistence + migration-equivalence verification;
16. final verification evidence.

Each task is intended to follow:

`RED -> minimal implementation -> GREEN -> regression -> self-review -> commit`.

## 34. Branch commit history relevant to this effort

From implementation-plan completion through current HEAD:

- `11dbcfca77eb` — establish CPI W1 architecture authority routing
- `9ee864b5626b` — harden CPI W1 workflow and governance semantics
- `310bb45e3f3c` — reconcile CPI W1 canonical authority status
- `9b98b823bc79` — remove remaining CPI W1 authority wording drift
- `9342e6bbd528` — add CPI W1 ingestion evidence foundation
- `ce70f262f513` — preserve CPI W1 artifact deletion metadata
- `414978bc9d6d` — restore CPI W1 migration dollar quoting
- `1dbd954ce8a9` — correct CPI W1 migration delimiter
- `f0d5667d806f` — add CPI W1 event disclosure evidence model
- `fe7b9bf473d1` — add CPI W1 design reasoning and session handoff
- `9a0a14615c60` — add CPI W1 official observation evidence
- `c6e28f98f0c4` — add CPI interpretation governance and serving controls
- `8e0df35abec6` — harden CPI governance activation boundaries
- `ae29879a8ca2` — enforce CPI evidence trust and forensic boundaries
- `cce09afe2929` — define CPI W1 semantic contracts
- `d62b4602dea9` — add bounded BLS CPI source capture
- `0bf0c785ce1e` — harden CPI source redirect and artifact durability
- `77dafc1da612` — parse CPI schedule evidence
- `dc137910f2b9` — align CPI W1 readiness and legal source gates
- `30714c2c30d5` — restore CPI promoter migration delimiter
- `7ed16ee45086` — freeze CPI knowledge-time trust boundary
- `582648f1f650` — bind CPI governance actors to IdP identity
- `cb2ad5f82941` — cover explicit CPI nonpublication evidence
- `b38ff7ea8d7d` — define CPI exception evidence authority

Current HEAD at compilation:

`b38ff7ea8d7df3ea7af6482468aa16b482feab71`

The branch has progressed significantly beyond the original Task 1 state. A future session must inspect current HEAD and actual plan progress rather than assuming the next task from old chat context.

## 35. High-risk areas that must keep being red-teamed

### Database/migrations

- cross-scope/domain composite FK mistakes;
- RETAINED artifact without verifiable object identity;
- runtime UPDATE/DELETE path on append-only evidence;
- provenance graph mismatch;
- subject registry row without typed evidence or vice versa;
- unique constraint collapsing distinct capture events;
- terminal run accepting new work;
- claim generation and attempt generation diverging;
- migration 001-009 accidental modification.

### Source/parser

- current release page accepted for wrong reference month;
- fixed-cell-position extraction;
- dash treated as unavailable without explicit context;
- stale current page treated as nonpublication;
- transport error converted into economic state;
- unsafe XLSX formula/macro/external-link behavior;
- parser-version semantic changes without differential replay.

### PIT/selector

- `accepted_at` used as source chronology;
- `created_at` used as knowledge time;
- dependent assertion remains eligible after relation invalidation;
- duplicate same semantic material creates false conflict;
- conflicting materials collapse to latest;
- supplemental disclosure becomes release anchor;
- correction silently overwrites historical material;
- serving-control history contaminates source reconstruction.

### Governance

- proposer controls threshold/expiry;
- equivalent identity allows self-approval;
- same expected version activates twice;
- no-op decision creates fake history;
- serving decision not atomically audited;
- domain WITHHELD bypassed by event ENABLED.

### Product seam

- release inferred from observation state;
- null rendered as zero;
- withheld value leaks via SEO/cache/notification;
- mutable URL exposed as stable proof;
- stale cache survives correction;
- HTTP health confused with data health.

### Operations

- duplicate promotion identity from reconciliation;
- collector writes canonical facts directly;
- promoter uses internet fallback;
- replay/backfill creates live side effects;
- orphan object not reconciled after DB failure;
- DB says RETAINED but object generation is missing/wrong.

## 36. Decisions that must not be changed casually

Stop and re-review the spec before changing:

- migrations 001-009;
- CPI event identity;
- event/disclosure separation;
- split envelope/observation promotion;
- Core 4 atomic observation-bundle rule;
- EVENT_RELEASE-only Core 4 actual rule;
- source reconstruction vs system-known PIT separation;
- append-only interpretation decisions;
- append-only serving-control decisions;
- preservation of conflicting official evidence;
- no latest-wins;
- no LLM/fuzzy extraction;
- collector/promoter trust separation;
- legacy FastAPI coexistence until Product Serving has its own migration.

## 37. Current coexistence rule

Do not confuse new W1 implementation with replacement of the existing product.

Current intended coexistence:

- legacy `src/cpi_ingestion.py` remains for current callers;
- legacy `pipeline_*` telemetry keeps its vocabulary;
- migration 009 remains historical executable foundation;
- migration 010+ adds CPI W1 objects;
- current FastAPI remains current serving until separate Product Serving work;
- new W1 modules do not silently reinterpret legacy status/enums.

## 38. How a new session should resume

Read in order:

1. `docs/architecture/AUTHORITY.md`
2. `docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md`
3. `docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md`
4. `docs/superpowers/handoffs/2026-09-20-cpi-w1-data-governance-session-handoff.md`
5. this file

Then:

- inspect current branch HEAD and recent commits;
- inspect actual plan progress and existing migrations/tests/modules;
- run relevant tests before changing the next subsystem;
- continue red-team review against section 35;
- if an implementation need conflicts with a frozen invariant, stop implementation and update/review the spec first.

## 39. Recommended next-session prompt

```text
이전 세션에서 진행하던 us-market-intelligence-pipeline CPI W1 Data & Governance 작업을 이어서 진행한다.

Repository:
https://github.com/hjh6709/us-market-intelligence-pipeline

작업 브랜치:
docs/cpi-w1-data-governance-2026-09-19

반드시 먼저 저장소 현재 HEAD와 recent history를 직접 확인하고 아래 문서를 처음부터 읽어.

1. docs/architecture/AUTHORITY.md
2. docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md
3. docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md
4. docs/superpowers/handoffs/2026-09-20-cpi-w1-data-governance-session-handoff.md
5. docs/superpowers/handoffs/2026-09-21-cpi-w1-full-session-decision-log.md

4번과 5번은 EVIDENCE_ONLY handoff/decision-log 문서다.
semantic conflict가 있으면 AUTHORITY.md가 가리키는 active spec과 executable repository가 우선한다.

대기업 상용 서비스 수준으로 계속 red-team한다.
시니어 개발자·PM·데이터·SRE/운영·보안/법무·CS·마케팅·조직 운영 관점에서 contradiction/ambiguity를 적극적으로 찾는다.

추측하지 말고 current repository state를 우선한다.
migration 001~009는 수정하지 않는다.
legacy current implementation과 CPI W1 target semantics를 섞지 않는다.
semantic contract 변경이 필요하면 구현을 멈추고 이유와 영향 범위를 먼저 보고한다.

현재 plan에서 실제 다음 미완료 task를 확인한 뒤 그 task부터 TDD 방식으로 진행한다.
```

## 40. Continuity rule

The purpose of these handoff documents is to make the repository self-explanatory enough that a future session does not need chat memory to understand why the current model exists.

When future work discovers a real contradiction, add the new rationale to a dated handoff/decision record instead of rewriting history to make the design look inevitable.

Preserve the distinction between:

- what was known then;
- what was decided then;
- what later evidence disproved;
- what changed;
- what is authoritative now.
