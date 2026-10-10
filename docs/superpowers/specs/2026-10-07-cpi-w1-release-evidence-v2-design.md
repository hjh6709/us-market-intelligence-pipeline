# R1 — CPI W1 Release Evidence V2 design

Status: **DRAFT FOR WRITTEN REVIEW — NOT IMPLEMENTED**

Date: 2026-10-07. Repository: `hjh6709/us-market-intelligence-pipeline`.
Branch: `feat/cpi-w1-official-corpus-readiness`.
Inspected starting HEAD and remote main: `8ae349afb41505dd99fb76562ae2b424e12fc71d`.

## 1. Intent and authority

This is a release-evidence corrective extension to the merged CPI W1 foundation,
not a replacement architecture. The user's R1 directive is authoritative for this
change. Existing accepted lifecycle, promotion, fencing, recovery, and legacy
coexistence contracts remain in force.

The user selected additive migration 014 with persisted V2 evidence and database
authorization guards. This document specifies that approach for review; selection
of the approach is not approval of this written specification or a future plan.

Success means reproducible capability-specific evidence, fail-closed readiness,
and exact tested-workload authorization, while preserving historical V1 data.
No gate becomes ELIGIBLE. No production authorization is created.

## 2. Inspected current state versus proposed state

Current implementation has immutable V1 evidence and authorization material tables,
capability registries, corpus-v2 expectations, and exact executor provenance checks
against authorization material. V1 permits a null tested workload. Its Python
authorization constructor does not prove that the executor artifact/job equals the
tested artifact/job. Review references currently prove hash shape, not review bytes.
The database resolver checks executor-to-material equality, not tested-evidence
equality. These are concrete R1 gaps, not claims that V2 already exists.

Existing uncommitted replay corrections dispatch per capability's registry-owned
extractor; their previous verification is historical evidence, not verification of
this proposed V2 design. Preserve those changes and unrelated photos/presentation
artifacts; do not stage them with this specification.

Official inventory currently contains 56 REMOTE_ONLY artifacts, zero materialized
official artifacts, and nine BLOCKED capability decisions. Synthetic fixtures do
not count as official evidence. No new BLS downloads are part of R1.

## 3. Artifact × capability replay ownership

An artifact owns provenance and immutable bytes, not a parser identity. Every
evaluation resolves `promotion_capability_id` through the capability registry and
uses its extractor version for dispatch, result identity, expected-diff matching,
and evidence generation. Multiple expectations yield independent results.

Artifact-wide `extractor_contract_version`, `replay_required`, and
`expected_semantics` stay forbidden. Unknown capabilities, kind mismatches, and
unsupported extractor versions fail closed. Expected-diff approval remains scoped
to the exact artifact, capability, extractor, and before/after semantics.

The implementation will surgically amend canonical design section 31.2: replace
entry-scoped extractor wording, describe replay against the applicable corpus for
each capability, and include capability identity in expected-diff wording. No
unrelated canonical architecture sections are rewritten.

## 4. CapabilityEvidencePolicyV1

Introduce a checked-in versioned policy registry with exactly one policy per ACTIVE
capability. Canonical policy payloads contain:

- schema and policy version, capability ID, and verified release-subject digest;
- coverage mode and a declarative required-coverage rule;
- required official evidence classes and conformance classes;
- required exceptional cases;
- whether zero official evidence is allowed;
- policy completeness and explicit reasons for incomplete coverage rules.

The policy digest is lowercase SHA-256 of canonical JSON, excluding its own digest.
Use sorted object keys, deterministic set ordering, UTF-8, and no binary floats.
Unknown fields/versions, duplicate identities, and invalid registry relations fail
validation. An incomplete rule is valid policy data but cannot support readiness.
All nine R1 policies require official evidence: zero-official allowance is false.

### Coverage obligations, not invented numeric thresholds

| ACTIVE capability | Coverage contract | Initial completeness |
| --- | --- | --- |
| BLS_CPI_RELEASE_ENVELOPE_HTML | Historical monthly release baseline and envelope exceptions | Coverage declared; official proof absent |
| BLS_CPI_CORE4_HTML | Same applicable monthly releases; numeric and availability exceptions | Coverage declared; official proof absent |
| BLS_CPI_REVISED_RELEASE_DATES | Explicit historical cancellation/exception evidence, including 2025-10 | Coverage declared; official proof absent |
| BLS_CPI_SCHEDULE_HTML | Official current schedule and mutation/change evidence | INCOMPLETE: exact coverage rule not frozen |
| BLS_CPI_GLOBAL_ICS_SCHEDULE | Official fallback/corroboration evidence | INCOMPLETE: exact coverage rule not frozen |
| BLS_CPI_TABLE1_REPRESENTATION_XLSX | Applicable releases and seasonal-revision representation edges | INCOMPLETE: exact coverage rule not frozen |
| BLS_CPI_CORE4_XLSX | Applicable releases and seasonal-revision extraction edges | INCOMPLETE: exact coverage rule not frozen |
| BLS_CPI_CORRECTION_NOTICE_HTML | Event-driven positive and negative notice cases | INCOMPLETE: exact coverage rule not frozen |
| BLS_CPI_CORRECTED_OBSERVATIONS_HTML | Verified correction cases linked to release evidence | INCOMPLETE: exact coverage rule not frozen |

For historical HTML, the repository's 2022-01 through 2026-08 reference-month
baseline determines obligations, not a generic count of 56. A claimed canceled
month is not silently omitted: it must have verified cancellation evidence or
remain a missing/unverified obligation. Ordinary releases, negative/zero values,
annual January periods, partial availability, post-shutdown recovery, latest
release, discovered changed-byte locators, and relevant corrections retain the
canonical design's applicable exception obligations. An absent exceptional artifact
is reported as missing evidence, not a parser success. Do not invent examples or
numerical minimums to make policies complete.

## 5. CapabilityEvidenceReadiness

Evaluate every ACTIVE capability by enumerating the registry first, never merely
the expectations present in the manifest. Return policy presence/completeness,
required obligations, applicable official entries, materialized/pinned counts,
replay results, conformance results, unresolved diffs, missing classes, status,
and deterministic blocking reasons.

READY requires a complete current policy, satisfied official coverage and
exception obligations, verified immutable bytes and expected semantics, passing
required replay/conformance, and no unresolved diff. Missing policy, zero official
evidence, missing extractor, or incomplete coverage is NOT_READY. Inventory mode
reports every capability without attempting remote collection. Global readiness
is the conjunction over required ACTIVE capabilities; an empty required set is a
configuration error, not vacuous success. Readiness does not update release gates.

## 6. Capability corpus and result digests

`capability_corpus_snapshot_digest` hashes a canonical projection containing the
capability/subject identity, registry extractor identity, and only applicable
official entries and relevant conformance/coverage material. Entry projections
bind artifact identity/locator, hash or explicit missing-hash state, materialization
and review/pinning state, capability-scoped expected semantic digest, source
contract identity, and relevant exceptional tags/coverage declarations.

Filter before sorting and hashing. Other capabilities' expectations, parser
versions, unrelated entries, and irrelevant metadata must not enter the projection.
If another capability provides evidence relevant to this capability's obligation
(for example cancellation evidence), include an explicit dependency projection;
that evidence is then relevant, not an unrelated mutation. Do not hash the whole
registry or manifest as a shortcut.

Expected-diff and replay digests are likewise capability-scoped. Sort by full
evaluation identity; reject duplicate identities rather than accepting last-wins.
Changed relevant hashes/semantics/coverage change digests; input order does not.

## 7. PromotionEvidenceSnapshotV2 and identity domains

V2 is an explicit schema with a separate exact parser and canonical payload:

- schema/evidence-policy version and release-subject digest;
- capability evidence policy digest;
- capability corpus snapshot digest;
- capability-scoped expected-diff approvals and replay result digests;
- source-contract digest, binding the exact source interpretation contract;
- tested job contract version;
- tested source-content digest;
- tested executor source revision;
- tested workload artifact digest.

The content digest hashes declared relevant source/config/policy inputs; it is not
a Git revision. The executor revision is the build's recorded source revision and
is compared only with the corresponding executor revision. Null tested build or
revision may represent blocked, incomplete evidence but never eligible evidence.
Mutable gate, runtime approval, and serving state are excluded from snapshots.

V1 canonical JSON, parser behavior, digests, and stored rows remain historical V1.
Never recalculate a frozen snapshot to hide a source mismatch. Existing V1 null-build
snapshots remain readable but cannot support a new eligible authorization. No
conversion or backfill from V1 to V2 occurs automatically.

## 8. ReviewArtifactV1 trust chain

Use a small immutable value object/helper to read repository-local review bytes
and verify SHA-256 against the claimed reference/digest. Preserve exact bytes;
do not normalize whitespace before hashing. Fail closed on missing files,
out-of-root references, mismatched digest, unsupported references, or a review
whose declared bindings do not match subject/policy/evidence/build under review.

A verified review record carries schema, reference, byte digest, and explicit
review bindings. Byte integrity is not human approval or organizational SoD. R1
does not implement reviewer IAM and does not invent review contents or signatures.
Expected-diff review bytes are also verified wherever such local approvals exist.

## 9. Additive migration 014 persistence

Do not edit migrations 001–013. Extend existing evidence/authorization tables in
014, retaining their exact-reference foreign keys and immutable triggers. Use
schema-discriminated constraints: V1 keeps its original required fields and null
V2-only fields; V2 requires its own identity/digest fields and does not reuse the
legacy global corpus/source-revision columns with a different meaning. Any relaxed
column-level NOT NULL is replaced by an equally strict schema-conditional CHECK.

Persist immutable capability policy payloads and verified review records with exact
subject/digest references. V2 material must reference V2 evidence and verified
review bytes, not a shape-only string. Database byte-digest checks and binding
constraints reject direct SQL insertion of mismatched review material. Python and
database canonical hashing must use the same explicit byte serialization contract;
PostgreSQL JSON display formatting is not the canonical JSON serializer.

Policy freshness cannot be inferred from a digest existing somewhere in history.
Persist append-only policy registrations per subject, with monotonic registration
versions and an effective latest registration. Registration is a configuration
binding, not an eligible release grant. Synchronizing changed checked-in policy
requires a new registration; old payloads/evidence remain immutable. V2 eligible
material and runtime revalidation must match the effective policy digest. Serialize
registration using the existing release-subject lock discipline, so a policy change
cannot race a final canonical write. Do not add an independent lock order or new
organizational approval workflow.

Historical rows remain byte-for-byte unchanged. Explicit V2 repository writers and
readers do not pass V2 through the V1 mapper. Extend migration verification with a
001–013 immutable hash manifest without replacing the existing 001–009 guarantee.

## 10. Authorization and canonical-write enforcement

New eligible material requires a non-null tested workload and executor revision,
executor artifact = tested artifact, executor job = tested job, and executor source
revision = tested executor revision. Also require exact subject, current policy,
snapshot digest, source contract, gate policy, and verified review bindings.

Enforce this in Python construction and database material insertion, authorization
resolution, attempt admission, and final canonical-write revalidation. Historical
material is not edited, but legacy shape-only/null-workload material cannot bypass
the strengthened boundary to create a new eligible execution. Preserve existing
executor-to-material checks, approval/revocation checks, generation fencing, and
domain → release → work → event lock ordering. A stale policy, stale approval,
wrong build, or mismatched identity is rejected before canonical evidence writes.

No new authorization is granted by R1. Negative tests use isolated synthetic/test
records; they must not enable checked-in gates or authorize production workloads.

## 11. Verification acceptance contract

Use RED → minimal implementation → GREEN for each behavioral change. Preserve the
already-observed materialized replay RED evidence and rerun its GREEN regression.
Required acceptance cases include:

1. Materialized v2 artifact without legacy fields produces independent registry-bound results.
2. Forbidden legacy field, unknown capability, kind mismatch, unsupported extractor fail closed.
3. Expected diffs remain capability/extractor scoped and replay digests deterministic.
4. Missing policy and zero official evidence each yield NOT_READY for an ACTIVE capability.
5. Incomplete coverage never becomes READY through synthetic fixtures or manifest omission.
6. Relevant hash/semantics mutations change scoped digests; unrelated mutations/order do not.
7. Policy changes invalidate old V2 evidence; stale registration cannot authorize a write.
8. V1 snapshots remain parseable with identical canonical digests and immutable stored rows.
9. V1/V2 field confusion and direct SQL constraint bypass attempts fail.
10. Null tested workload, wrong artifact, wrong job, and wrong executor revision cannot authorize.
11. Review digest/content and semantic-binding mismatches fail in Python and PostgreSQL.
12. Claim/final-write checks reject stale policy, revoked approval, and stale worker generation.
13. Fresh 001→014 and upgrade 001–013→014 schemas are equivalent; seeded V1 history survives.
14. Existing legacy serving, CPI/governance, recovery, and migration regressions remain green.
15. Every checked-in capability gate remains BLOCKED; no production authorization is created.

Run corpus, materialization/review, policy/readiness, V1/V2, gate/authorization,
migration/legacy suites, relevant full Python tests, real isolated PostgreSQL tests,
compileall, and diff whitespace validation. Report skips and environmental failures,
not merely a green aggregate. After GREEN perform one independent R1 red-team pass
before claiming completion; resolve concrete findings without unrelated redesign.

## 12. Scope limits and handoff

Excluded: bulk BLS collection, Product API/UI, deployment, production IAM, reviewer
SoD, billing, notifications, changed CPI ingestion semantics, and platform refactors.
Do not reopen PR #39 or merge anything. Historical FOUNDATION_VERIFIED = YES is
distinct from R1 verification, RELEASE_ELIGIBLE, and production/customer readiness.

Expected release status remains **RELEASE_ELIGIBLE = NO**. Official evidence,
unfrozen coverage policies, and unresolved BLS access remain explicit blockers.
After written-spec approval, prepare an implementation plan and obtain its review
and execution-method selection before product code or migration implementation.

Self-review: scope is R1 only; current/proposed states are separated; no arbitrary
coverage minimums, synthetic official evidence, silent V1 reinterpretation, or
source-content/Git-revision equality is allowed. The effective-policy registration
is explicitly proposed here for approval, not represented as existing foundation.
