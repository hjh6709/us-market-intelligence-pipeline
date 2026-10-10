# CPI W1 Release Evidence V2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. Execution requires separate user approval; this document authorizes no implementation.

**Goal:** Close R1 release-evidence gaps with capability-specific policies/readiness, explicit V2 snapshots, and persisted exact-build authorization without releasing CPI W1.

**Architecture:** Extend the merged foundation through migration 014, preserving immutable V1 history and existing lock/fencing semantics. Immutable policy payloads plus append-only effective registrations bind V2 evidence to the current policy; verified local review bytes and exact tested executor identities guard authorization. All checked-in gates remain BLOCKED.

**Tech Stack:** Existing Python/unittest/psycopg, PostgreSQL (CI 17.6), canonical JSON and SHA-256; no new product dependencies.

**Spec:** `docs/superpowers/specs/2026-10-07-cpi-w1-release-evidence-v2-design.md` — approved by the user on 2026-10-07. Its original draft label records creation-stage status; this plan does not change the approved scope.

**Execution record (2026-10-11):** Tasks 1–10 completed locally after explicit
execution approval; one independent review's six findings resolved at code
`075bb456`. See the [R1 evidence](../../evidence/cpi-w1-release-evidence-v2-2026-10-07.md).
No push, merge, production authorization or next product phase. RELEASE_ELIGIBLE = NO.

## Global Constraints

- Branch `feat/cpi-w1-official-corpus-readiness`; inspected planning HEAD `64a9a7637ec27460f383d7bb918c5ac54f9cf5da`, foundation parent `8ae349afb41505dd99fb76562ae2b424e12fc71d`.
- Migrations 001–013 are immutable; only new `014_cpi_w1_release_evidence_v2.sql` may implement the approved schema delta.
- No bulk BLS downloads, gate ELIGIBLE changes, production authorization, push, merge, or reopening PR #39.
- `RELEASE_ELIGIBLE = NO`; historical FOUNDATION_VERIFIED is not fresh R1 verification.
- Preserve legacy serving, `src/cpi_ingestion.py`, pipeline_* semantics, and unrelated working-tree files.
- Every behavioral task: test → observed RED → minimal implementation → GREEN → regression → diff self-review → task-scoped commit, only after execution approval.
- Existing local replay corrections must not be reset to reproduce RED. Retain their historical RED evidence and rerun GREEN; new assertions still require new RED.
- Run tests against temporary fixtures and isolated disposable PostgreSQL databases, never authorize live execution or alter a customer database.
- Do not guess coverage thresholds, manufacture official evidence/reviews, or compare content hashes with Git revisions.

## Review Focus

## Approved execution clarifications — 2026-10-10

Tasks 6–8 are one connected authorization-enforcement unit. Their intermediate
local commits are NOT release-qualified; migration 014 must not be applied outside
disposable test databases until all three tasks and their regressions are verified.
Migrations 001–013 and all existing fencing/security invariants remain unchanged.

Python/PostgreSQL canonical vectors must cover duplicate keys before JSONB conversion,
Unicode composed/decomposed forms (distinct bytes; no normalization), escapes,
integer/alternate numeric representations and forbidden floating-point values,
nested objects, deterministic key ordering, and order independence. Validate raw
JSON with a duplicate-aware parser before JSONB; a JSONB-only check is insufficient.

Future production transition preconditions (documentation/tests only in R1): stop
new promotion admission and all promotion workers, drain/rollback in-flight canonical
transactions, acquire the existing domain-exclusive barrier, inventory active V1
claims, and record interruption/pause decisions before applying 014. If drain cannot
be proven, do not apply. Preserve immutable historical authorizations/attempts.
After migration, V1 grants are ineligible for admission, renewal, and final writes;
interrupted V1 work remains paused/ineligible and must never silently resume. Revoke
old grants through the existing append-only control flow where applicable; recovery
requires separately reviewed V2 evidence/authorization and explicit operator action,
not V1 conversion. Verify schema/hash history, V1 rejection, V2 guards, and absence
of new canonical writes before releasing the maintenance barrier. This is not a
production deployment or an authorization to create production grants during R1.

Tasks 6/8/9 must exercise this transition with an isolated pre-014 database containing
active V1 claims, preserved historical rows, interrupted work, and post-014 rejection.

Task 10 verified enforcement detail: V2 promotion authorization checks require
READ COMMITTED transactions. REPEATABLE READ and SERIALIZABLE pin snapshots which
advisory locks do not refresh; the shared 014 assertion rejects them rather than
letting an old policy/control snapshot authorize canonical writes. Collection and
historical reads are not redefined. Future migration recovery must verify promotion
connection isolation before reopening admission; no isolation is silently changed.

### Original review focus

1. Concurrent policy registrations using the same expected version must have one winner, not two effective policies — Task 6.
2. A review symlink, duplicate JSON key, or valid hash with wrong semantic bindings must not be accepted — Tasks 4 and 6.
3. A historically APPROVED V1 authorization must not admit a new claim or finish an old claim after 014 — Tasks 7 and 8.
4. A claimed cancellation or shared artifact must not silently remove a monthly obligation or contaminate another capability's digest — Tasks 2 and 3.
5. Policy change between claim and canonical write must be observed under the existing lock order — Task 8.

## File ownership map

New focused modules:

- `src/cpi_w1_evidence_policy.py`: policy schema/registry and canonical serialization contract.
- `src/cpi_w1_evidence_readiness.py`: capability obligations, scoped digests, deterministic readiness.
- `src/cpi_w1_review_artifact.py`: immutable verified local review and semantic binding.
- `src/cpi_w1_evidence_snapshot_v2.py`: V2 model/parser; V1 module remains historical.
- `db/migrations/014_cpi_w1_release_evidence_v2.sql`: all new DB objects and replacement guards.
- `config/cpi_w1_capability_evidence_policies.json`: all nine ACTIVE policies.
- `db/migrations/immutable-001-013.sha256`: additional foundation byte freeze.

Modify existing replay/report, gate/authorization/repository, migration verification,
canonical section 31.2, and current-vs-target documentation only where R1 requires.
Keep large repository files intact; no unrelated extraction/refactor. New tests use
matching focused module names and `tests/integration/test_cpi_w1_evidence_v2_postgres.py`.

## Fixed implementation contracts

### A. Canonical JSON and policy DB representation

Define `canonical_evidence_bytes(value: object) -> bytes` in the policy module:
UTF-8, sorted keys, compact separators, unescaped non-ASCII, no trailing newline,
no floats/NaN, duplicate keys rejected on parsing, string keys only. Integers are
allowed only for explicit version/count fields; policy set fields are normalized
and sorted before serialization. Hash exact returned bytes, not JSONB display text.

`promotion_capability_evidence_policies` stores immutable `policy_digest` PK,
`release_subject_digest`, `promotion_capability_id`, schema/version, completeness, canonical payload
BYTEA, and parsed payload JSONB. Exact unique reference is
`(policy_digest, release_subject_digest, promotion_capability_id)`.
DB verifies byte hash, UTF-8 parse, supported schema/keys, JSON-to-byte canonical
equality, and agreement of relational columns with the payload. Use PostgreSQL
built-in SHA-256; add no extension. Implement a private canonical JSON renderer in
014 for the restricted payload vocabulary, with shared Python/SQL test vectors.

`promotion_capability_evidence_policy_registrations` stores immutable registration
UUID, `release_subject_digest`/`promotion_capability_id`, policy digest, `expected_registration_version`,
`registration_version`, actor, and timestamp. Unique `(release_subject_digest, registration_version)`;
exact FK to the policy relation. The initial version is 1 with expected 0; subsequent
versions are exactly previous+1. Highest committed version is effective. No UPDATE,
DELETE, mutable current-policy flag, timestamp ordering, or latest-wins conflict
resolution for official evidence. This versioned configuration relation is not a
release approval and does not change evidence validity.

`register_cpi_capability_evidence_policy(subject, capability, policy_digest,
expected_version, actor) -> registration UUID` takes domain-shared then exact
release-subject transaction locks, checks the expected version, and inserts once.
A trigger enforces the same discipline for direct SQL. Stale expected version
rejects even for identical digest; callers reuse the returned registration rather
than silently retrying. Registrations require a stored policy matching subject and
capability; INCOMPLETE policies may be registered but cannot authorize promotion.
No registration or gate seed rows are created by 014. Missing effective registration
fails closed. Repository/config comparison must reject a checked-in current policy
that differs from the effective DB registration; do not auto-register to cure it.

### B. ReviewArtifactV1 canonical binding

Review source is a repository-local canonical JSON file. The verified object stores
`review_ref`, exact raw bytes, byte digest, and parsed payload. Review payload has
schema `cpi-w1-review-artifact-v1`, `purpose`, and `bindings`:

- Authorization purpose `PROMOTION_AUTHORIZATION`: subject, capability, policy digest,
  evidence snapshot digest, source-contract digest, tested source-content digest,
  executor source revision, tested workload digest, tested job, and gate policy version.
- Expected-diff purpose `EXPECTED_DIFF`: subject/capability, artifact hash, extractor
  version, prior expected semantic digest, and new actual semantic digest.

Use exact schema-dependent keys. Authorization bindings cannot contain null tested
build identities. Do not bind the authorization-material digest or a gate digest
that itself includes this review digest: that creates a hash cycle. The material
separately binds its gate digest and the verified review digest.

`load_review_artifact(repo_root: Path, review_ref: str, claimed_digest: str) -> ReviewArtifactV1`
requires a canonical repository-relative path, rejects absolute/traversal paths and
symlink components, requires a regular file, verifies raw SHA-256 and canonical JSON
bytes, and rejects unsupported schema/purpose/duplicate keys. `require_bindings(expected:
Mapping[str, object]) -> None` compares the entire exact binding set. Byte integrity
does not prove reviewer identity/SoD. Existing prose-only historical references remain
historical; do not synthesize approval JSON on their behalf.

DB `promotion_release_review_artifacts` stores immutable `(review_ref, review_digest)`
exact reference, bytes, parsed schema/purpose/bindings. Its guard verifies hash,
canonical encoding, and parsed-column equality. V2 material's composite FK and guard
verify the review bindings against joined evidence, policy, subject, executor, and
gate-policy fields. A correct hash of an unrelated review is insufficient.

### C. V1/V2 and authorization boundary

V1 parsers, canonical serialization/digests, and historical reads remain unchanged.
V1 dataclass construction may represent existing history; it is not an execution
approval. Retire V1 `from_review` for new eligible authorization with explicit
`LEGACY_AUTHORIZATION_RETIRED`; repository V1 material/grant creation and direct SQL
new V1 material/grant inserts reject. Existing V1 rows, revocation/control history,
and historical lookup remain readable. Resolver excludes V1; attempt admission,
heartbeat/final-write revalidation reject V1. Do not edit or delete old records,
silently upgrade them, or compare V1 content identity with an executor revision.
Existing already-claimed legacy promotion loses execution eligibility; preserve
the existing pause/error behavior, never resume it automatically. Legacy market
serving and non-promotion collection are unaffected. Tests that previously authorized
V1 promotion must use explicit V2 test fixtures, while preserving V1 history tests.

V2 evidence has schema `cpi-w1-promotion-evidence-snapshot-v2` and evidence policy
`cpi-w1-evidence-v2`. V2 material uses `cpi-w1-authorization-v2` and its own schema;
keep existing gate policy unchanged. Evidence may be blocked with null workload or
executor revision; eligible material cannot. V2 policy and source-contract references
must match the verified capability registry/subject relation. Source-content digest
and executor revision remain separate fields.

### D. Persistence and enforcement surface

Extend `promotion_release_evidence_snapshots`: add capability/policy/scoped-corpus,
source-contract/content, and tested-executor-revision columns. V1 requires old fields
and NULL V2 fields; V2 requires new fields and NULL legacy global corpus/revision.
Replace relaxed NOT NULLs with schema-conditional checks. Preserve immutable triggers
and exact `(id, subject, digest)` FK. Validate exact canonical V2 payload/digest at
insertion; reject unknown schema or mixed fields. Do not rewrite V1 data.

Extend material with explicit V2 schema/policy and verified review FK. One reusable
DB assertion `assert_cpi_v2_authorization_binding(authorization_id UUID) -> VOID`
joins evidence, material, review, policy, effective registration, and current approval;
checks tested artifact/job/revision exactly. A lower-level material assertion is
used before a grant exists. Python constructor compares current policy/config/source
contract and review bindings; DB checks persisted relations independently.

Replace resolver and attempt guard in 014 without weakening existing signatures or
executor checks. Repository final assertion calls the shared DB assertion while
holding existing domain → release → work → event locks. Policy registration uses
domain → release only. Check SQL canonical-write guard entrypoints as well as Python;
do not permit direct SQL to bypass final policy validation. No new lock namespace,
lock inversion, grants, SECURITY DEFINER shortcut, or production authorization.

---

### Task 1: Preserve replay correction and reconcile canonical wording

**Files:** existing `scripts/replay_cpi_w1_corpus.py`, `tests/test_cpi_w1_corpus.py`, `tests/test_cpi_w1_evidence_snapshot.py`; section 31.2 of canonical 2026-09-19 design; new `tests/test_cpi_w1_release_evidence_docs.py`.
**Interfaces:** retain `replay_entry(...)`/`build_report(...)` signatures and registry dispatch; later tasks consume their result dictionaries.

- [x] Add docs assertions for artifact × capability ownership and absence of stale entry-scoped wording; record RED using `.venv/bin/python -m unittest tests.test_cpi_w1_release_evidence_docs -v`.
- [x] Run `tests.test_cpi_w1_corpus.CpiW1MaterializedV2ReplayTest`; retain existing observed historical RED record, do not revert product code.
- [x] Amend only section 31.2 and minimally fix any newly failing replay assertion; preserve all nine required replay cases.
- [x] GREEN: `.venv/bin/python -m unittest tests.test_cpi_w1_release_evidence_docs tests.test_cpi_w1_corpus tests.test_cpi_w1_evidence_snapshot -v`.
- [x] Review exact diff and whitespace; commit only Task 1 files as `fix: reconcile capability-scoped CPI corpus replay`. Do not stage other evidence/photos.

### Task 2: Canonical policies for every ACTIVE capability

**Files:** create policy module/config and `tests/test_cpi_w1_evidence_policy.py`.
**Interfaces:** `CapabilityEvidencePolicyV1.from_mapping(value: Mapping[str, object], registry: PromotionCapabilityRegistry) -> CapabilityEvidencePolicyV1`; `.policy_digest`, `.complete`, `.payload()`; `CapabilityEvidencePolicyRegistry.require(capability_id: str) -> CapabilityEvidencePolicyV1`; canonical bytes helper from contract A.

- [x] RED tests: missing ACTIVE policy; duplicate/mismatched subject; unknown keys; reordered sets stable; float/NaN rejected; incomplete coverage explicit; nine policies all forbid zero official evidence.
- [x] Run `.venv/bin/python -m unittest tests.test_cpi_w1_evidence_policy -v`; observe missing implementation/assertion failure, not environment failure.
- [x] Implement strict models and exact nine-policy coverage matrix from the spec. Encode monthly interval obligations and verified cancellation requirements; six unfrozen surfaces remain INCOMPLETE.
- [x] GREEN/regression: `.venv/bin/python -m unittest tests.test_cpi_w1_evidence_policy tests.test_cpi_w1_promotion_capabilities tests.test_cpi_w1_release_subject -v`.
- [x] Self-review threshold/source claims and commit Task 2 files as `feat: add fail-closed capability evidence policies`.

### Task 3: Scoped digests and readiness report

**Files:** create readiness module and `tests/test_cpi_w1_evidence_readiness.py`; modify replay report and corpus tests.
**Interfaces:** `capability_corpus_snapshot_digest(manifest: Mapping[str, object], registry: PromotionCapabilityRegistry, policy: CapabilityEvidencePolicyV1) -> str`; `evaluate_capability_readiness(registry, policies, manifest, replay_results, conformance_results, approvals) -> tuple[CapabilityEvidenceReadiness, ...]`. Result fields match spec section 5; dependencies are explicit projections, not whole-registry hashes.

Pin tests `test_missing_active_policy_is_not_ready` and `test_unrelated_capability_mutation_preserves_digest` to these assertions (fixture factories belong in this test module):

```python
self.assertEqual(result.status, "NOT_READY")
self.assertIn("POLICY_MISSING", result.blocking_reasons)
self.assertEqual(before_digest, unrelated_mutation_digest)
self.assertNotEqual(before_digest, relevant_hash_mutation_digest)
```

- [x] RED tests: missing policy/zero official/empty required set, omitted ACTIVE expectation, synthetic-only evidence, incomplete policy, unverified cancellation, missing January coverage, duplicate result identities.
- [x] RED digest tests: unrelated capability expectation change stable; relevant hash/semantic/tag/dependency change differs; ordering stable. Run `.venv/bin/python -m unittest tests.test_cpi_w1_evidence_readiness -v`.
- [x] Implement filtered canonical corpus/diff/replay projections and obligation evaluation; supplement existing report with all ACTIVE rows, never mutate gates or fetch URLs.
- [x] GREEN: `.venv/bin/python -m unittest tests.test_cpi_w1_evidence_readiness tests.test_cpi_w1_evidence_policy tests.test_cpi_w1_corpus -v`; inventory report must show nine NOT_READY, zero official materialized, no network.
- [x] Review scope independence and commit `feat: report capability-scoped CPI evidence readiness`.

### Task 4: Verified review bytes and exact semantic binding

**Files:** create review module and `tests/test_cpi_w1_review_artifact.py`; minimally modify expected-diff review validation in replay; retain capture-review contract.
**Interfaces:** `ReviewArtifactV1`, loader and binding assertion from contract B; expected-diff validation receives verified review objects, not trusted caller-provided hashes.

Pin `test_valid_hash_wrong_bindings_rejected`: the loader succeeds for real canonical bytes, then `review.require_bindings(expected)` raises `ReviewArtifactError` because its policy digest differs. Do not make this only a hash-shape test.

- [x] RED tests: canonical authorization/diff review accepted; wrong bytes/bindings rejected; missing file, traversal, symlink, noncanonical encoding, duplicate JSON keys, unknown purpose rejected; whitespace changes digest.
- [x] Run `.venv/bin/python -m unittest tests.test_cpi_w1_review_artifact -v` and observe behavioral RED.
- [x] Implement immutable helper and local approval checks without approving any checked-in evidence; historical prose references remain explicitly unverified.
- [x] GREEN/regression: `.venv/bin/python -m unittest tests.test_cpi_w1_review_artifact tests.test_cpi_w1_corpus_review tests.test_cpi_w1_corpus_materialization tests.test_cpi_w1_corpus -v`.
- [x] Self-review cyclic hash/path handling and commit `feat: verify CPI review artifact bytes and bindings`.

### Task 5: V2 snapshots and pure eligible-material validation

**Files:** create snapshot V2 module/tests; modify authorization and release-gate modules/tests; preserve V1 serialization module.
**Interfaces:** `PromotionEvidenceSnapshotV2.from_mapping(value) -> PromotionEvidenceSnapshotV2`; `.payload()`, `.canonical_json`, `.evidence_snapshot_digest`; `PromotionAuthorizationMaterialV2.from_review(*, evidence, gate_decision, executor, current_policy, source_contract_digest, review: ReviewArtifactV1) -> PromotionAuthorizationMaterialV2`.

- [x] RED V2 tests: exact schema/digest roundtrip, null build readable only as blocked, unknown/mixed fields rejected, current-policy/source-contract mismatch rejected; V1 frozen digest unchanged.
- [x] RED eligible-material tests: null workload/revision, wrong artifact/job/revision, wrong review binding, stale policy fail; equal source-content hash is not executor-revision evidence. Run `.venv/bin/python -m unittest tests.test_cpi_w1_evidence_snapshot_v2 tests.test_cpi_w1_authorization -v`.
- [x] Implement explicit V2 models and gate validation using scoped evidence. Retire V1 review-to-new-authorization constructor per contract C, without changing V1 historical serialization.
- [x] GREEN/regression: run V1/V2 snapshot, policy, review, gate, authorization suites together; all actual config gates remain BLOCKED.
- [x] Self-review no implicit V1 coercion and commit `feat: bind CPI V2 evidence to reviewed tested builds`.

### Task 6: Migration 014 immutable evidence/policy/review persistence

**Files:** create 014, integration test module, immutable-001-013 manifest; modify repository explicit writers and migration verification/tests.
**Interfaces:** repository `create_promotion_evidence_snapshot_v2(connection, snapshot, created_by_subject) -> UUID`; `store_capability_evidence_policy(connection, policy) -> str`; `register_capability_evidence_policy(connection, policy, expected_version: int, actor: str) -> UUID`; `store_review_artifact(connection, review) -> tuple[str, str]`. SQL objects/registration interface per A/D.

Pin `test_concurrent_expected_version_has_one_winner`: synchronize two registration transactions with expected 0; collect one UUID and one constraint failure. Query `MAX(registration_version)` and `COUNT(*)` for the subject; both must equal 1. Pin `test_review_json_columns_cannot_disagree_with_bytes` with direct SQL INSERT whose bytes hash is valid but parsed bindings differ; require a constraint exception.

- [x] RED real PG tests for new objects, schema-discriminated rows, payload/hash mismatches, review semantic-column tampering, immutable UPDATE/DELETE, missing FK and unsupported versions.
- [x] RED concurrency test: two independent connections register expected 0; one succeeds, one rejects, effective version exactly 1. Subsequent stale version and registration without policy reject.
- [x] RED fresh/upgrade + hash-freeze tests; run `RUN_POSTGRES_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_cpi_w1_evidence_v2_postgres tests.test_cpi_w1_migration_verification -v` with an approved isolated test DSN.
- [x] Implement 014 tables, schema changes, canonical byte/hash guards, registration function/trigger, immutable triggers and explicit repository writers. Extend hash loader without loosening original exact 001–009 manifest validation.
- [x] GREEN using the same command; Python/SQL canonical vectors include Unicode, escaping, reordered keys, integers, forbidden floats, and parsed-field disagreement. No seeded policy/gate/grant rows.
- [x] Self-review lock acquisition in direct SQL, row/history preservation and commit `feat: persist versioned CPI evidence policies and V2 snapshots`.

### Task 7: Persisted authorization guards and V1 runtime retirement

**Files:** extend new 014; modify repository/authorization tests; extend V2 PG integration and existing CPI PG fixtures.
**Interfaces:** repository `create_promotion_authorization_material_v2(connection, material, evidence_snapshot_id, created_by_subject) -> UUID`; SQL material/bound-authorization assertions per D; existing resolver signature retained.

- [x] RED direct SQL and Python cases: null tested workload, wrong artifact/job/revision, nonexistent/current-policy mismatch, wrong review, V1 new material/grant rejected.
- [x] RED historical test: seed V1 evidence/material/APPROVED grant/control before applying 014; afterward bytes/rows readable and unchanged, resolver returns no V1 authorization; V1 revocation history still appendable.
- [x] Run focused authorization + real PG suites; confirm invariant failures, not missing DSN/skips.
- [x] Implement V2 material/grant assertions, resolver and attempt admission replacement guards; convert only execution-positive test fixtures to explicitly reviewed V2 fixtures. Preserve historical V1 tests and collection/legacy tests.
- [x] GREEN/regression: authorization, repository, gate and both CPI PostgreSQL modules; compare all seeded V1 business values before/after migration.
- [x] Self-review every entrypoint and commit `fix: enforce tested-build authorization and retire legacy promotion grants`.

### Task 8: Policy freshness through heartbeat and final canonical writes

**Files:** modify repository `assert_current_promotion_authorization`/heartbeat use and promoter guard integration; extend new 014 final SQL guards only as required; repository/promoter/PG tests.
**Interfaces:** shared DB authorization assertion from Task 7 remains sole persisted invariant predicate; existing Claim identity and pause workflow unchanged.

- [x] RED two-connection tests: claim under registration 1, commit registration 2, then canonical promotion rejected with no canonical writes; old heartbeat pauses through existing behavior. V1 claimed pre-upgrade cannot finish.
- [x] RED serialized race: final writer holds domain/release lock before policy registration; registration waits, then any subsequent write sees version 2. Test reverse order too; no sleep-only proof, use synchronization barriers and bounded lock timeouts.
- [x] RED revocation/stale-generation/direct SQL bypass regressions; run focused repository/promoter and PG suites.
- [x] Implement common final revalidation under existing domain → release → work → event order; prevent indirect writes from avoiding it. No new pause semantics or automatic policy approval.
- [x] GREEN/regression includes existing recovery/deadlock/fencing CPI tests; verify rejected transaction leaves no observations/envelope promotion records.
- [x] Self-review lock-order paths and commit `fix: fence CPI canonical promotion against stale release policy`.

### Task 9: Fresh/upgrade equivalence, legacy preservation, and CI coverage

**Files:** migration verification module/tests; existing/new PG modules; `.github/workflows/ci.yml` only to include new PG module; current-vs-target, R1 evidence document.
**Interfaces:** retain `compare_fresh_and_upgrade(base_dsn)` legacy 001–009 path; add `compare_foundation_upgrade(base_dsn: str) -> MigrationEquivalenceResult` for seeded 001–013→014 path with explicit historical preservation result.

- [x] RED comparison tests missing new constraints/functions/triggers or mutated V1 values; preserve original legacy fixture and test null-workload historical row. Schema snapshot includes new policy/review tables, guard functions and constraints.
- [x] Implement isolated fresh/upgrade comparisons, complete 001–013 frozen manifest verification, and new PG CI test inclusion; never alter old migration bytes.
- [x] GREEN full commands below, capturing actual run/pass/skip counts and why each skip occurred. PostgreSQL tests cannot be counted as verified if skipped.
- [x] Generate deterministic all-nine capability inventory and evidence under `docs/evidence/cpi-w1-release-evidence-v2-2026-10-07.md`; distinguish old replay verification from this run. Update current-vs-target only for tested implementation, keeping official access and release blockers explicit.
- [x] Self-review evidence assertions against logs and commit `test: verify CPI V2 migration and legacy compatibility` with scoped docs/CI changes.

### Task 10: One independent R1 red-team and final handoff

**Files:** evidence report; code only if a concrete R1 finding requires corrective TDD.
**Interfaces:** completed branch + exact HEAD, spec and logs are audit inputs; no unrelated architecture review.

- [x] Dispatch one independent reviewer after GREEN, as explicitly authorized by R1, to try to disprove readiness closure, policy invalidation, digest scope, exact build, review binding, V1 isolation, migration history, and blocked gate preservation.
- [x] Each concrete finding gets its own observed RED, minimum correction, GREEN/regression and scoped commit; do not expand feature scope.
- [x] Re-run affected and final verification at the exact post-correction HEAD; save audit verdict/counts. An unresolved correctness finding prevents completion claims.
- [x] Return R1 required report: starting/final SHA, branch, exact files, 014 necessity, RED/fixes/GREEN, per-capability output, V1/V2 compatibility, authorization negative tests, blockers and CI status. No push means GitHub new-head CI is NOT RUN, not green by inference.
- [x] Stop: `RELEASE_ELIGIBLE = NO`; do not push/merge or begin a next product phase without new user instruction.

## Final verification commands (execution phase only)

Use `.venv/bin/python`, existing locked environment and an isolated test PostgreSQL
DSN supplied at execution. Do not put secrets in logs. Do not run these during
plan authoring or treat their presence here as permission to change a database.

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_cpi_w1_*.py' -v
.venv/bin/python -m unittest discover -s tests -v
RUN_POSTGRES_INTEGRATION=1 .venv/bin/python -m unittest tests.test_cpi_w1_migration_verification tests.integration.test_cpi_w1_postgres tests.integration.test_cpi_w1_evidence_v2_postgres -v
RUN_POSTGRES_INTEGRATION=1 .venv/bin/python -m unittest tests.integration.test_event_session_foundation_postgres tests.integration.test_postgres_market_bars tests.integration.test_validation_lineage_postgres -v
.venv/bin/python -m unittest tests.integration.test_research_identity tests.integration.test_pipeline_detail -v
node --test tests/research_ui.cjs
.venv/bin/python -m compileall -q src scripts dags tests
git diff --check
```

Inventory command, after the evidence directory exists in the execution phase:

```bash
.venv/bin/python -m scripts.replay_cpi_w1_corpus --inventory-only --output docs/evidence/cpi-w1-release-evidence-v2-inventory-2026-10-07.json
```

Research integration uses existing `RESEARCH_TEST_DATABASE_URL`; PostgreSQL runs
require `DATABASE_URL` and `RUN_POSTGRES_INTEGRATION=1`. Preserve existing replay CLI
arguments; inventory mode may return 0 while reporting NOT_READY, whereas enforcing
release-readiness mode must return nonzero. Capture both exit-code semantics and
no-network assertions in tests.

## Plan self-review and approval boundary

Spec sections 3–6 map to Tasks 1–3; review section 8 to Task 4; identity/V1 section 7
to Tasks 5/7; migration section 9 to Tasks 6/9; authorization/locking section 10 to
Tasks 7/8; verification/remaining blockers sections 11–12 to Tasks 9/10. Every review
focus has an owning negative test. No product implementation or migration application
has been performed in this planning turn. Existing dirty work remains untouched.

Recommended execution: native task-by-task implementation, because SQL and Python
guards share tightly coupled invariants, followed by the separately authorized
independent R1 red-team. Alternatively use subagent-driven execution if the user
selects it. Wait for written-plan approval and execution-method choice before either.
