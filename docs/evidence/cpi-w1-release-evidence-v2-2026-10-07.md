# CPI W1 R1 local verification — Release Evidence V2

Execution dates: 2026-10-10 through 2026-10-11 (Asia/Seoul).
Branch: `feat/cpi-w1-official-corpus-readiness`.
Starting implementation commit: `64a9a7637ec27460f383d7bb918c5ac54f9cf5da`.
Independently reviewed final code commit: `075bb4563538fd5fd2202f09611fcd94ff8e106a`.
The subsequent evidence/plan-status commit changes documentation only.
This record concerns R1 only, not historical PR #39 or production readiness.

## Status

- Tasks 1–10 locally implemented, independently reviewed and tested.
- R1 `FOUNDATION_VERIFIED = YES`, bounded to this local foundation verification.
- `RELEASE_ELIGIBLE = NO`; all nine checked-in capability gates remain BLOCKED.
- No official downloads, production authorizations, push, merge or deployment.
- GitHub exact-new-head CI: **NOT RUN (no push permitted)**.

## Observed TDD evidence

Task 1: stale §31.2 ownership wording failed its assertion; corrected artifact ×
capability wording and preserved the prior materialized corpus replay correction.
Tasks 2–5: missing policy/readiness/review/V2 models and explicit negative binding
assertions failed, then passed with strict canonical bytes, policy obligations,
scoped digest/readiness evaluation, verified review files and V2 tested identities.
V1 review-to-new-authorization construction now explicitly rejects legacy use.

Task 6: missing DB objects failed 3 assertions; additional RED cases exposed absent
registration concurrency enforcement, unknown/missing/null policy fields, malformed
coverage obligations, review binding keys and missing repository writers. Strict
DB guards close these failures. Duplicate JSON keys are examined in JSON before
JSONB; Unicode is not normalized. Canonical vectors cover escaped decoded duplicate
keys, nested duplicates, noncanonical ordering, integers, forbidden float/exponent/
negative-zero forms and composed/decomposed Unicode.

Task 7: missing V2 authorization persistence failed; V1 attempt admission incorrectly
reached a uniqueness error (23505) instead of legacy retirement (23514), then passed
with explicit V2 admission proof. Exact artifact/job/executor revision mismatches
are rejected even when their candidate bytes and hashes are internally valid.
Historical V1 null-workload rows, material, APPROVED grant and control history are
seeded before 014 and preserved; after migration V1 grants cannot resolve or admit,
but revocation can be appended. Synthetic positive execution fixtures explicitly
bind their historical extractor/test policy; they are not official readiness proof.

Task 8: three RED failures demonstrated stale-policy final checks, heartbeat renewal
and direct canonical SQL insert. Additional RED proved work-before-release admission
lock inversion using `pg_blocking_pids` plus NOWAIT, not sleep-only inference.
Shared DB assertions now revalidate through final writes and heartbeat savepoints;
canonical SQL guards preserve the original lineage/fencing guards. Admission
discovers without work lock, then reselects under domain → release → work fencing.
Policy registration waits behind a final writer; subsequent writes reject stale
policy. Heartbeat closes the old claim into the existing PAUSED path; no auto-resume.

Task 9: missing foundation comparison failed; reapplying 014 failed with duplicate
function error. Migration reapplication now preserves schema/history and reinstalls
V2 guards after the legacy runner reapplies earlier migrations.

## Final local verification after independent-review corrections

| Verification | Run | Passed | Failed/errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| Full Python (`unittest discover -s tests -v`, PostgreSQL opt-out) | 814 | 608 | 0 | 206 |
| CPI Python discovery (`test_cpi_w1_*.py`, PostgreSQL opt-out) | 467 | 316 | 0 | 151 |
| Combined CPI/event-session/provider-bars/raw-lineage PostgreSQL | 181 | 181 | 0 | 0 |
| New V2 PostgreSQL + migration comparisons/manifest | 20 | 20 | 0 | 0 |
| Research identity/pipeline detail isolated PostgreSQL | 4 | 4 | 0 | 0 |
| Browser (`node --test tests/research_ui.cjs`) | 6 | 6 | 0 | 0 |

These rows overlap and must not be summed as unique tests. PostgreSQL assertions
were actually run, not inferred from skipped discovery. All database writes occurred
in generated UUID-named disposable test databases on the development test container;
the supplied database is used only as an administrative connection to create/drop
those exact test databases. Python compileall and whitespace checks passed.

Full discovery skips comprise 181 existing PostgreSQL cases, 14 new isolated V2
cases, two migration comparisons and four research cases, all actually verified
in the explicit DB runs above; five unrelated opt-in Kafka/Spark vertical-slice
and Paper journal/recovery cases were not run in R1. They are not counted as passed.

Fresh 001→014, legacy 001–009→014 and seeded foundation 001–013→014 have equivalent
schema snapshots, including functions, triggers, constraints and reference rows.
Original legacy market fixture and V1 business values are preserved. Both strict
001–009 manifest and additional 001–013 byte freeze pass. No old migration changed.

## Future production transition contract — not executed in R1

Stop promotion admission and all workers; drain or roll back canonical transactions.
Acquire the existing domain-exclusive maintenance barrier, inventory active V1
claims and record interruption/pause decisions. If quiescence is not proven, do not
apply 014. Preserve all evidence, attempt and authorization/control history.

After 014, V1 authorization is ineligible for admission, heartbeat and final writes.
Interrupted work cannot silently resume. Append revocation through existing control
flow as appropriate. Recovery requires separately reviewed V2 evidence/material,
effective registration, exact tested build and explicit operator resumption; never
convert or relabel V1 as V2. Verify hash history, schema, V1 refusal, V2 negative
guards and absence of new canonical writes before releasing maintenance fences.
The defensive active-V1 upgrade test intentionally checks an old running claim;
it is not permission to skip production drain or migration preconditions.
Promotion connection isolation must be READ COMMITTED before admission is reopened.
The shared V2 assertion rejects REPEATABLE READ and SERIALIZABLE; advisory locks
cannot refresh their pinned snapshots. No code silently changes transaction isolation.

## Release evidence remains unavailable

The companion inventory records 56 REMOTE_ONLY official artifacts, zero pinned
materialized artifacts, 111 artifact × capability inventory evaluations and four
synthetic conformance entries. Every ACTIVE capability is NOT_READY:

- BLS_CPI_SCHEDULE_HTML
- BLS_CPI_GLOBAL_ICS_SCHEDULE
- BLS_CPI_REVISED_RELEASE_DATES
- BLS_CPI_RELEASE_ENVELOPE_HTML
- BLS_CPI_CORE4_HTML
- BLS_CPI_TABLE1_REPRESENTATION_XLSX
- BLS_CPI_CORE4_XLSX
- BLS_CPI_CORRECTION_NOTICE_HTML
- BLS_CPI_CORRECTED_OBSERVATIONS_HTML

Six policies explicitly remain incomplete because their coverage contracts are
unfrozen. Missing official evidence is not a parser success. Review byte integrity
does not prove human identity/SoD; production IAM and customer readiness are out of
scope. No checked-in gate is changed or DB policy/authorization automatically seeded.

## Independent review and corrective TDD

One fresh-context read-only reviewer audited `54cd626ba2b68e442d2effee6cd4010e8963f708`.
The review was briefly interrupted by a credit error and resumed with the same
reviewer, not a replacement or second independent review. It found five important
and one minor issue. No finding was accepted on speculation: each was reproduced
and corrected within R1, then checked again by that same reviewer at `075bb456`.

| Finding | Observed RED | Correction commit | Final result |
| --- | --- | --- | --- |
| Stale ordinary/cancellation replay proof | Changed expected semantics/subject or wrong dependency hash/extractor still counted | `d95196a` | Exact current evaluation and byte/semantic bindings required |
| Missing conformance class or unlisted proof | Supplied PASS labels incorrectly produced READY | `d95196a` | Join fixture identity/hash/semantics and require every class |
| Configured policy differs from effective DB policy | Resolver returned the synthetic old-policy authorization; final assertion did not refuse it | `628a7ed` | Actual checkout policy reread, resolution refused and heartbeat paused |
| Stale transaction snapshot | Both REPEATABLE READ and SERIALIZABLE accepted stale-policy canonical INSERT | `075bb45` | Shared SQL assertion refuses with 23514; canonical row count zero |
| Incomplete cancellation dependency projection | Review/source-contract/replay mutations did not change digest | `e183473` | Relevant dependency subject, contract, review, replay and tags bound |
| Wrong expected-diff transition field names (minor) | Distinct before/after pairs falsely collapsed into one duplicate identity | `e183473` | Actual `expected_semantics_sha256` / `actual_semantics_sha256` used |

Independent correction-only closure: **all six findings closed; no unresolved
correctness finding**. Reviewer independently ran focused proof tests (48/48),
V2/migration checks (20/20), immutable hash checks, and both stale-isolation
reproductions; its databases were dropped and no checkout files were changed.

Default repository checks compare actual checked-in policy with effective DB state
at resolution and final/heartbeat validation, without registering anything. DB
guards independently enforce persisted effective-policy and exact-build relations;
they do not read repository files. Existing positive integration fixtures inject
explicit synthetic current-policy inputs; separate negatives use the real default
checkout config. Those test inputs never alter checked-in gates or authorize a live
workload. Historical pre-014 seeding explicitly emulates the old Python boundary;
the new check is restored before actual 014 transition/rejection assertions.

Exact tested workload, job contract and executor source revision remain distinct
and equal only to their corresponding V2 fields. Wrong/null build, mismatched review
bytes/bindings, stale registration, revoked approval and stale worker generation
are rejected by the relevant Python/SQL guards. V1 canonical bytes, null-build history
and control records are preserved, but no V1 authorization resumes or becomes V2.

The local ignored ledger/logs are under
`.superpowers/sdd/2026-10-07-cpi-w1-release-evidence-v2/`; `task10-*-red.log` captures
the corrective failures. Early task REDs documented above include some original
tool output rather than a dedicated saved log. Final logs contain actual unittest
run/pass/skip outputs; they are not inferred from CI. GitHub CI remains NOT RUN.

## Exact R1 changed-file inventory

Relative to starting implementation commit `64a9a763` (29 tracked paths):

```text
.github/workflows/ci.yml
config/cpi_w1_capability_evidence_policies.json
db/migrations/014_cpi_w1_release_evidence_v2.sql
db/migrations/immutable-001-013.sha256
docs/engineering/current-vs-target.md
docs/evidence/cpi-w1-release-evidence-v2-2026-10-07.md
docs/evidence/cpi-w1-release-evidence-v2-inventory-2026-10-07.json
docs/superpowers/plans/2026-10-07-cpi-w1-release-evidence-v2.md
docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md
scripts/replay_cpi_w1_corpus.py
src/cpi_w1_authorization.py
src/cpi_w1_evidence_policy.py
src/cpi_w1_evidence_readiness.py
src/cpi_w1_evidence_snapshot_v2.py
src/cpi_w1_migration_verification.py
src/cpi_w1_repository.py
src/cpi_w1_review_artifact.py
tests/integration/test_cpi_w1_evidence_v2_postgres.py
tests/integration/test_cpi_w1_postgres.py
tests/test_cpi_w1_authorization.py
tests/test_cpi_w1_corpus.py
tests/test_cpi_w1_evidence_policy.py
tests/test_cpi_w1_evidence_readiness.py
tests/test_cpi_w1_evidence_snapshot.py
tests/test_cpi_w1_evidence_snapshot_v2.py
tests/test_cpi_w1_migration_verification.py
tests/test_cpi_w1_release_evidence_docs.py
tests/test_cpi_w1_repository.py
tests/test_cpi_w1_review_artifact.py
```

No unrelated photographs, presentation outputs, older official-corpus evidence,
legacy ingestion implementation or migrations 001–013 were staged or modified.
Remaining release blockers are official bytes/access, six unfrozen coverage rules,
and out-of-scope production approval/deployment/customer readiness. No next product
phase is started. `FOUNDATION_VERIFIED = YES`; `RELEASE_ELIGIBLE = NO`.
