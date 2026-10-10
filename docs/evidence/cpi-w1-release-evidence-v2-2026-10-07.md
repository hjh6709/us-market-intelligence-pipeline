# CPI W1 R1 local verification — Release Evidence V2

Execution date: 2026-10-10. Branch: `feat/cpi-w1-official-corpus-readiness`.
Starting implementation commit: `64a9a7637ec27460f383d7bb918c5ac54f9cf5da`.
This record concerns R1 only, not historical PR #39 or production readiness.

## Status

- Tasks 1–9 locally implemented and tested; independent Task 10 review pending.
- R1 `FOUNDATION_VERIFIED` is **not yet asserted** until review findings are resolved.
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

## Local verification before independent review

| Verification | Run | Passed | Failed/errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| Full Python (`unittest discover -s tests -v`, PostgreSQL opt-out) | 802 | 600 | 0 | 202 |
| Combined CPI/event-session/provider-bars/raw-lineage PostgreSQL | 179 | 179 | 0 | 0 |
| New V2 PostgreSQL + migration comparisons/manifest | 18 | 18 | 0 | 0 |
| Research identity/pipeline detail isolated PostgreSQL | 4 | 4 | 0 | 0 |
| Browser (`node --test tests/research_ui.cjs`) | 6 | 6 | 0 | 0 |

These rows overlap and must not be summed as unique tests. PostgreSQL assertions
were actually run, not inferred from skipped discovery. All database writes occurred
in generated UUID-named disposable test databases on the development test container;
the supplied database is used only as an administrative connection to create/drop
those exact test databases. Python compileall and whitespace checks passed.

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

## Independent review and final exact-head verification

Pending Task 10. Do not treat this pre-review evidence as final foundation approval.
