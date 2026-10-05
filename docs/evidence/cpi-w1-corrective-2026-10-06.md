# CPI W1 three-blocker corrective verification — 2026-10-06

## Revision, authority and scope

- Existing PR #39; branch `docs/cpi-w1-data-governance-2026-09-19`.
- Independently fetched starting SHA: `a45ddeacdcfdb65f49a00f30fe067e480138cf55`.
- Tested implementation SHA: `15eee8b53d45b82d0b222b2e788f3a57223ecce2`.
- This evidence and status-ledger follow-up changes documentation/generated evidence
  only, not executable code or tests. The exact evidence-bearing revision is
  reproducibly identified with `git log -1 --format=%H -- docs/evidence/cpi-w1-corrective-2026-10-06.md`.
- Authority: accepted three-blocker corrective instructions and canonical design
  sections 39.2.1, 39.3, 39.5. No replacement architecture or product/runtime scope.
- The 2026-10-05 evidence remains historical; its verification does not establish
  these corrected invariants or verify a later executable revision.

## RED → minimum fix → GREEN

### 1. Structured promotion target identity

RED: actual PostgreSQL scheduling for one artifact, one capability and August plus
September raised `same ingestion work identity changed immutable metadata`.

Correction: immutable month-start `target_reference_month DATE`; COLLECT requires
NULL, PROMOTE requires non-NULL. Unique work identity is artifact + release subject
+ target month. Repository insert/resolution/comparison and Claim retain the exact
structured target. Scheduler passes the bound target explicitly. All six promotion
families reject candidate month mismatches; ReleaseSubject still excludes month.

Additional RED: a valid September row carrying the expected August operational key
was incorrectly returned as `ALREADY_SCHEDULED`. Existing-work lookup now consumes
structured subject/month identities and rejects contradictory operational metadata.
Draft NULL-target rows cannot satisfy scheduling.

GREEN: two distinct persisted work items converge on repeat scheduling; same triple
converges; different months stay distinct; PostgreSQL rejects NULL/invalid/non-null
COLLECT targets and target mutation; candidate mismatch and wrong-key/draft-NULL
scheduling fail closed. Reapplying migration 010 preserves targetless draft rows
without inventing a target from work_key. These rows require explicit reviewed repair,
not automatic canonical execution.

### 2. Wall-clock lease expiry, including local-row waits

RED: promotion began before expiry, waited on the real domain fence until expiry,
then succeeded without reclaim. Using `clock_timestamp()` instead of transaction
time fixed this initial-lock case.

Independent review identified a further RED: `FOR NO KEY UPDATE` held the attempt
row until expiry without obstructing canonical FK references. Final terminalization
had qualified its UPDATE before the lock wait and incorrectly committed afterward.
The minimum correction explicitly acquires the attempt row then rechecks ownership.
Current-claim validation also checks wall-clock validity after acquiring the work
row, and COLLECT heartbeat uses that locked check. Removing the COLLECT guard
reproduced renewal of an expired lease after a work-row-only wait.

GREEN: actual domain-wait and final-attempt-wait promotions raise StaleClaimError;
zero events/disclosures/observations/recovery-journal rows commit, and work/attempt
do not become successful. Work-lock-wait COLLECT heartbeat cannot extend the expired
lease. All CPI W1 lease validity/expiry/grant/renewal paths use wall-clock deadlines;
created_at/accepted_at/finished_at retain transaction-time semantics.

Final independent review found the same lock-only wait in retry and pause. Both
were reproduced RED with a real attempt `FOR NO KEY UPDATE` holder released after
expiry. Retry now locks the attempt then revalidates the current claim; the SQL pause
function locks WORK then ATTEMPT before evaluating wall-clock ownership. Both GREEN
cases leave work CLAIMED and attempt RUNNING, without retry/pause terminalization.

### 3. Attempt-bound heartbeat authorization

RED: approve A, claim under A, approve matching B, renew A: PostgreSQL raised
`ambiguous active promotion authorization`.

Correction: heartbeat validates its bound authorization, executor and runtime-control
decision through the existing exact-bound helper under DOMAIN → RELEASE_SUBJECT →
WORK fences. It does not resolve a replacement grant. New admission still uses the
ambiguous-set fail-closed resolver.

GREEN: B does not break A's renewal; revoking A pauses/terminalizes the A-bound
attempt even with B approved, without rebinding; a new ambiguous claim creates zero
attempts and leaves the work PENDING. Existing revocation and final-authorization
regressions also pass.

## Verification commands and observed results

Python 3.13; Java 21; local PostgreSQL. All mutation-capable DB tests used the newly
created dedicated `cpi39_fix_7622cef284c8` database, never the existing market DB.
Migration verification additionally creates its own isolated fresh/upgrade DBs.
No external data APIs or broker orders were invoked.

Use `DB=postgresql://market:market@localhost:55432/cpi39_fix_7622cef284c8` in the
commands below. Counts overlap and must not be summed as unique coverage.

| Check | Command | Observed result |
| --- | --- | --- |
| Blocker-related unit regressions | `.venv/bin/python -m unittest tests.test_collect_cpi_w1 tests.test_cpi_w1_repository tests.test_cpi_w1_event_migration tests.test_cpi_w1_ingestion_migration -q` | 85 run, OK |
| Full Python discovery | `.venv/bin/python -m unittest discover -s tests` | 749 run, OK, 184 skipped; DB cases run separately |
| CPI/governance/migration | `RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=$DB .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres tests.test_cpi_w1_migration_verification tests.test_cpi_w1_governance tests.test_cpi_w1_governance_migration -q` | 171 run, OK |
| Remaining PostgreSQL CI regressions | `RUN_POSTGRES_INTEGRATION=1 RUN_PAPER_POSTGRES_INTEGRATION=1 DATABASE_URL=$DB RESEARCH_TEST_DATABASE_URL=$DB .venv/bin/python -m unittest tests.integration.test_event_session_foundation_postgres tests.integration.test_postgres_market_bars tests.integration.test_validation_lineage_postgres tests.integration.test_research_identity tests.integration.test_pipeline_detail tests.integration.test_paper_execution tests.integration.test_paper_recovery -q` | 52 run, OK |
| Browser | `node --test tests/research_ui.cjs` | 6 passed |
| Compile | `.venv/bin/python -m compileall -q src scripts dags tests` | exit 0 |
| Patch whitespace | `git diff --check` | exit 0 |
| Official corpus | `.venv/bin/python -m scripts.replay_cpi_w1_corpus --inventory-only --output docs/evidence/cpi-w1-corrective-2026-10-06-corpus.json` | exit 0; official corpus NOT ready |

The initial sandbox-only discovery failed to start four Spark class fixtures with
JAVA_GATEWAY_EXITED. Normal local permissions resolved this environment failure.
An intermediate full run also exposed two outdated collector test-double signatures;
they were corrected to match the structured repository API before the final green run.

Fresh/001–009-upgrade target-schema equivalence and legacy fixture preservation pass.
The exact immutable 001–009 byte manifest passes; none of those files changed.
Draft-schema reapplication is separately exercised by the NULL-target regression;
it does not claim that targetless historical rows were semantically repaired.

## Independent review and remote acceptance gate

Separate spec and safety reviewers challenged the corrected working tree. They found
the scheduling shortcut and final/local-row lock timing cases above; those were
reproduced RED and corrected GREEN. The safety review of 4d62b426 additionally found
the retry/pause lifecycle gap documented above; it was not accepted as complete.
After the 15eee8b5 correction, the safety reviewer found no remaining concrete blocker
in that follow-up diff (reviewers inspected code/test source; database execution
evidence is the command table above). Exact-final-head review is an additional
delivery gate, not implied by a successful earlier review or CI run.

Exact-head GitHub CI is a separate required acceptance check. Query
`gh run list --repo hjh6709/us-market-intelligence-pipeline --commit <evidence-bearing SHA>`
and verify headSha and conclusion through `gh run view <id> --json headSha,status,conclusion`.
Do not substitute old successful run 37326624408. Current checks are accessible at
[PR #39 checks](https://github.com/hjh6709/us-market-intelligence-pipeline/pull/39/checks).
The final delivery report records the exact resulting SHA and matching CI run URL.

## Readiness separation

- Local foundation contract verification: PASS; merge readiness additionally requires
  exact-head remote CI and no concrete independent-audit blocker.
- RELEASE_ELIGIBLE = NO: 56 REMOTE_ONLY, 0 materialized/pinned official artifacts,
  9 BLOCKED capability decisions. Fixture approvals are not production approvals.
- PRODUCTION_DEPLOYED / CUSTOMER_AVAILABLE / MARKETING_APPROVED / LEGAL_APPROVED:
  not established by this corrective pass.
- PR remains draft; this work does not mark it ready or merge it.
