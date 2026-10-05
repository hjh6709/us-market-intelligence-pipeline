# CPI W1 accepted foundation verification — 2026-10-05

## Revision and scope

- PR: #39, `docs/cpi-w1-data-governance-2026-09-19`.
- Remote baseline fetched: `b24eb6436494d0081a35b4098d222f0c20fd7ed3`.
- Phase 13: `c62a96973d10b515eeb96df05d524534123a00f3`.
- Tested implementation revision / Phase 14: `8815136e2be39d79b4ea25b48bdb8b95598f0d6c`.
- Fetched main: `83e0259f2d587a50e5b670dc521e9a9b75283442`.
- Later documentation-only evidence commits do not change tested executable code.
- Scope: accepted CPI W1 foundation deltas in additive migrations 010–013,
  contracts, repository, promoter, selector, governance, orchestration and tests.
  Migration 001–009 byte identity is independently verified.

## Phase 14 behavior and RED/GREEN

Same-transaction recovery journal covers event creation, schedule, disclosure,
disclosure links/artifacts, markers, observations, interpretation decisions and
serving-control decisions. Queue-only bookkeeping is excluded.

Immutable snapshots bind the committed journal watermark, deterministic state
digests, safety counts, operator and review references. Domain enable requires the
exact snapshot FK and zero unsafe-uncontained events under the exclusive domain
fence. Individually WITHHELD unresolved events may remain when domain containment
is removed. Event fingerprints retain their event-level role.

RED: `test_domain_reenable_rejects_stale_recovery_snapshot` failed with
`AssertionError: SerializationFailure not raised` when only a new canonical event
was created after a snapshot. Minimum correction: journal event creation too.
GREEN: the same test and the complete suites below passed. Original structural
RED failures preceded the Phase 14 implementation; corrected source-inspection
tests restrict lock-order checks to the relevant method.

## Verification commands and observed results

Environment: local Python 3.13 virtualenv, local PostgreSQL test service on port
55432, Spark local mode. Database suites mutate isolated test fixtures. No external
market-data requests or broker orders were made in these verification commands.

| Check | Command | Observed result |
| --- | --- | --- |
| Full Python unit/regression discovery | `.venv/bin/python -m unittest discover -s tests` | 736 run, OK, 171 skipped; DB suites exercised separately |
| CPI W1 + migration + governance | `RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres tests.test_cpi_w1_migration_verification tests.test_cpi_w1_governance tests.test_cpi_w1_governance_migration` | 158 run, OK: 115 CPI PostgreSQL, 4 migration, 39 governance |
| Remaining CI DB regression | `RUN_POSTGRES_INTEGRATION=1 RUN_PAPER_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market RESEARCH_TEST_DATABASE_URL=postgresql://market:market@localhost:55432/market .venv/bin/python -m unittest tests.integration.test_research_identity tests.integration.test_pipeline_detail tests.integration.test_event_session_foundation_postgres tests.integration.test_postgres_market_bars tests.integration.test_validation_lineage_postgres tests.integration.test_paper_execution tests.integration.test_paper_recovery -v` | 52 run, OK |
| Research browser semantics | `node --test tests/research_ui.cjs` | 6 passed |
| Gate, snapshot and corpus regression after doc update | `.venv/bin/python -m unittest tests.test_cpi_w1_release_gate tests.test_cpi_w1_evidence_snapshot tests.test_cpi_w1_corpus` | 24 run, OK |
| Compile | `.venv/bin/python -m compileall -q src dags tests` | exit 0 |
| Whitespace | `git diff --check` | exit 0 |
| Official inventory and semantic conformance | `.venv/bin/python -m scripts.replay_cpi_w1_corpus --inventory-only --output docs/evidence/cpi-w1-foundation-2026-10-05-corpus.json` | inventory command exit 0; official corpus NOT ready; conformance ready |

Counts overlap; do not add them as unique test coverage.

Migration verification creates separate fresh and legacy-upgrade databases,
compares their target schemas and verifies legacy fixture preservation. The exact
001–009 hashes live in `db/migrations/immutable-001-009.sha256` and passed byte
verification. Reusing one test database is not the migration-equivalence proof.

## Direct semantic completion assessment

YES means implemented and exercised by repository tests, not production proof.

| Accepted foundation condition | YES/NO | Evidence |
| --- | --- | --- |
| Reference-month event identity and separated evidence/governance/serving | YES | event, selector, governance suites |
| Structured exact release subject | YES | release-subject contracts/tests |
| Capability registry with scoped approvals | YES | capability/gate tests |
| Evidence snapshot distinct from approval and authorization | YES | evidence-snapshot/gate tests |
| Artifact target is collector-owned and immutable | YES | Phase 13 PostgreSQL tests |
| Capability-aware deterministic corpus replay | YES | corpus regression and attached inventory |
| Official corpus materialized and fully ready | NO | 56 REMOTE_ONLY, 0 pinned official entries |
| Work identity binds artifact, capability and subject | YES | repository and PostgreSQL constraints |
| Runtime grant/control/executor binding | YES | authorization and attempt-binding tests |
| Revocation checked at final canonical mutation | YES | revoked-authorization final-commit test |
| Admission pause creates no attempt | YES | pending pause and unauthorized admission tests |
| Owned pause closes attempt and retains provenance | YES | claim/reclaim/heartbeat tests |
| Approval does not auto-resume PAUSED work | YES | explicit approval/resume test |
| Global domain/release/work/event lock order | YES | promoter order and PostgreSQL regression |
| Duplicate scheduling has one global work set | YES | concurrent promotion scheduling test |
| Stale workers cannot write canonical evidence | YES | reclaim and stale promoter tests |
| Same-transaction journal excludes queue metadata | YES | journal + rollback + queue tests |
| Snapshot immutable and bound to watermark/digests | YES | snapshot constraints and governance tests |
| Domain enable rejects stale and unsafe snapshots | YES | actual PostgreSQL tests |
| Domain enabled differs from all events enabled | YES | contained unresolved event recovery test |
| Fresh/upgrade equivalence and legacy coexistence | YES | migration harness + 52 CI DB regressions |
| Existing migrations 001–009 unchanged | YES | immutable hash manifest verification |
| Forbidden platform scope absent | YES | changes restricted to CPI foundation and evidence |

## Capability and authorization status

The checked-in registry has nine capability decisions, all BLOCKED. These are
historical checked-in review/evidence vectors and have not been promoted into
approval for this new implementation revision. Their referenced evidence includes
an older tested source revision and null executable workload digests. Tests use
explicit fixture authorizations to prove behavior; fixtures do not grant runtime
production permission.

The current inventory has 56 official entries, 111 capability results, 0 pinned
materialized official artifacts, 56 REMOTE_ONLY entries and 4 synthetic conformance
fixtures. `official_corpus_ready=false`, `conformance_ready=true`, and
`evidence_requirements_satisfied=false`. Inventory-only exit 0 is not a release
approval.

## Readiness and limitations

- FOUNDATION_VERIFIED: locally verified implementation; remote CI pending at
  evidence creation. Complete official-source release evidence remains blocked.
- RELEASE_ELIGIBLE: NO, checked-in gates and official corpus remain blocked.
- PRODUCTION_DEPLOYED: not established by this work.
- CUSTOMER_AVAILABLE: not established by this work.
- MARKETING_APPROVED: not established by this work.
- LEGAL_APPROVED: not established by this work.
- Merge: not performed. Foundation merge and release are separate decisions.
- Trusted workforce identity and snapshot generation are internal application
  boundaries; this PR does not deliver production IAM or a customer Product API.
- Domain snapshot scans current CPI state under an exclusive fence; production
  scale/latency and operational rollout are not established by these local tests.
- Existing user-owned untracked blog/presentation directories are preserved and
  excluded from commits. Thus `git status --short` is not entirely empty.

## Phase review

The review found and corrected a concrete recovery race: event creation changed
snapshot state without advancing its watermark. Journal rows now also use each
table's own primary key rather than an incidental referenced disclosure key.
Rollback, immutable evidence, fencing and containment were checked. No latest-wins,
float canonical observations, Product API/UI, billing, production IAM, CDN or
notifications were added.
