# Current implementation versus historical target

## R1 local V2 extension (2026-10-10)

Local Tasks 1–9 add capability-scoped evidence policies/readiness, canonical verified
review bytes, explicit V2 snapshots and additive migration 014. Current effective
policy registration, exact tested workload/job/executor revision and review binding
are enforced in Python and PostgreSQL material/grant/admission/final-write paths.
V1 serialization/history remains historical; V1 cannot authorize new or resumed
promotion. Legacy market serving is unchanged. See the [R1 local verification](../evidence/cpi-w1-release-evidence-v2-2026-10-07.md).

Independent R1 review is pending; this is not production authorization. All nine
capability gates remain BLOCKED, all nine readiness rows NOT_READY, and
`RELEASE_ELIGIBLE = NO`. No Product/UI/deployment/IAM work or official bulk collection
is part of this extension. Older baseline statements below retain their historical
time boundary and do not supersede this explicitly dated R1 record.

Status: `STATUS_LEDGER` only. This page records progress and gaps at inspected PR #39
baseline `b24eb6436494d0081a35b4098d222f0c20fd7ed3`; it is not semantic authority and
cannot override current executable behavior or the CPI W1 canonical target. See
[the authority index](../architecture/AUTHORITY.md).

| Capability | Implemented at baseline | Foundation added in that pass | Historical target, not yet implemented |
| --- | --- | --- | --- |
| event lifecycle | historical catalog writes legacy `economic_events` with release-first semantics | exact four-event universe; append-only lifecycle chain, transition checks, idempotency/conflict boundary, current view; no rows populated | official schedule/change adapter and compatibility migration |
| event facts | legacy scalar event fields and FRED/ALFRED context | immutable canonical identity; exact event/code/unit ontology; append-only official revisions; provider-local consensus selector; historical surprises plus dynamic current-canonical projection; serialized idempotent writes; no rows populated | official/external provider adapters and historical consensus backfill |
| trading time | release-relative legacy windows | immutable calendar parent/session FK with local-date enforcement; append-only marker revisions; concurrent current-primary enforcement; snapshot-owned pure S0 and S+N planning | calendar provider refresh and persisted plan adapter |
| quality | collection plus evolving legacy coverage fields | executable exact run/work-item/session/interval/coverage/eligibility/reason/maturity constraints | persisted metric-level quality decisions for target analyses |
| reactions | `PRE_60M`, `POST_5M/30M/60M` legacy rows | typed `event_session_reaction_v2` price/activity registry, exact formulas, marker-level identity, close-boundary guard and pure required-window resolver; no v2 rows | session-aware reaction computation and backfill |
| macro | FRED/ALFRED contexts | PIT rules documented | feature/regime production tables |
| comparables | absent | deterministic filter/sample-policy contract; outline only | versioned comparable-set computation |
| strategy validation | one exploratory rule and stored results | hypothesis/simulation contract | time-split validation and cost-model registry |
| execution | manual guarded Alpaca Paper intent journal | experiment lineage schema deferred | fills, positions, reconciliation and outcomes |
| operations | historical/manual DAGs; partial durable telemetry | target DAG/freshness/failure contracts | full operational schedules and observability |
| serving | local FastAPI/browser pages | resource boundaries documented | authenticated deployed product topology |
| research bars | provider aggregate requests stored in `market_bars` | provider storage explicitly reserved for research/serving | stricter duplicate/conflict collection checks |
| raw validation | archive → Kafka → Spark bounded replay | immutable `validation_runs` parent; append-only `validation_reconstructed_bars`; idempotent same-content replay; determinism conflict; comparison-only view | managed object storage and on-demand workers |

## Compatibility transition

- **Phase A — current:** existing serving reads legacy `economic_events`, legacy windows and provider `market_bars`.
- **Phase B — target:** normalized adapters populate event lifecycle, official observations, external consensus and canonical surprise.
- **Phase C — target:** new serving resources read normalized facts and v2 reactions.
- **Phase D — target:** retained legacy endpoints use an explicit compatibility projection.
- **Phase E — target:** scalar `forecast/actual/surprise`, `quality_status`, and `value_source` writes are frozen/deprecated.

Permanent dual-write is not the canonical solution.

“Foundation” means additive schema, pure contracts or tests. It does not mean an operational ingestion path exists.

## CPI W1 working-branch status

This section is a coordination snapshot for the CPI W1 working branch. It remains
`STATUS_LEDGER`, not semantic authority. Always verify the branch HEAD and CI before
using it for release decisions.

Current implemented foundation on the CPI W1 branch includes:

- additive migrations 010-013 for ingestion/artifact lineage, event/disclosure
  evidence, Core 4 observations, interpretation governance, and serving-control
  history;
- DB enforcement that canonical CPI evidence is attributable to
  `ECONOMIC_PROMOTE` attempts rather than collector attempts;
- forensic source-artifact metadata immutability, with only the retained-object
  lifecycle transition `RETAINED -> DELETED_BY_POLICY` allowed;
- target-only Python semantic contracts and deterministic material
  fingerprints;
- bounded BLS source retrieval and development/test filesystem artifact storage;
- CPI schedule candidate parsing with authoritative HTML versus fallback ICS
  source-role separation;
- guarded schedule-assertion promotion with artifact-hash binding, dedicated
  extractor contracts, source-role verification, explicit-cancellation constraints,
  and event fencing;
- BLS release-envelope and Core 4 HTML extraction;
- fenced CPI W1 repository/promotion primitives for release-envelope, secondary
  corroborating-representation topology, and atomic Core 4 promotion;
- CPI source reconstruction and SYSTEM_KNOWN_PIT selector with authority-tier
  schedule selection, transitive provenance invalidation, semantic conflict
  preservation, knowledge fingerprinting, and live serving-control overlay;
- application governance boundary for interpretation requests/approvals/activation
  and emergency serving control, with trusted workforce-principal objects and
  event-fenced re-enable verification;
- explicit CPI correction notice and correction-observation promotion boundaries
  with CORRECTION_NOTICE provenance and promoter-driven correction rehearsal;
- CPI corpus inventory/replay tooling, staging capture boundary, reviewed-byte
  promotion boundary, and fail-closed parser release gate infrastructure;
- runtime extractor release-gate snapshot separating official byte collection from
  canonical promotion eligibility, with durable deferred-promotion audit evidence;
- controlled CPI W1 collector/orchestrator skeleton for bounded capture, immutable
  artifact persistence, gate-aware promotion scheduling, scoped replay
  reconciliation, and run finalization.
- capability-aware orchestration with immutable artifact-to-reference-month target
  bindings, independent per-capability gate outcomes, exact deferred-decision audit
  identity, duplicate-delivery convergence, and artifact-scoped PostgreSQL advisory
  locking for concurrent scheduling.

The following are **not implemented or not launch-ready** and must not be represented
to Product, Marketing, Sales, CS, or users as production capability:

- Table 1 XLSX binary parser/corpus validation and live corroboration ingestion path;
- live correction source discovery/locator/parser path;
- all replay-required official CPI corpus artifacts are still not materialized,
  reviewed, SHA-256 pinned, and semantically approved; the parser release gate
  therefore remains intentionally not ready;
- production IdP/RBAC integration for the implemented governance application boundary;
- golden-corpus differential replay and parser-change release gate;
- collector/orchestrator is implemented at the W1 development boundary, but live
  canonical promotion remains intentionally blocked by the current extractor gate
  until reviewed official corpus evidence is ready;
- production object-storage retention, orphan cleanup, integrity repair, and
  restore procedures;
- Product API/UI/SEO/cache/notification withholding integration;
- production observability/SLO/on-call/runbook/DR evidence;
- final legal/readiness sign-off and Public Beta/Paid Launch gates.

The current filesystem artifact store is a deterministic development/test adapter,
not a production immutable/versioned object-storage claim. Its local generation token
must not be treated as a cloud object generation or as production recoverability
evidence.

A green CI run proves only the tested repository contract at that commit. It does not
by itself prove deployment, source availability, legal approval, operational
readiness, or customer-facing launch readiness.

## Accepted CPI W1 foundation delta status

Overall status at the inspected baseline: `ACCEPTANCE_INCOMPLETE`.

The PR contains a substantial working CPI W1 foundation, but the accepted post-PR
red-team amendment is not yet implemented. The following inventory separates code
that should remain from behavior that must change and foundation evidence that must
be added. It is a planning/status statement only; the canonical semantics live in
the CPI W1 design specification.

### KEEP

- immutable source artifacts, assertion history, and forensic provenance;
- reference-month CPI event identity and explicit disclosure topology;
- Decimal/NUMERIC canonical observations and deterministic material identity;
- current-source and SYSTEM_KNOWN_PIT reconstruction separation;
- append-only interpretation governance and serving-control overlay;
- lease/claim-generation fencing and legacy serving coexistence;
- the additive 010-013 foundation as historical migration evidence.

### AMEND

- Phases 1-14 implement the accepted subject, capability, authorization, PAUSED,
  fencing, orchestration, and domain recovery corrections on this working branch.
- Domain recovery uses an immutable snapshot FK, a committed mutation watermark,
  and zero unsafe-uncontained events; individually WITHHELD events may remain.
- Phase 15 local CI-equivalent verification is complete. Phase 16 evidence is
  recorded in [the 2026-10-05 verification report](../evidence/cpi-w1-foundation-2026-10-05.md).
  Remote CI at the newly pushed revision remains a completion gate.

### ADD

- implemented domain recovery mutation journal and immutable recovery snapshot;
- 001-009 migration hash manifest plus fresh-versus-upgrade equivalence harness;
- final verification and evidence package tied to the exact tested HEAD.

Until every accepted delta phase is implemented and verified, PR #39 must not be
described as `FOUNDATION_VERIFIED` for the accepted CPI W1 delta, merge-ready, production
ready, or customer-launch ready.

## 2026-10-06 bounded corrective verification

Implementation revision `15eee8b53d45b82d0b222b2e788f3a57223ecce2` corrects the three
independent-audit blockers: structured target month in durable promotion work identity,
post-lock wall-clock lease validation, and exact attempt-bound heartbeat authorization.
Existing scheduling also rejects contradictory work-key metadata and unreviewed
targetless draft rows. Retry/pause also reject lease expiry after attempt-row waits.
These are foundation corrections, not product/runtime additions.
See [new corrective evidence](../evidence/cpi-w1-corrective-2026-10-06.md) for RED/GREEN,
migration/legacy checks, independent review and the exact-head remote CI acceptance gate.
The previous dated evidence is historical, not a substitute for this verification.
RELEASE_ELIGIBLE remains NO; no production/customer/marketing/legal readiness is claimed.
