# Current implementation versus historical target

Status: `STATUS_LEDGER` only. This page records progress and gaps at baseline
`63633a50c85c88e507be458067ecf6706220f813`; it is not semantic authority and
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
  promotion boundary, and fail-closed parser release gate infrastructure.

The following are **not implemented or not launch-ready** and must not be represented
to Product, Marketing, Sales, CS, or users as production capability:

- Table 1 XLSX binary parser/corpus validation and live corroboration ingestion path;
- live correction source discovery/locator/parser path;
- all replay-required official CPI corpus artifacts are still not materialized,
  reviewed, SHA-256 pinned, and semantically approved; the parser release gate
  therefore remains intentionally not ready;
- production IdP/RBAC integration for the implemented governance application boundary;
- golden-corpus differential replay and parser-change release gate;
- controlled collector/orchestrator and reconciliation;
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
