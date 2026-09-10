# Current implementation versus approved target

Baseline revision: `63633a50c85c88e507be458067ecf6706220f813`.

| Capability | Implemented now | Foundation added in this pass | Approved target, not yet implemented |
| --- | --- | --- | --- |
| event lifecycle | historical catalog writes legacy `economic_events` with release-first semantics | canonical identity plus append-only scheduled/released lifecycle versions; no rows populated | official schedule/change adapter and migration from compatibility rows |
| event facts | legacy scalar event fields and FRED/ALFRED context | canonical observation-code/unit registry; append-only official revision, external consensus and PIT-safe surprise schema | official/external provider adapters and historical consensus backfill |
| trading time | release-relative legacy windows | pure marker-driven planner over one trusted verified calendar snapshot; corrected post-market/closed S0 | calendar provider refresh and persisted plan adapter |
| quality | collection plus evolving legacy coverage fields | distinct run/work-item/session/coverage/eligibility/reason/maturity vocabulary | persisted metric-level quality decisions for target analyses |
| reactions | `PRE_60M`, `POST_5M/30M/60M` legacy rows | complete `event_session_reaction_v2` definition registry; no calculations | session-aware reaction computation and backfill |
| macro | FRED/ALFRED contexts | PIT rules documented | feature/regime production tables |
| comparables | absent | deterministic filter/sample-policy contract; outline only | versioned comparable-set computation |
| strategy validation | one exploratory rule and stored results | hypothesis/simulation contract | time-split validation and cost-model registry |
| execution | manual guarded Alpaca Paper intent journal | experiment lineage schema deferred | fills, positions, reconciliation and outcomes |
| operations | historical/manual DAGs; partial durable telemetry | target DAG/freshness/failure contracts | full operational schedules and observability |
| serving | local FastAPI/browser pages | resource boundaries documented | authenticated deployed product topology |
| research bars | provider aggregate requests stored in `market_bars` | provider storage explicitly reserved for research/serving | stricter duplicate/conflict collection checks |
| raw validation | archive → Kafka → Spark bounded replay historically wrote a collidable bar identity | separate `validation_reconstructed_bars`, comparison view and run/version checkpoint seams | managed object storage, durable validation-run registry and on-demand workers |

## Compatibility transition

- **Phase A — current:** existing serving reads legacy `economic_events`, legacy windows and provider `market_bars`.
- **Phase B — target:** normalized adapters populate event lifecycle, official observations, external consensus and canonical surprise.
- **Phase C — target:** new serving resources read normalized facts and v2 reactions.
- **Phase D — target:** retained legacy endpoints use an explicit compatibility projection.
- **Phase E — target:** scalar `forecast/actual/surprise`, `quality_status`, and `value_source` writes are frozen/deprecated.

Permanent dual-write is not the canonical solution.

“Foundation” means additive schema, pure contracts or tests. It does not mean an operational ingestion path exists.
