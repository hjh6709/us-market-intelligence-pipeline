# Current implementation versus approved target

Baseline revision: `63633a50c85c88e507be458067ecf6706220f813`.

| Capability | Implemented now | Foundation added in this pass | Approved target, not yet implemented |
| --- | --- | --- | --- |
| event facts | catalog + compatibility event row | append-only observation/consensus/surprise schema with exact event/metric lineage | official-source ingestion and revision backfill |
| trading time | release-relative legacy windows | pure planner over verified session rows | calendar provider refresh in operational DAG |
| quality | collection plus evolving coverage fields | canonical state vocabulary and session-aware planner outputs | persisted four-axis decisions for all analyses |
| reactions | PRE_60M, POST_5M/30M/60M legacy rows | target metric names/version contract | session-aware computation and backfill |
| macro | FRED/ALFRED contexts | PIT rules documented | feature/regime production tables |
| comparables | absent | contract and plan | versioned comparable-set computation |
| strategy validation | one exploratory rule and stored results | hypothesis/simulation contract | time-split validation and cost-model registry |
| execution | manual guarded Alpaca Paper intent journal | experiment lineage schema deferred | fills, positions, reconciliation and outcomes |
| operations | historical/manual DAGs; partial durable telemetry | target DAG/freshness/failure contracts | full operational schedules and observability |
| serving | local FastAPI/browser pages | resource boundaries documented | authenticated deployed product topology |
| raw validation | archive → Kafka → Spark bounded replay | boundary made explicit | managed object storage and on-demand workers |

“Foundation” means additive schema, pure contracts or tests. It does not mean an operational ingestion path exists.
