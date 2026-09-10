# Presentation-ready engineering facts — 2026-09-10

Use this page for the next PPT. It deliberately separates existing evidence, this pass, and target architecture.

## Verified current baseline

- Repository baseline: remote `main` at `63633a50c85c88e507be458067ecf6706220f813`.
- Product identity: **Economic Event Intelligence & Strategy Validation Platform**.
- Existing research evidence covers 202 releases, 10 symbols and 2,020 event-symbol combinations.
- Existing selected market data evidence records 308,512 1m rows, 112,593 derived 3m rows, 70,090 derived 5m rows, 30,270 event-selected daily rows and 11,700 unique stored daily rows.
- Existing research evidence records 2,020 PIT macro contexts, 8,080 legacy impact rows and 2,020 legacy strategy rows; 1,988 are calculable.
- The legacy strategy is not an economic-surprise strategy. After the documented 10 bp cost, its mean is negative; do not present it as profitable or predictive.
- Archived raw-SIP validation covers 7,360,804 trades and is separate from the aggregate-bar 202×10 research path.
- The browser serving layer and guarded manual Alpaca Paper path exist. Research results do not automatically submit orders.

## Verified in this architecture pass

- Canonical architecture, data, research, operations/deployment and configuration contracts now separate current implementation from target design.
- A validated Archify target diagram is available; it is a target map, not proof that every node runs today.
- Additive PostgreSQL foundations exist for release observations, consensus snapshots, derived surprises, trading sessions and event markers. Point-in-time facts reject update/delete, and surprise inputs must match the same event and metric.
- A pure planner maps releases to verified `S-1/S0/S+1` sessions and preserves pre-market, regular-session, post-market, holiday and early-close semantics.
- Shared types keep collection, market state, observed coverage and analysis eligibility separate.
- Final post-change verification ran 299 Python tests with zero failures (44 opt-in integration skips) and 6 Node UI tests with zero failures. On a fresh disposable PostgreSQL 17.6 database, migrations `001..009` applied successfully and `009` reapplied successfully; 35 PostgreSQL foundation and validation-sink behavior tests then passed.

## Target architecture only

- Official observation and consensus adapters, revision backfill and surprise population.
- Operational exchange-calendar ingestion and session-plan persistence.
- New session-aware reaction calculation through session closes.
- Macro regimes, comparable events, registered hypotheses and time-split simulations.
- Research-experiment-to-Paper lineage, fill/position reconciliation and risk limits.
- Authenticated public deployment, TLS, backups and managed services.

## Safe slide language

Say: “The existing pipeline is operational for the evidenced research and manual Paper boundaries. This pass adds tested schema and planning foundations for the approved target architecture.”

Do not say: “The target architecture is fully implemented,” “the platform is production-ready,” “the strategy is profitable,” “the 202×10 study came through Kafka/Spark,” or “research signals automatically place Paper orders.”
