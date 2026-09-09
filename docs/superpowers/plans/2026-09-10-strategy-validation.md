# Strategy validation implementation plan

**Goal:** turn exploratory rules into registered hypotheses while separating historical simulations from forward Paper experiments. This plan is not selected for the 2026-09-10 implementation slice.

### Task 1: Freeze hypothesis and simulation identities

Files: `db/migrations/011_strategy_validation.sql`, `tests/test_strategy_validation_migration.py`, `docs/architecture/research-contracts.md`, `docs/configuration/README.md`.

Interface: immutable `strategy_hypotheses`, `simulation_runs`, `simulation_observations`, and `cost_models`; a run references one hypothesis/version, feature set, quality gate, split policy and cost model.

1. Add tests requiring immutable versions, declared entry/exit markers, costs, exclusions and lineage FKs.
2. Run `.venv/bin/python -m unittest tests.test_strategy_validation_migration -v` and retain RED output.
3. Add migration `011` only; do not transform `event_strategy_results` or claim equivalence.
4. Run targeted tests, apply migrations `001..011` twice to disposable PostgreSQL and run the full suite.
5. Commit `feat(strategy): add registered hypothesis schema`.

### Task 2: Execute deterministic historical simulation

Files: `src/strategy_hypotheses.py`, `src/historical_simulation.py`, `tests/test_historical_simulation.py`.

Interface: `simulate(*, hypothesis, observations, split, cost_model) -> SimulationResult`; entry occurs at the next eligible bar open after the signal, never at an already observed close.

1. Add failing tests for train/test overlap, future-data access, next-bar execution, unavailable prices, partial inputs and transaction costs.
2. Run `.venv/bin/python -m unittest tests.test_historical_simulation -v` and retain RED output.
3. Implement pure deterministic replay and explicit exclusions; no broker import is allowed.
4. Run targeted tests twice and compare result hashes, then run the full suite.
5. Commit `feat(strategy): add time-split historical simulation`.

### Task 3: Define the forward Paper experiment boundary

Files: `db/migrations/012_paper_experiments.sql`, `src/paper_experiments.py`, `tests/test_paper_experiments.py`, `tests/test_research_execution_boundary.py`, `docs/execution/paper-sandbox.md`.

Interface: `approve_experiment(experiment_id, risk_envelope, approver)`, then explicit order intents reference the approved experiment. Research modules cannot import `src.alpaca_paper_execution`.

1. Add failing architecture-boundary, approval, partial-fill, cancel and uncertain-POST reconciliation tests.
2. Run `.venv/bin/python -m unittest tests.test_paper_experiments tests.test_research_execution_boundary -v` and retain RED output.
3. Add experiment/fill/position/outcome records and GET reconciliation by client order ID; keep live endpoints impossible by configuration.
4. Run broker-fake integration, restart recovery, targeted tests and full suite.
5. Commit `feat(paper): add approved forward experiment lineage`.
