# PR #36 corrective foundation evidence — 2026-09-10

This directory records the bounded corrective review of PR #36. The review started from remote `main` at `63633a50c85c88e507be458067ecf6706220f813`; the final narrow merge-blocker pass started from PR head `9c3bcac5b0683c5b1901485711a6d7b4a32d7fb8` and verifies foundation source revision `3bd765f4b1ea8a2c03952cf1e521177d707166c2`.

The evidence proves the corrected event/session foundation semantics, not the target product features described in the architecture contracts. In particular:

- **Implementation truth:** legacy research serving still reads provider-aggregated `market_bars`; the guarded Paper path remains separate.
- **Foundation truth:** migration `009`, the pure contract module, and the verified-session planner now encode lifecycle, observation/revision, PIT-safe surprise, marker, maturity, and provenance invariants.
- **Target truth:** production adapters/backfills, canonical reaction recomputation, macro regimes, comparables, simulation, the full Paper lifecycle, and managed deployment are not implemented.

Verification includes 317 Python tests with zero failures, 6 Node tests, and 46 PostgreSQL 17.6 behavior tests after a fresh `001..009` apply and a second apply of `009`. The architecture diagram did not change in the narrow pass, so its prior Archify 9/9 and visual receipt remain dated diagram evidence rather than being regenerated. The exact T01–T50 results are recorded in `counterexample-matrix.md`; the C1–C15 contract review is recorded in `verification.json`.

This evidence does **not** authorize merge by itself. Exact-head GitHub CI must still pass after the evidence/diagram commit is pushed, and PR #36 must remain open and unmerged for this corrective task.

The historical market-bar audit found 308,512 provider-aggregated rows and no raw-reconstructed rows in the checked historical database. It therefore proves that the old schema/code allowed an origin collision, but does not prove that a historical overwrite occurred or whether a separate raw-validation database was used.

Historical evidence under sibling directories was not rewritten.
