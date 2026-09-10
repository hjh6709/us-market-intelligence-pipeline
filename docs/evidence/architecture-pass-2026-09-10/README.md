# PR #36 corrective foundation evidence — 2026-09-10

This directory records the bounded corrective review of PR #36. The review started from remote `main` at `63633a50c85c88e507be458067ecf6706220f813`, used PR head `96fed9cb1102f90bc0529c82ff617cd5d2684e46` as the strict corrective starting point, and verifies foundation source revision `b7e738eb575e2f561a0f2e7d87ad717920c290dd`.

The evidence proves the corrected event/session foundation semantics, not the target product features described in the architecture contracts. In particular:

- **Implementation truth:** legacy research serving still reads provider-aggregated `market_bars`; the guarded Paper path remains separate.
- **Foundation truth:** migration `009`, the pure contract module, and the verified-session planner now encode lifecycle, observation/revision, PIT-safe surprise, marker, maturity, and provenance invariants.
- **Target truth:** production adapters/backfills, canonical reaction recomputation, macro regimes, comparables, simulation, the full Paper lifecycle, and managed deployment are not implemented.

Verification includes 299 Python tests with zero failures, 6 Node tests, and 35 PostgreSQL 17.6 behavior tests after a fresh `001..009` apply and a second apply of `009`. Archify passed 9/9 validation plus automated viewport checks and independent light/dark visual review. The exact T01–T34 results are recorded in `counterexample-matrix.md`; the C1–C15 contract review is recorded in `verification.json`.

This evidence does **not** authorize merge by itself. Exact-head GitHub CI must still pass after the evidence/diagram commit is pushed, and PR #36 must remain open and unmerged for this corrective task.

The historical market-bar audit found 308,512 provider-aggregated rows and no raw-reconstructed rows in the checked historical database. It therefore proves that the old schema/code allowed an origin collision, but does not prove that a historical overwrite occurred or whether a separate raw-validation database was used.

Historical evidence under sibling directories was not rewritten.
