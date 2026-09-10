# PR #36 corrective foundation evidence — 2026-09-10

This directory records the bounded corrective review of PR #36. The review started from remote `main` at `63633a50c85c88e507be458067ecf6706220f813` and the previously reviewed PR head `b8c9b127f74181278dbdb6098926936188136b66`.

The evidence proves the corrected event/session foundation semantics, not the target product features described in the architecture contracts. In particular:

- **Implementation truth:** legacy research serving still reads provider-aggregated `market_bars`; the guarded Paper path remains separate.
- **Foundation truth:** migration `009`, the pure contract module, and the verified-session planner now encode lifecycle, observation/revision, PIT-safe surprise, marker, maturity, and provenance invariants.
- **Target truth:** production adapters/backfills, canonical reaction recomputation, macro regimes, comparables, simulation, the full Paper lifecycle, and managed deployment are not implemented.

Verification includes 270 Python tests, 6 Node tests, 19 PostgreSQL 17.6 behavior tests after applying migrations 001–009 twice, and Archify validation plus independent semantic/source-grounding and light/dark visual review. The 18 semantic requirements are recorded individually in `verification.json`; all 18 are satisfied and no canonical-architecture merge blocker remains in this bounded foundation.

The historical market-bar audit found 308,512 provider-aggregated rows and no raw-reconstructed rows in the checked historical database. It therefore proves that the old schema/code allowed an origin collision, but does not prove that a historical overwrite occurred or whether a separate raw-validation database was used.

Historical evidence under sibling directories was not rewritten.
