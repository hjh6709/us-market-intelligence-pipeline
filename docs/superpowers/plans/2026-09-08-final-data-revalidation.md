# Final Data Revalidation Plan

> **Goal:** Restore and revalidate the complete 202-event × 10-symbol research dataset under the corrected collection/observed-coverage contract, then refresh analysis evidence without changing strategy logic.

## Scope

- Keep the current event catalog, symbol universe, Alpaca SIP collection, 3m/5m derivation, macro analysis 90% rule, and strategy logic unchanged.
- Correct the collection manifest so daily, session, derived, overall observed coverage, and provider collection status are recorded separately.
- Re-run the existing batched market-context collection through the current code, using Upsert only.
- Recompute the existing `multi_event_sip_v1` impacts and `v1` exploratory strategy from the restored bars.
- Export price-free evidence and update README/assignment wording to the measured result.
- Do not add `research-validation-v2`, execution/risk features, or presentation slides/video.

## Tasks

1. Add regression tests that fail while the manifest serializer confuses daily and overall coverage.
2. Add a small serializer/summary helper in `scripts/collect_market_event_context.py`; preserve legacy keys only when their meaning remains accurate.
3. Update `scripts/evidence/export_multi_event_summary.py` to report collection status and each observed-quality layer independently.
4. Run targeted tests, then the full 202×10 collection against the preserved local PostgreSQL database.
5. Re-run `src.macro_event_impact`, `src.event_strategy_backtest`, and the evidence exporters.
6. Verify exact event/symbol/window counts, duplicate business keys, collection outcomes, observed-quality distributions, and serving reads.
7. Update repository documentation and evidence, run the full unit and isolated PostgreSQL integration suites, then merge the branch back into the actual local `main` after CI passes.

## Safety

- No table truncation or destructive migration against the main local database.
- External collection is bounded to the catalog dates 2022-01-01 through 2026-08-26 and uses the existing two batched requests per event plus pagination.
- Existing historical result versions remain; current versions are updated through their established business keys.
