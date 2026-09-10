## 목적과 범위

PR #36은 새 제품 기능을 확장하지 않고 canonical event/session foundation 계약을 고정합니다. 현재 구현, 이번 foundation, 목표 아키텍처를 아래처럼 분리합니다.

## IMPLEMENTED NOW

- 기존 연구 서빙은 provider-aggregated `market_bars`와 legacy impact/strategy 결과를 계속 읽습니다.
- raw SIP 재구성 검증은 `validation_reconstructed_bars`에만 기록되며 research bar 경로와 물리적으로 분리됩니다.
- 브라우저 연구 조회와 guarded manual Alpaca Paper 경계는 기존 상태를 유지합니다. 연구 결과는 주문을 자동 제출하지 않습니다.

## FOUNDATION ONLY

- migration `009`는 정확한 event universe/ontology, append-only lifecycle·official observation·consensus·marker·calendar·validation lineage와 PIT-safe surprise 제약을 제공합니다.
- immutable surprise history와 `current_canonical_surprises` projection은 marker correction이나 later consensus backfill 뒤 stale derivation을 current에서 제외합니다.
- `src/trading_sessions.py`는 snapshot-owned `US_EQUITIES`/`America/New_York`, current primary marker, pre/regular/post/closed S0와 verified S+N 규칙을 검증합니다.
- `src/platform_contracts.py`는 typed reaction-v2 price/activity definition, close guard와 quality/maturity taxonomy를 순수 계약으로 제공합니다.
- marker와 immutable record functions는 transaction-scoped identity lock으로 concurrent same-content idempotency와 different-content domain conflict를 보장합니다.
- normalized foundation은 이번 PR에서 legacy serving에 연결하거나 historical metric을 재해석하지 않습니다.

## TARGET ONLY

- production official-observation/consensus adapters와 backfill
- operational exchange-calendar ingestion과 canonical reaction recomputation/backfill
- macro regimes, comparables, registered hypotheses, strategy simulator
- automated research-to-order flow와 full Paper fill/position/outcome lifecycle
- authenticated managed/public deployment, TLS, backups, production operations

## Corrective verification

- [x] T01–T50 exact semantic tests: 50/50 GREEN
- [x] Full Python: 317 run, 262 passed, 55 skipped, 0 failed
- [x] Node UI: 6/6 passed
- [x] PostgreSQL 17.6: fresh migrations `001..009`, second `009` apply, 46/46 behavior tests passed
- [x] Python compileall
- [x] Archify: 9/9, 0 errors, 0 warnings; automated viewport and manual light/dark review passed
- Exact final-head GitHub CI is checked after the final push; the PR check result is the authoritative receipt for that commit.

Detailed evidence is in `docs/evidence/architecture-pass-2026-09-10/`. PR #36 must remain open and must not be merged as part of this corrective task.
