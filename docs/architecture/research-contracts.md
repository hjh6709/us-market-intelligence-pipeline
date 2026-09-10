# Research and validation contracts

Status: **foundation implemented, reaction calculation not implemented**. `src/platform_contracts.py` and `src/trading_sessions.py` enforce the vocabulary and pure planning semantics below. Current stored impact rows remain legacy `multi_event_sip_v1` results and are never renamed into v2.

## Event-session model

The canonical market is `US_EQUITIES` in `America/New_York`. `S0` is the first session able to absorb the current primary marker:

| Release phase | Reaction S0 |
| --- | --- |
| `PRE_MARKET` | same-day session |
| `REGULAR_SESSION` | same-day session |
| `POST_MARKET` | next trading session |
| `MARKET_CLOSED` | next trading session |

Early close is a session-day property. Market interval is only `REGULAR`, `EXTENDED`, or `CLOSED`. A marker correction is a new revision; the planner uses the latest revision and preserves marker identity in downstream reaction identity.

## Reaction metric contract v2

Every v2 reaction identity includes `(economic_event_marker_id, symbol, reaction_metric, metric_contract_version)`. Therefore an FOMC statement and press conference can each have `POST_5M` without inventing secondary-marker metric names.

All definitions use `event_session_reaction_v2`, endpoint tolerance 60 seconds, and `NO_CROSS_SESSION_FILL`. `CLOSE`/`OPEN` below are the required price fields.

| Metric | Exact start -> end | Applicability / maturity |
| --- | --- | --- |
| `POST_1M` | marker−1m bar `CLOSE` -> marker-minute bar `CLOSE` | both endpoints required |
| `POST_5M` | marker−1m bar `CLOSE` -> marker+4m bar `CLOSE` | both endpoints required |
| `POST_15M` | marker−1m bar `CLOSE` -> marker+14m bar `CLOSE` | both endpoints required |
| `POST_30M` | marker−1m bar `CLOSE` -> marker+29m bar `CLOSE` | both endpoints required |
| `POST_60M` | marker−1m bar `CLOSE` -> marker+59m bar `CLOSE` | both endpoints required |
| `RELEASE_TO_OPEN` | marker−1m bar `CLOSE` -> last valid pre-open reference `CLOSE` | pre-market with valid reference |
| `OPEN_GAP` | last valid pre-open reference `CLOSE` -> first S0 regular bar `OPEN` | valid pre-open reference required |
| `OPEN_30M` | first S0 regular bar `OPEN` -> S0 open+29m bar `CLOSE` | both endpoints required |
| `OPEN_60M` | first S0 regular bar `OPEN` -> S0 open+59m bar `CLOSE` | both endpoints required |
| `EVENT_TO_CLOSE` | marker−1m bar `CLOSE` -> S0 regular close `CLOSE` | pre-market/regular only; post-market/closed is `NOT_APPLICABLE` |
| `SESSION_RETURN_S0` | S0 regular open `OPEN` -> S0 regular close `CLOSE` | both endpoints required |
| `S0_CLOSE_TO_S+1_CLOSE` | S0 regular close `CLOSE` -> S+1 regular close `CLOSE` | matures at S+1 close |
| `S0_CLOSE_TO_S+3_CLOSE` | S0 regular close `CLOSE` -> S+3 regular close `CLOSE` | matures at S+3 close |
| `S0_CLOSE_TO_S+7_CLOSE` | S0 regular close `CLOSE` -> S+7 regular close `CLOSE` | matures at S+7 close |
| `EVENT_VOLUME_RATIO` | event-window `VOLUME` / reference-window `VOLUME` | reference window required |
| `OPEN_60M_VOLUME_RATIO` | S0 opening-60m `VOLUME` / reference opening-60m `VOLUME` | reference window required |
| `EVENT_REALIZED_VOL` | event-window returns -> realized volatility | minimum return count required |
| `EVENT_VOLATILITY_RATIO` | event realized volatility / reference realized volatility | reference window required |

For an 08:30 ET release, the POST endpoints are exactly 08:29→08:30, 08:29→08:34, 08:29→08:44, 08:29→08:59, and 08:29→09:29. For a 09:30 open, `OPEN_30M` ends at 09:59 and `OPEN_60M` at 10:29. Missing endpoints are not filled across sessions.

Pre-event drift is context, not reaction. Legacy `PRE_60M`, `POST_5M`, `POST_30M`, and `POST_60M` retain their recorded v1 definitions.

## Quality state contract

This normal state is valid and requires no reason:

```text
SUCCEEDED + REGULAR + COMPLETE + ELIGIBLE + reason=None
```

These states are invalid:

```text
FAILED + ELIGIBLE
DATA_NOT_AVAILABLE + COMPLETE
DATA_NOT_AVAILABLE + ELIGIBLE
SKIPPED + ELIGIBLE
NOT_YET_MATURE without eligible_at
ELIGIBLE while eligible_at is later than assessed_at
```

Reason code and detail are either both absent or both present. Closed-day and sparse-extended-hours outcomes remain explicit; provider safety lag is availability, not coverage. Metric maturity is per metric, so `POST_5M` may be eligible while S+7 remains `NOT_YET_MATURE`.

## Target-only research layers

PIT macro features/regimes, comparable-event computation, hypothesis registry, and historical simulation remain target-only. Future implementations must use source-time/vintage semantics, deterministic comparable relaxation, next-eligible-bar execution after a close-based signal, explicit cost models, and time-split validation. They are not implemented by migration 009.

Corporate-action handling is also target-only. Event association is not causation, and research results are not expected returns, portfolio returns, or investment advice.
