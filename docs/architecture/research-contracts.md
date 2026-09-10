# Research and validation contracts

Status: target contract. The complete vocabulary and planner are foundation code only; current stored impact rows remain legacy metrics and are not reinterpreted.

## Event-session model

Each target research unit binds an event, symbol, exact primary/secondary marker IDs, verified calendar snapshot, `S-1/S0/S+N`, provider-aggregate source/feed and metric-specific quality decision. All persisted instants are UTC; exchange-local time determines business semantics.

`S0` is the first regular trading session that can absorb the primary marker. Premarket and regular-session releases use the same-day session. Post-market and market-closed releases use the next session. `release_calendar_date` and nullable `release_day_session` are preserved separately.

## Reaction metric contract v2

Target announcement metrics: `POST_1M`, `POST_5M`, `POST_15M`, `POST_30M`, `POST_60M`.

Target session/open metrics: `RELEASE_TO_OPEN`, `OPEN_GAP`, `OPEN_30M`, `OPEN_60M`, `EVENT_TO_CLOSE`, `SESSION_RETURN_S0`.

Target persistence metrics: `S0_CLOSE_TO_S+1_CLOSE`, `S0_CLOSE_TO_S+3_CLOSE`, `S0_CLOSE_TO_S+7_CLOSE`.

Target activity metrics: `EVENT_VOLUME_RATIO`, `OPEN_60M_VOLUME_RATIO`, `EVENT_REALIZED_VOL`, `EVENT_VOLATILITY_RATIO`.

Pre-event drift is context, not event reaction. Legacy `PRE_60M`, `POST_5M`, `POST_30M`, and `POST_60M` rows retain their original `multi_event_sip_v1` definitions and evidence.

Every v2 definition in `REACTION_METRIC_DEFINITIONS` declares category, anchor marker, start/end endpoints, start/end price, session clipping, endpoint tolerance, maturity, applicability and `event_session_reaction_v2`. Examples:

```text
POST_5M
start = last completed bar strictly before primary marker, CLOSE
end   = bar ending at marker + 5 minutes, CLOSE

OPEN_GAP
start = last valid pre-open/premarket reference, CLOSE
end   = first regular-session bar, OPEN

S0_CLOSE_TO_S+1_CLOSE
start = S0 regular close, CLOSE
end   = S+1 regular close, CLOSE
```

Metric readiness is evaluated separately. On a closed day, a same-session immediate metric may be `NOT_APPLICABLE`, while an S0 persistence metric can mature after the next trading session. Future S+7 is `NOT_YET_MATURE` with `eligible_at`; provider safety lag is `DATA_NOT_AVAILABLE`, not missing coverage.

## Point-in-time macro features and regimes

The target layers remain `PIT observations -> PIT features -> macro regimes`. Historical eligibility uses source-time/vintage semantics such as official `published_at` or FRED/ALFRED `realtime_start`, never backfill `first_observed_at` alone. The current event's newly released value cannot enter its own `PRE_EVENT` regime. Daily FRED context is not intraday market state.

Persist `pit_selection_version`, `macro_feature_version`, and `macro_regime_version`. Initial explainable dimensions are inflation, labor, policy-rate direction, 2s10s curve and volatility percentile/state. Unsupported risk-on/risk-off or restrictive/accommodative labels are prohibited.

## Comparable events

Canonical v1 is deterministic filtering, not forced top-k nearest neighbors:

```text
L0 same event type
L1 + same observation code
L2 + same surprise direction
L3 + surprise magnitude bucket
L4 + selected macro-regime dimensions
L5 + same release/session relation
L6 + confound/overlap policy
```

Persist requested and effective criteria, explicit relaxation steps, candidate/eligible counts, selected members, match rationale, sample count, distribution statistics and version. Never silently relax or use top-k to manufacture sample size. Default interpretation is N>=10 descriptive, 5<=N<10 `LIMITED_SAMPLE`, and N<5 `INSUFFICIENT_SAMPLE`. Similarity is descriptive, not causal.

## Hypotheses and historical simulation

A strategy hypothesis freezes inputs, signal rule, entry/exit markers, universe, quality rules, cost model and version before evaluation. If a signal needs bar N's close, earliest execution is the next eligible bar's open. Simulation evidence records signal time, target/actual entry and exit, prices, gross return, costs, net return, MAE, MFE, simulation version and cost-model version.

Time-split validation is mandatory before predictive or Paper-eligible promotion. Exploratory descriptive simulation may exist without predictive claims. The existing negative legacy strategy result remains historical evidence and is not upgraded to target simulation.

## Corporate actions and interpretation

Provider historical requests currently use raw adjustment. Target S+1/S+3/S+7 individual-stock persistence keeps raw source prices but flags or excludes samples materially affected by splits, dividends or other corporate actions. Raw close-to-close returns are not universally clean across such events.

Event association is not causation. Results are not expected returns, portfolio returns or investment advice.
