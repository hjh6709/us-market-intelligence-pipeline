# Research and validation contracts

Status: target contract. Current legacy metrics and strategy results are preserved as baselines, not renamed into this target.

## Event-session model

Each research unit binds `economic_event_id`, symbol, verified `S-1/S0/S+1`, event markers, source/feed and a quality decision. All timestamps are stored in UTC; exchange-local time is presentation metadata.

## Reaction metrics

The canonical target metric set is:

- close-to-anchor returns for `5m`, `15m`, `30m`, `60m`, session close and next-session close;
- pre-event drift over `60m`;
- realized volatility and volume over the same declared intervals;
- benchmark-relative return when a benchmark is explicitly configured.

Every metric declares `[start, end)` interval semantics, anchor marker, price field, session clipping policy, expected/observed counts, eligibility and `metric_version`. A closed market produces `NOT_APPLICABLE`, not synthetic zero return.

## Point-in-time macro features and regimes

Features use only observations whose `observed_at` is at or before the event cutoff. Regime labels are derived from a named feature set and version; later revisions cannot leak backward. “FRED context” is not the event's official actual or market consensus unless explicitly sourced as such.

## Comparable events

A comparable-set record names its candidate universe, filters, distance features, normalization, top-k rule, version and run. It stores selected event IDs and distances. Similarity is descriptive evidence, not causal attribution.

## Hypotheses and simulations

A hypothesis declares inputs, signal rule, entry/exit markers, universe, quality requirements, cost model and version before evaluation. Historical simulation records positions, fills under stated assumptions, exclusions and aggregate metrics. Time-split validation is mandatory for predictive claims. The existing pre-60/post-60 rule remains a legacy exploratory baseline.

## Interpretation boundaries

Event association is not causation. Results are not future expected returns, portfolio returns or investment advice. Missing consensus, revisions, quote-level slippage and comparison days constrain claims and must appear beside results.
