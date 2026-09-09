# Canonical data contracts

Status: target contract; implemented foundations are listed explicitly in [current-vs-target.md](../engineering/current-vs-target.md).

## Event identity and observations

`economic_events` identifies a release by stable event ID, event type, reference period, scheduled/released time and official source. It is not the value history.

`economic_release_observations` is append-only. Its business key is `(economic_event_id, metric_name, revision_number, observed_at)`. Required provenance is `value`, `unit`, `source`, `source_url`, `released_at`, `observed_at`, `ingested_at`, and `payload_sha256`. A correction inserts a new revision; it never mutates an earlier value.

`economic_consensus_snapshots` stores the estimate available at a specific `observed_at`, including provider, contributor count when available, unit and payload hash. One “latest” consensus may be selected only by an explicit point-in-time cutoff.

`economic_surprises` is derived, not ingested: `actual - consensus` plus optional standardized value. It references the exact actual observation and consensus snapshot, records algorithm/version and run ID, and is unique for that input/version tuple.

## Trading sessions and markers

`trading_sessions` stores exchange, session date, open/close UTC, calendar source/version and early-close flag. The planner consumes verified rows; it does not infer holidays from weekdays.

An event maps to `MARKET_CLOSED`, `PRE_MARKET`, `REGULAR_SESSION`, or `POST_MARKET`. `S0` is the release-date session when open; otherwise it is the next verified session. `S-1` and `S+1` are adjacent verified sessions. Markers include `RELEASE`; FOMC may add `STATEMENT` and `PRESS_CONFERENCE` with their own timestamps.

## Market data

Alpaca SIP 1-minute bars are the canonical research input. Daily bars provide context. Derived 3m/5m bars retain source-count and partial-window status; they do not fabricate missing minutes. The raw SIP archive is immutable validation input and stays independent from provider-bar research lineage.

## Quality axes

| Axis | Meaning | Canonical examples |
| --- | --- | --- |
| collection | Did the provider request and persistence contract finish? | `SUCCEEDED`, `FAILED` |
| market | Was a verified session expected? | `OPEN`, `EARLY_CLOSE`, `CLOSED` |
| coverage | How much expected market data was observed? | `COMPLETE`, `PARTIAL`, `EMPTY`, `NOT_APPLICABLE` |
| eligibility | May this input enter a named analysis? | `ELIGIBLE`, `INELIGIBLE`, `NOT_EVALUATED` |

Eligibility is versioned by analysis contract. No single `COMPLETE` flag may stand in for all four axes.

## Lineage and lifecycle

Curated tables carry provider/source, feed where relevant, source observation time, ingestion time, algorithm/version and run ID. Raw payloads use an immutable URI plus SHA-256 rather than being copied into presentation evidence. Curated records may be upserted only by deterministic business key; append-only facts are never updated. Retention or deletion policy must be recorded before any destructive lifecycle automation is enabled.
