# Trading sessions, quality and reactions design

## Scope

Introduce a deterministic planner that consumes verified exchange sessions and emits event classification, S-1/S0/S+1 and named markers. Define four independent quality axes and a versioned target reaction vocabulary.

## Interface

Input is an aware UTC release timestamp plus ordered `TradingSession` values containing exchange, session date, open/close UTC, early-close flag and calendar lineage. Output is `EventSessionPlan` with `market_status`, `release_phase`, adjacent sessions and markers.

Normal releases classify against open/close. Market-closed dates map S0 to the next verified session. FOMC can carry statement and press-conference markers. The planner never guesses holidays or calls a provider.

## Failure rules

Naive timestamps, overlapping/unsorted sessions, no future S0, no previous S-1, insufficient S+1, marker timestamps without timezone and unsupported marker kinds fail explicitly. Bounded/incomplete horizons are not accepted by this foundation interface.

## Verification

Gold tests cover the shared CPI/employment pre-market timing contract, FOMC regular-session markers, Good Friday, early close and exchange-local date mapping across UTC boundaries. Reaction computation remains target-only in this pass.

## Delivery status

Selected P1: pure planner, quality/reaction constants and tests. Calendar-provider integration, persistence and new reaction rows are not selected.
