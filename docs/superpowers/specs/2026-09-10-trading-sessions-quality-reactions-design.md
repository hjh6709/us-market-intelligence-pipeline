# Trading sessions, quality and reactions design

## Scope

Provide a pure, marker-driven session planner and shared semantic registries without changing legacy analytics. Verified sessions must share the trusted `US_EQUITIES` / `America/New_York` mapping, one calendar source and one durable snapshot ID.

## Planner contract

`plan_event_sessions(*, event_id, markers, sessions, market_code, planner_version)` accepts timezone-aware instants, normalizes to UTC, validates local session dates and chooses S0 as the first regular session able to absorb the primary marker. Premarket and regular markers use same-day S0; post-market and closed-day markers use the next session. FOMC `STATEMENT` is primary and `PRESS_CONFERENCE` secondary. Marker IDs and kinds are unique.

## Quality and reaction contracts

Run outcome, work-item outcome, interval/day type, coverage, eligibility, reason and per-metric maturity remain independent. The canonical `event_session_reaction_v2` registry defines every announcement, session/open, persistence and activity metric with exact endpoints, prices, clipping, tolerance, maturity and applicability. `PRE_EVENT_DRIFT_60M` is context, not reaction. Legacy `PRE_60M` and `POST_*` rows are not renamed.

Calendar refresh, planner persistence and v2 reaction computation remain target-only.
