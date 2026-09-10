STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION

# Point-in-time macro and comparable-events design

## Scope

Build leakage-safe macro features and reproducible comparable-event sets after event observation and session contracts are operational.

## Target interfaces

Feature computation uses source-time/vintage eligibility at a cutoff; system ingestion time alone is insufficient. The current event's new value cannot enter its PRE_EVENT regime. A regime references explicit inflation, labor, policy-rate direction, 2s10s curve and volatility dimensions plus selection/feature/regime versions.

Comparable v1 uses deterministic filters from event type through observation code, surprise direction/bucket, selected regime dimensions, session relation and overlap policy. It persists requested/effective criteria, explicit relaxation, member rationale and sample statistics. It never uses top-k distance to force a sample.

## Failure rules and tests

Later revisions are excluded, unavailable features remain explicit null/ineligible values, and insufficient comparable candidates return a typed insufficient-universe result. Tests use revision traps, deterministic ties and repeated runs.

## Delivery status

Documentation and plan only. No current UI or FRED context row is relabeled as a feature, regime or comparable set.
