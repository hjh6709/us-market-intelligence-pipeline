STATUS: PLANNING OUTLINE — EXPAND BEFORE EXECUTION

# Strategy validation design

## Scope

Separate hypothesis registration, historical simulation and forward Paper experiments. Preserve the current pre-60/post-60 result as a legacy exploratory baseline.

## Target interfaces

A frozen hypothesis includes universe, input versions, quality gate, signal, entry/exit markers and cost model. A simulation references the hypothesis and records exclusions, assumed fills, positions and aggregate metrics. A Paper experiment references one frozen hypothesis and an explicit risk envelope.

## Failure rules and tests

Unregistered versions, missing lineage, train/test overlap, future-data access and undeclared cost assumptions fail. Tests cover deterministic simulation, time split, cost application, exclusions and proof that research code cannot submit orders.

## Delivery status

Documentation and plan only. No automated strategy-to-order path is introduced.
