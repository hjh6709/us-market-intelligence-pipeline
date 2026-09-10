# Trading sessions, quality and reactions implementation plan

**Goal:** make calendar/session semantics executable without changing legacy metric outputs.

**Architecture:** a pure planner consumes verified session records. Quality states and target metric names live in the shared contract module. Provider refresh and persistence are separate later adapters.

**Tech:** Python dataclasses/enums, `zoneinfo`, `unittest`.

**Status:** executable corrective plan for PR #36.

### Task 1: Contract tests first

Files: `tests/test_trading_sessions.py`, `tests/test_platform_contracts.py`.

- [ ] Add failing tests for premarket/regular same-day S0, post-market/closed next-session S0, early close, DST, trusted timezone, one source/snapshot, local dates, overlap, unique marker ID/kind and FOMC roles.
- [ ] Add failing tests for Good Friday, future S+7, provider safety lag, sparse premarket, complete v2 definitions and PRE drift exclusion.
- [ ] RED: run `python -m unittest tests.test_trading_sessions tests.test_platform_contracts -v`; the old S0/coarse taxonomy/incomplete vocabulary must fail.

### Task 2: Minimal implementation

Files: `src/trading_sessions.py`, `src/platform_contracts.py`.

Interface: `plan_event_session(*, event_id: str, markers: Sequence[EventMarker], sessions: Sequence[TradingSession], market_code: str, planner_version: str) -> EventSessionPlan`; `metric_maturity(...) -> QualityAssessment`.

- [ ] Minimally implement pure marker/session validation and S-1/S0/S+1 selection; do not add a provider or persistence.
- [ ] Add distinct status enums and the full immutable `event_session_reaction_v2` registry; do not compute or rename legacy rows.
- [ ] GREEN: rerun the exact targeted command, then the entire suite.
- [ ] Commit boundary: `fix(contracts): correct session quality and reaction semantics`.

### Task 3: Integration boundary

1. Document that exchange-calendar refresh, persisted quality decisions and target metric computation remain unimplemented.
2. Run `.venv/bin/python -m unittest tests.test_trading_sessions tests.test_platform_contracts -v`, the full Python suite and static link/diff checks.
3. Commit `docs(calendar): record planner boundary and verification`.
