# Trading sessions, quality and reactions implementation plan

**Goal:** make calendar/session semantics executable without changing legacy metric outputs.

**Architecture:** a pure planner consumes verified session records. Quality states and target metric names live in the shared contract module. Provider refresh and persistence are separate later adapters.

**Tech:** Python dataclasses/enums, `zoneinfo`, `unittest`.

### Task 1: Contract tests first

Files: `tests/test_trading_sessions.py`, `tests/test_platform_contracts.py`.

1. Add failing gold tests for the shared CPI/employment pre-market timing contract, FOMC statement/press conference, Good Friday closure, early close and exchange-local mapping of UTC inputs.
2. Add failing tests proving quality axes are distinct and target reaction names/version are stable.
3. Run `.venv/bin/python -m unittest tests.test_trading_sessions tests.test_platform_contracts -v` and retain RED output.
4. Commit the test intent with implementation in Task 2 after GREEN; do not leave a red-only branch commit.

### Task 2: Minimal implementation

Files: `src/trading_sessions.py`, `src/platform_contracts.py`.

1. Add aware-UTC validation, ordered non-overlapping session validation and S-1/S0/S+1 selection.
2. Emit `MARKET_CLOSED`, `PRE_MARKET`, `REGULAR_SESSION`, `POST_MARKET` and named markers.
3. Add enums/constants only; do not modify current analytics jobs.
4. Re-run targeted tests to GREEN, then run the entire suite.
5. Commit `feat(calendar): add verified-session planning contracts`.

### Task 3: Integration boundary

1. Document that exchange-calendar refresh, persisted quality decisions and target metric computation remain unimplemented.
2. Run `.venv/bin/python -m unittest tests.test_trading_sessions tests.test_platform_contracts -v`, the full Python suite and static link/diff checks.
3. Commit `docs(calendar): record planner boundary and verification`.
