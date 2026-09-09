# Corrective engineering pass — 2026-09-09

This report supersedes neither historical measurements nor the old audit.
Starting remote main, independently fetched: `316a8a35341df9c961ec66f727ada89103eae3d1`.
PRs 32–34 were merged; the three corresponding main CI runs succeeded.
Base checkout was clean. Work is isolated in `fix/corrective-engineering`.

## Reproduced and corrected

| Severity | Finding / root cause | Correction | Evidence impact |
| --- | --- | --- | --- |
| HIGH | `serving_repository.py` detail/symbol filters omitted source/feed | Pin canonical source/feed/version | Mixed-source fixture leaked OTHER symbol before fix; canonical data currently not mixed |
| HIGH | Overview impact total constrained only analysis version | Apply the same canonical source/feed filter | Removing 2 noncanonical fixture rows incorrectly changed total 8→6 before fix; 9 focused tests pass after fix |
| HIGH | `event_strategy_backtest.py` PRE/POST joined across feed/source | Match provenance and filter canonical parent | Fixture calculated 10 rows instead of 1; fixed result is 1 row / 0.9% |
| HIGH | `paper_web.py` broker account lookup blocked local journal | Pinned server `ALPACA_PAPER_ACCOUNT_ID`; local journal independent; remote account match before broker operations | Existing scope identity retained, configuration now explicit |
| MEDIUM | `paper.html` Promise.all hid journal on account failure | Independent settled results; edits invalidate reviewed intent | No external order sent in this pass |
| MEDIUM | `pipeline_serving.py` silently capped work/check lists | Declared limit/offset/total/has_more, bounded pages | 501-item isolated DB regression |
| MEDIUM | `pipeline_serving.py` single-run query joined every work item to every check | Independent aggregates, direct run lookup | Avoids Cartesian intermediate rows and phantom NULL-tuple count |
| MEDIUM | Pipelines lacked selected run inspection, edges; RUNNING styled as failure | Detail, metadata, pagers, edge rendering, semantic status mapping | Browser inspected historical 2,020-work-item run and offset 100 |
| MEDIUM | Research marker snapped to nearest bar | Exact timestamp text; marker only when exact bar exists | No synthetic price; 3 JS regressions |
| LOW | Release selector used browser-local calendar day, showing next-day FOMC in Korea | Explicit UTC date label | Browser reproduced July 29 FOMC as July 30; UTC formatter regression |
| MEDIUM | Research repeated chart initialization / excess timeframe requests | Reuse comparison charts; separate bar reload | Repeated browser validation pending |
| LOW | Overview duplicated backend limitations | Render API limitations in Korean | No numeric changes |
| LOW | CI bootstrapped 3.14, project pins 3.13 | Align bootstrap to 3.13 | Previous CI was passing, not broken |

## Verification so far

- Before changes: `uv run --extra airflow --with pytest python -m pytest -q`:
  206 passed, 8 skipped, 21 subtests, 29.80 seconds.
- Batch A focused: 16 passed including actual PostgreSQL mixed provenance.
- Batch B focused: 29 passed; isolated journal/recovery subset 9 passed.
- Batch C API/repository/integration: 17 passed, 4 subtests.
- JS exact-time, UTC label, chart reuse and bar-only reload tests:
  `node --test tests/research_ui.cjs`: 6 passed, 117 ms.
- Current default suite: 208 passed, 11 opt-in skipped, 21 subtests, 15.97 seconds.
- Attempt combining default + integration environment failed a default CLI assertion
  because DATABASE_URL was intentionally overridden. Default and integration runs
  must be separate. No production change was needed for that test setup error.
- Initial isolated integration attempts: 10 passed / 1 failed (raw.market.v1 not
  initialized in the fresh test broker). After explicit topic initialization,
  all 11 integration tests passed in 31.78 seconds. This includes PostgreSQL,
  Kafka producer, Spark checkpoint/restart, Kafka→Spark→PostgreSQL, Paper
  recovery/concurrency and the new identity/pagination tests.
- `uv run --extra airflow python -m compileall -q src dags scripts tests`: exit 0.
- `uv build`: wheel and source distribution built, exit 0.
- After documentation/CI edits, full default suite: 208 passed, 11 skipped,
  21 subtests, 18.24 seconds. After archive moves: focused docs/API/pipeline
  suite 23 passed, 4 subtests, 0.43 seconds.
- Markdown relative file-target scan (fenced code excluded): 0 broken targets.
  This is not an external URL or Markdown heading-anchor validation.
- Starlette deprecation warning and post-exit Py4J closed-logger message remain.

## Current stored data: independently read, not recomputed

Read-only transaction against existing research database:

- impacts: alpaca/sip/multi_event_sip_v1 8,080;
  alpaca/sip/cpi_sip_v1 192 (separate historical analysis).
- strategy pre60_momentum_post60/v1: 2,020, calculable 1,988,
  mean -0.15649002350100603622%.
- canonical market bars: 1m 308,512; 3m 112,593; 5m 70,090; unique daily 11,700.
- macro_event_contexts 2,020.

No research database writes or external orders were performed by these tests.
Current strategy v1 has one canonical parent; durable strategy identity still
does not version source/feed/analysis separately. Future contracts need a versioned
identity/migration. Provider vintage is stored but the macro selection algorithm
is not independently versioned in the durable context key.

## Repository acceptance still pending

- Local code, subsystem audit, course mapping, UI journeys and regression checks
  are recorded below. Remote branch/CI and main synchronization are separate
  acceptance steps; local test success is not a claim that main has changed.

## Browser journeys in this pass

- Local modified app on port 8018, existing research DB read-only: Overview
  displayed 202 events, 10 symbols, 308,512 minute bars, 8,080 impacts, baseline
  -0.1565% and backend limitations. Latest run correctly labelled legacy.
- Research PCE/AAPL: repeated stored-result load, 1m→3m→5m→1m, observed counts
  174/60/36; 3m PARTIAL 6 and 5m PARTIAL 5. Exact official UTC time remains.
  Unit browser-script tests separately verify instance reuse and bar-only requests.
- Pipelines: existing historical run, 2,020 work items, metadata and offset-100
  page inspected; connected graph and repeated graph/run tab changes inspected.
- Paper with missing config: account/journal errors visible; submission disabled.
- Separate port-8019 **UI fixture**, not broker execution evidence: account GET
  returns 503 while one labelled fixture journal row remains visible. Failed
  GET-only recovery preserves the list; review then quantity change clears the
  reviewed intent and keeps submission disabled. No broker or research writes.
- Final restarted server: Overview, Research (3m→5m and repeat load), Pipelines
  (real run, offset 100, lineage), and disabled/unconfigured Paper each returned
  an empty browser console error/warning list (`dev.logs`, limit 50). This does
  not claim that intentional HTTP 503 fixture responses are successful requests.
- [Viewport screenshot receipts](../evidence/corrective-pass/README.md) were
  captured and visually inspected; no reconstructed terminal frame is used here.

## Final local verification commands

```bash
uv run --extra airflow --with pytest python -m pytest -q
# 208 passed, 12 opt-in skipped, 21 subtests, 13.64 s

node --test tests/research_ui.cjs
# 6 passed

DATABASE_URL=postgresql://market:market@127.0.0.1:55439/corrective \
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:59092 \
RUN_POSTGRES_INTEGRATION=1 RUN_PAPER_POSTGRES_INTEGRATION=1 \
RUN_KAFKA_INTEGRATION=1 RUN_SPARK_KAFKA_INTEGRATION=1 \
RUN_KAFKA_SPARK_POSTGRES_INTEGRATION=1 \
RESEARCH_TEST_DATABASE_URL=postgresql://market:market@127.0.0.1:55439/corrective \
uv run --extra airflow --with pytest python -m pytest -q tests/integration
# 12 passed, 27.48 s — disposable PostgreSQL/Kafka only

uv run --extra airflow python -m compileall -q src dags scripts tests
uv build
git diff --check
# All exit 0 after final Python changes
```

Published research metrics did not change. No original research database rows
were recalculated in this corrective pass. The extra regression fixtures exist
only in the isolated test database. Current README identity/architecture wording
was corrected; historical experiment artifacts were not rewritten.

## Dedicated audit: preserved contracts and bounded conclusions

| Subsystem | Assessment | Inspected contract / limitation |
| --- | --- | --- |
| Official release manifest / timestamps | NO ISSUE / VERIFIED within tests | `economic_event_schedule.py`: official source metadata, timezone-aware UTC and deterministic identity; no fabricated calendar expansion |
| Provider bars | NO ISSUE / VERIFIED within tests | `historical_bars.py`: validation, bounded pagination and malformed-response failures; not every real provider outage rerun |
| Context windows | NO ISSUE / VERIFIED | `market_event_context.py`: requested half-open end includes T+120 candidate; serving excludes that endpoint; observed daily sessions selected separately |
| FRED/ALFRED | NO ISSUE in preserved selection contract; precision limit | Valid provider vintage, non-null value, observation cutoff; daily series use prior day. Date-granularity vintages do not independently prove every intraday publication timestamp |
| Derived 3m/5m | NO ISSUE / VERIFIED within tests | Actual source bars only; source/expected counts, PARTIAL preserved; no forward fill |
| Schema / sink | NO ISSUE in tested keys; future provenance limit | Migrations and integration enforce business-key Upsert, OHLC/interval checks and parent FKs; strategy/context selection version limitations below |
| Impact analysis | NO ISSUE / VERIFIED within tests | `macro_event_impact.py`: half-open intervals, existing 90% and endpoint rules; macro contexts served alongside, not a predictive input |
| Strategy | HIGH, corrected | PRE/POST source/feed join fixed; current negative v1 and cost unchanged |
| Kafka | NO ISSUE / VERIFIED within tests | `market_event.py` identity includes exchange; publisher idempotence/acks and bounded trace; no global exactly-once claim |
| Spark | NO ISSUE / VERIFIED within tests | Explicit raw schema, valid-trade filtering, event identity, 1m aggregation, DISK_ONLY batch; restart and vertical tests isolated |
| Airflow / telemetry | MEDIUM observability limits, documented | Four DAG audit in `airflow-audit.md`; fixed retries, pools, mapped event units; not every failure can be durably logged during DB outage |
| Serving | HIGH, corrected | Canonical detail/symbol/overview reads; bounded run detail metadata; null remains unknown |
| Four UIs | MEDIUM, corrections under final validation | Actual run detail/edges, exact time, chart reuse, separate requests, local journal independent of account request |
| Paper | HIGH, corrected boundary | Local account scope; verify account before remote operations; journal-first core unchanged; no POST retry, no live or auto research execution |
| Tests / CI | LOW, aligned | Existing successful run 34297707069 bootstrapped 3.14.7 but uv selected CPython 3.13.15. Workflow now explicitly selects 3.13; new regressions added to CI, not yet remotely executed |
| Documentation | LOW, corrected / final review pending | Old plan and 5/6-session scripts archived with links preserved; current script kept; DAG and curriculum mapping added |
| Public safety / secrets | Intentional deployment restriction | Keys server-side, Paper writes default off; local operator-only write UI, not public auth/CSRF readiness. No keys read or committed in this pass |

The referenced Notion page could not be opened by the available web reader
(non-retryable URL access error). No Notion content was used as implementation
evidence. Repository code, tests, and preserved artifacts remain the basis.

### Not defects / intentionally not redesigned

- `schedule=None` is controlled historical backfill, not a missing cron schedule.
- Provider success plus PARTIAL observations is valid, not task failure.
- Negative exploratory mean is retained, not optimized or called a predictive model.
- Current strategy parent data is unambiguous, so no deadline-risk schema migration
  was introduced. Future alternative analysis contracts need a versioned strategy key.
- Context provider vintage is preserved, but the selection algorithm is not
  independently versioned in its durable identity.
- The old 3.14 bootstrap was not a failing CI: uv chose a supported project Python.
- Existing large-scale, September 8 direct, historical Airflow and routing-v2
  measurements remain separate and were not all rerun in this pass.
