# Baseline audit — 2026-09-10

## Repository baseline

- independently fetched remote: `origin/main`
- verified remote HEAD: `63633a50c85c88e507be458067ecf6706220f813`
- isolated branch: `architecture-contract-2026-09-10`
- original local `main` was not modified
- pre-change suite: `220` tests, `OK`, `12` skipped, 13.083 seconds

## Implemented at baseline

- official event catalog and 202×10 market-context collection using Alpaca SIP bars;
- FRED/ALFRED point-in-time macro context selection;
- legacy event impacts and pre-60/post-60 exploratory strategy results;
- PostgreSQL curated tables, deterministic keys, pipeline telemetry for covered DAGs;
- FastAPI/browser research, quality, pipeline and guarded Alpaca Paper pages;
- immutable local raw-trade archive, bounded Kafka replay and Spark validation path.

## Contradictions found

| Existing statement/model | Baseline evidence | Resolution |
| --- | --- | --- |
| old MVP docs present IEX/news/LLM/automated trading as the architecture | current code uses Alpaca SIP, FRED/ALFRED and manual Paper isolation | mark old documents superseded; canonical contract separates current and target |
| `economic_events` contains `actual`, `forecast`, `surprise` in one mutable row | migration `002` | retain for compatibility; add append-only target foundations rather than rewrite evidence |
| calendar-relative event windows are treated as analysis windows | current context/impact code | preserve legacy outputs; add verified session planner foundation before changing metrics |
| `COMPLETE` can be read as both request success and data quality | current historical evidence and evolving coverage code | canonical four-axis quality contract; no historical evidence rewrite |
| Paper order journal can look like strategy execution evidence | migration `008` and Paper service | label as manual intent; target experiment/fill/outcome model remains separate |

## Current limitations

The baseline does not implement official release revision ingestion, consensus snapshot ingestion, verified exchange-calendar refresh, the target reaction metric set, comparable-event search, walk-forward validation, managed deployment or automated research-to-order execution. Historical evidence remains immutable and proves only its recorded run.
