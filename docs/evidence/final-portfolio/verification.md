# Final verification — 2026-09-09

Baseline was independently fetched and verified as `origin/main` `ef8de1184d2b4f9080799a4ea8bde4470a49d999` before work began. Validation ran from the isolated `audit/final-portfolio-completion` worktree.

## Automated results

| Scope | Result |
| --- | --- |
| Full unit/default suite | **213 passed, 8 skipped**, 12.957 s |
| Isolated PostgreSQL integration | **5 passed**, 5.024 s |
| Kafka producer + Spark checkpoint + Kafka→Spark→PostgreSQL | **3 passed**, 25.723 s |
| Python compileall | passed |
| Source distribution + wheel build | passed |
| Wheel contents | four HTML templates and both vendored chart libraries present |
| Browser validation | four product pages, no console warning/error |

The first Kafka integration attempt failed because no broker was listening on port 9092. The isolated compose Kafka service was then started and the exact test group passed. This was an environment startup failure, not a passing test being reported as the first attempt.

## Isolated infrastructure

- compose project: `final-portfolio-validation`
- PostgreSQL: port 55433, separate volume
- Kafka: port 9092, temporary per-test topics
- existing research PostgreSQL: port 55432, read-only for browser validation

The destructive integration test that truncates `market_bars` was run only against port 55433. It never targeted the existing 308,512-row research database.

## Vertical evidence

- Spark test finalized OHLC `100 / 105 / 99 / 102`, volume 11, trade count 4, VWAP 101.727273.
- Two late rows were dropped by watermark.
- Checkpoint restart retained one output row without duplication.
- Kafka→Spark→PostgreSQL published five events, retained three unique finalized trades and kept one row before/after restart.
- Paper integration exercised durable recovery, concurrent intent protection, partial fill, cancellation and isolated account scope against mocks plus PostgreSQL; it made no external broker request.

## Current boundaries

- latest durable Airflow UI record predates the coverage-separation contract and is labelled `legacy-pre-separation`;
- the 2026-09-08 direct revalidation is documented separately and is not rewritten as an Airflow run;
- Paper browser write remains disabled by default;
- position reconciliation still requires an opening baseline;
- OpenLineage/Marquez was not added because the core product already exposes durable lineage and the deadline prioritizes correctness and reproducibility.
