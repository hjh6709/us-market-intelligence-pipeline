# Final product browser validation — 2026-09-09

Validation target: isolated completion worktree, FastAPI on `127.0.0.1:8765`, existing local PostgreSQL on `127.0.0.1:55432`.

No new provider collection, raw-trade replay, broker submission or broker cancellation was performed during this validation.

| Page | Actual browser observation | Console warnings/errors |
| --- | --- | ---: |
| `/overview` | 202 releases, 10 assets, 2,020 intervals, 308,512 1m rows, 8,080 impact rows, -0.1565% baseline | 0 |
| `/` | 202 release selector, PCE/AAPL detail, 174 observed 1m bars, PARTIAL 0, 9 chart canvases | 0 |
| `/pipelines` | latest durable run SUCCEEDED, 2,020 work items, 40 warnings, 0 failures, old-contract label visible | 0 |
| `/paper` | Paper account ACTIVE, live trading false, one canceled journal order, web review returned `broker_request_sent=false` | 0 |

Paper account identifiers and credentials are not returned to the browser. The validation used review only. `paper_order_submission_enabled=false`, so a new financial action could not be submitted from this server process.

## Captures

- [Overview](../../images/portfolio/overview.jpg)
- [Research](../../images/portfolio/research.jpg)
- [Pipelines](../../images/portfolio/pipelines.jpg)
- [Paper Execution](../../images/portfolio/paper-execution.jpg)

These captures are current product evidence. The older session-7 videos remain historical presentation evidence and do not include every final product surface.
