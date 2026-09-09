# Corrective pass browser receipts — 2026-09-09

Actual local browser captures of the modified app on port 8018. Research data was
read from the existing PostgreSQL instance. Paper credentials were not loaded and
Paper writes were disabled. These are UI verification screenshots, not a new
provider ingestion run or broker-order demonstration.

- [Overview](overview.png): current stored counts and negative baseline.
- [Research](research.png): UTC release label and stored PCE/AAPL result.
- [Pipeline run](pipeline-detail.png): durable run list and drill-down entry.
- [Lineage](pipeline-lineage.png): connected research path; lower validation and
  operations lanes remain accessible by scrolling in the actual page.
- [Paper disabled](paper-disabled.png): configuration error shown, no silent empty
  account, writes locked.

Repeated interactions, fixture-only broker outage behavior, console results and
exact test commands are in the [engineering audit](../../engineering/corrective-pass.md).
The temporary outage UI fixture was explicitly labelled `FIXTURE`; it was not an
actual Alpaca failure and did not call a broker. Its server has been stopped.
