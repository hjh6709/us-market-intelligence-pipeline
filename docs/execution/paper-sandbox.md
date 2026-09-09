# Isolated web Paper execution

The `/paper` product page demonstrates a manually approved Alpaca Paper workflow. It is downstream from, but technically isolated from, research.

## Browser flow

1. Read sanitized Paper account readiness and market clock.
2. Enter a request ID, symbol, quantity and limit price.
3. Select **Validate & review**. This performs no broker write.
4. Type exactly `SUBMIT PAPER ORDER`.
5. Submit to the fixed Paper endpoint when server-side write enablement is true.
6. Read the durable journal, reconcile individual status with GET, or type `CANCEL PAPER ORDER` to cancel.

## Enforced guardrails

- Paper endpoint is fixed; live endpoint selection is absent.
- server credentials never enter HTML or browser JavaScript;
- `BUY LIMIT DAY`, regular hours only;
- quantity 1–10 and notional no more than USD 1,000;
- journal commit occurs before broker POST;
- request ID reuse cannot change the original intent;
- no automatic POST retry;
- recovery and reconciliation are GET-only;
- research signals and analysis objects are not accepted as order input.

## Explicit non-guarantees

The current position comparison requires an opening baseline and therefore reports position reconciliation as unverified. Paper behavior does not prove live-trading readiness. Research signals remain `NO_TRADE`; only the separate manual form can request a Paper order.
# Local account boundary

Set server-side `ALPACA_PAPER_ACCOUNT_ID` to the existing Alpaca Paper account ID
before enabling the web sandbox. It preserves the existing `alpaca-paper:<id>`
journal identity without a broker lookup. `/api/v1/paper/orders` requires only
this pinned scope and PostgreSQL, not broker availability or credentials.
Broker operations verify that the configured credentials belong to that account;
a mismatch fails closed. Recovery is GET-only and never submits an order.

Write-enabled use is operator-controlled/local only. Authentication and CSRF
protection for a public multi-user deployment are not implemented. Keep
`ENABLE_PAPER_WEB_ORDERS=false` on public/read-only demonstrations.
