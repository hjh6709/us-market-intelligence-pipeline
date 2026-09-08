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
