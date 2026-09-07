#!/usr/bin/env python3
"""Explicit Alpaca paper connectivity probe; historical signals are not accepted."""
import argparse
import json
import os
import sys
from pathlib import Path

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.live_market_smoke import _read_env_file
from src.paper_execution import AlpacaPaperBroker, OrderIntent, PaperOrderService


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["account", "clock", "recover", "submit", "reconcile", "cancel"])
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--request-id")
    parser.add_argument("--symbol")
    parser.add_argument("--qty", type=int)
    parser.add_argument("--limit-price")
    parser.add_argument("--enable-paper-orders", action="store_true")
    parser.add_argument("--output", type=Path, help="Save a sanitized recovery report (recover only)")
    args = parser.parse_args()
    values = {**_read_env_file(args.env_file), **os.environ}
    try:
        if args.output and args.action != "recover":
            raise ValueError("output is only supported for recovery")
        if values.get("ALPACA_PAPER_KEY_ID") or values.get("ALPACA_PAPER_SECRET_KEY"):
            key, secret = values.get("ALPACA_PAPER_KEY_ID", ""), values.get("ALPACA_PAPER_SECRET_KEY", "")
        else:
            key, secret = values.get("APCA_API_KEY_ID", ""), values.get("APCA_API_SECRET_KEY", "")
        broker = AlpacaPaperBroker(key, secret)
        account = broker.account()
        ready = (account.get("status") == "ACTIVE"
                 and account.get("trading_blocked") is False
                 and account.get("account_blocked") is False)
        if args.action == "account":
            print(json.dumps({"broker": "alpaca-paper", "account_ready": ready,
                              "order_submitted": False}))
            return 0 if ready else 1
        if args.action == "clock":
            print(json.dumps({"broker": "alpaca-paper", **broker.clock()}, indent=2))
            return 0
        service = PaperOrderService(values.get("DATABASE_URL", DEFAULT_DATABASE_URL),
                                    broker, "alpaca-paper:" + str(account["id"]))
        if args.action == "recover":
            result = {"broker": "alpaca-paper", "market_clock": broker.clock(), **service.recover()}
            payload = json.dumps(result, indent=2) + "\n"
            if args.output:
                # Do not overwrite credentials, a previous report, or any existing file.
                with args.output.open("x") as report:
                    report.write(payload)
            print(payload, end="")
            return 0 if result["journal_status"] == "TERMINAL_SNAPSHOT_CONFIRMED" and all(
                position["observed_account_qty"] is not None for position in result["positions"]
            ) else 1
        intent = OrderIntent(request_id=args.request_id, symbol=args.symbol,
                             qty=args.qty, limit_price=args.limit_price)
        if args.action == "submit" and not ready:
            raise ValueError("paper account is not available for trading")
        result = service.run(intent, args.action, enabled=args.enable_paper_orders)
        print(json.dumps({"broker": "alpaca-paper", **result}, indent=2))
        return 1 if result["last_error"] else 0
    except Exception as error:
        # Do not print raw transport or database errors, credentials, or account payloads.
        print(f"paper probe failed ({type(error).__name__}); check configuration and journal",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
