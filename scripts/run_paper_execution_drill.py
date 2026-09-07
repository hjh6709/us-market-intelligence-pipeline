#!/usr/bin/env python3
"""Local mock HTTP + real PostgreSQL journal; no external orders or API calls."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql

from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.paper_execution import AlpacaPaperBroker, OrderIntent, PaperOrderService


class MockPaper:
    def __init__(self):
        self.orders = {}
        self.posts = 0
        self.status = "partially_filled"
        self.lose_response = True
        self.lookup_missing = False
        self.mismatch = False

    def __call__(self, request):
        if request.method == "POST":
            self.posts += 1
            order = json.loads(request.content)
            order.update(id=str(uuid4()), status="new", filled_qty="0")
            self.orders[order["client_order_id"]] = order
            if self.lose_response:
                raise httpx.ReadTimeout("simulated lost response after acceptance", request=request)
            return httpx.Response(200, json=order)
        if request.method == "DELETE":
            self.status = "pending_cancel"
            return httpx.Response(204)
        order = self.orders.get(request.url.params["client_order_id"])
        if order is None or self.lookup_missing:
            return httpx.Response(404)
        filled_qty = "2" if self.status == "filled" else (
            "1" if self.status in {"partially_filled", "pending_cancel", "canceled"} else "0"
        )
        result = dict(order, status=self.status, filled_qty=filled_qty)
        if self.mismatch:
            result["symbol"] = "AAPL"
        return httpx.Response(200, json=result)


def run_drill(database_url):
    started = time.monotonic()
    schema = "paper_drill_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            def connect(url, **kwargs):
                return psycopg.connect(url, options=f"-csearch_path={schema}", **kwargs)
            with connect(database_url) as db:
                db.execute(Path("db/migrations/008_paper_order_intents.sql").read_text())
            mock = MockPaper()
            broker = AlpacaPaperBroker("mock-key", "mock-secret",
                                       transport=httpx.MockTransport(mock))
            def service():
                return PaperOrderService(database_url, broker, "local-mock:drill", connect=connect)
            intent = OrderIntent(request_id="lost-response", symbol="NVDA", qty=2, limit_price="100")
            first = service().run(intent, "submit", enabled=True)
            assert first["state"] == "UNKNOWN"
            recovered = service().run(intent, "submit", enabled=True)
            assert recovered["state"] == "partially_filled" and recovered["filled_qty"] == "1"
            assert mock.posts == 1
            mock.status = "filled"
            filled = service().run(intent)
            assert filled["state"] == "filled" and filled["filled_qty"] == "2"
            # A stale snapshot cannot regress final state or cumulative fills.
            mock.status = "partially_filled"
            stale = service().run(intent)
            assert stale["state"] == "filled" and stale["last_error"] is not None
            mock.status = "filled"
            mock.mismatch = True
            assert service().run(intent)["last_error"] is not None
            mock.mismatch = False
            mock.lookup_missing = True
            missing = service().run(intent, "submit", enabled=True)
            assert missing["last_error"] == "not_found_no_resubmit" and mock.posts == 1
            mock.lookup_missing = False
            conflict = intent.model_copy(update={"limit_price": intent.limit_price + 1})
            try:
                service().run(conflict, "submit", enabled=True)
            except ValueError:
                pass
            else:
                raise AssertionError("changed intent was accepted")
            parallel = OrderIntent(request_id="parallel", symbol="NVDA", qty=2, limit_price="100")
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: service().run(parallel, "submit", enabled=True), range(2)))
            assert mock.posts == 2
            mock.status = "new"
            cancel = OrderIntent(request_id="cancel", symbol="NVDA", qty=2, limit_price="100")
            service().run(cancel, "submit", enabled=True)
            canceled = service().run(cancel, "cancel", enabled=True)
            assert canceled["state"] == "pending_cancel"
            mock.status = "canceled"
            canceled = service().run(cancel)
            assert canceled["state"] == "canceled" and canceled["filled_qty"] == "1"
            disabled = OrderIntent(request_id="disabled", symbol="NVDA", qty=2, limit_price="100")
            try:
                service().run(disabled, "submit")
            except ValueError:
                pass
            else:
                raise AssertionError("disabled submission was accepted")
            with connect(database_url) as db:
                count = db.execute("SELECT count(*) FROM paper_order_intents").fetchone()[0]
            assert count == 3 and mock.posts == 3
            return {
                "environment": "local_mock_http_real_postgresql",
                "external_api_calls": 0, "actual_alpaca_orders": 0,
                "journal_rows": count, "mock_order_posts": mock.posts,
                "lost_response": first, "restart_recovery": recovered,
                "filled": filled, "cancel_confirmed": canceled,
                "concurrent_attempts": len(results), "duplicate_posts": 0,
                "checks": {name: True for name in (
                    "restart_reconciles_without_resubmit", "partial_fill_recorded",
                    "stale_snapshot_rejected", "mismatched_snapshot_rejected",
                    "not_found_does_not_resubmit", "changed_intent_rejected",
                    "concurrent_submission_serialized", "cancel_ack_not_final",
                    "disabled_submission_blocked")},
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "temporary_schema_removed": True,
            }
        finally:
            # Only this invocation's random test schema, never project market tables.
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run_drill(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    payload = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
    print(payload, end="")


if __name__ == "__main__":
    main()
