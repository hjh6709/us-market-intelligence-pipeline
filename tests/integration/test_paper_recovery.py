import os
from pathlib import Path
import unittest
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql

from scripts.run_paper_execution_drill import MockPaper
from src.cpi_ingestion import DEFAULT_DATABASE_URL
from src.paper_execution import AlpacaPaperBroker, OrderIntent, PaperOrderService


@unittest.skipUnless(os.environ.get("RUN_PAPER_POSTGRES_INTEGRATION") == "1", "opt-in PostgreSQL test")
class PaperRecoveryTest(unittest.TestCase):
    def test_restart_reads_journal_without_submitting_or_assuming_empty_account(self):
        url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
        schema = "paper_recovery_" + uuid4().hex
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            try:
                def connect(url, **kwargs):
                    return psycopg.connect(url, options=f"-csearch_path={schema}", **kwargs)
                with connect(url) as db:
                    db.execute(Path("db/migrations/008_paper_order_intents.sql").read_text())
                mock = MockPaper()
                position_fails = False
                def handle(request):
                    if request.url.path.startswith("/v2/positions/"):
                        self.assertEqual(request.method, "GET")
                        if position_fails:
                            return httpx.Response(503)
                        # 7 shares existed outside this journal; never call this mismatch.
                        return httpx.Response(200, json={"symbol": "NVDA", "qty": "9"})
                    return mock(request)
                broker = AlpacaPaperBroker("mock", "mock", transport=httpx.MockTransport(handle))
                def service():
                    return PaperOrderService(url, broker, "local-mock:recovery", connect=connect)
                self.assertEqual(service().recover()["journal_status"], "EMPTY")
                intent = OrderIntent(request_id="restart", symbol="NVDA", qty=2, limit_price="100")
                service().run(intent, "submit", enabled=True)
                partial = service().recover()
                self.assertEqual(partial["orders"][0]["filled_qty"], "1")
                self.assertEqual(partial["nonterminal_orders"], 1)
                self.assertEqual(partial["journal_status"], "ATTENTION_REQUIRED")
                mock.status = "filled"
                result = service().recover()
                self.assertEqual(result["journal_status"], "TERMINAL_SNAPSHOT_CONFIRMED")
                self.assertEqual(result["positions"][0], {
                    "symbol": "NVDA", "journal_buy_filled_qty": "2",
                    "observed_account_qty": "9", "comparison_status": "BASELINE_REQUIRED"})
                self.assertFalse(result["position_reconciliation_verified"])
                self.assertEqual(mock.posts, 1)
                position_fails = True
                self.assertIsNone(service().recover()["positions"][0]["observed_account_qty"])
                mock.lookup_missing = True
                unresolved = service().recover()
                self.assertEqual(unresolved["order_lookup_errors"], 1)
                self.assertEqual(unresolved["orders"][0]["filled_qty"], "2")
                self.assertEqual(mock.posts, 1)
                # A different account scope cannot read this account's orders.
                other = PaperOrderService(url, broker, "local-mock:other", connect=connect)
                self.assertEqual(other.recover()["journal_orders"], 0)
                with connect(url) as db:
                    db.execute("""INSERT INTO paper_order_intents
                        (scope, request_id, client_order_id, intent)
                        SELECT 'local-mock:other', 'bulk-' || i, 'bulk-client-' || i, '{}'::jsonb
                        FROM generate_series(1,101) AS i""")
                with self.assertRaisesRegex(ValueError, "at most 100"):
                    other.recover()
                self.assertEqual(mock.posts, 1)
            finally:
                admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
