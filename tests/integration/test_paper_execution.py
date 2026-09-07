import os
import unittest

from scripts.run_paper_execution_drill import run_drill
from src.cpi_ingestion import DEFAULT_DATABASE_URL


@unittest.skipUnless(os.environ.get("RUN_PAPER_POSTGRES_INTEGRATION") == "1",
                     "set RUN_PAPER_POSTGRES_INTEGRATION=1 for isolated paper journal drill")
class PaperExecutionIntegrationTest(unittest.TestCase):
    def test_durable_recovery_concurrency_and_cancel(self):
        result = run_drill(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
        self.assertEqual(result["journal_rows"], 3)
        self.assertEqual(result["mock_order_posts"], 3)
        self.assertTrue(all(result["checks"].values()))
