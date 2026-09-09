import os
import unittest
from pathlib import Path
from uuid import uuid4

import psycopg

from src.pipeline_serving import PostgresPipelineRepository, PipelineServingService


@unittest.skipUnless(os.environ.get("RESEARCH_TEST_DATABASE_URL"), "requires isolated test DB")
class PipelineDetailTest(unittest.TestCase):
    def test_detail_pages_large_run_and_preserves_unknown_counts(self):
        url = os.environ["RESEARCH_TEST_DATABASE_URL"]
        run = "pagination-" + uuid4().hex
        service = PipelineServingService(PostgresPipelineRepository(url))
        with psycopg.connect(url) as db:
            db.execute(Path("db/migrations/006_pipeline_runs.sql").read_text())
            db.execute("""INSERT INTO pipeline_runs VALUES
                (%s,'test-dag','{}','test-hash',now(),'test-version','RUNNING',now(),NULL)""", (run,))
            db.execute("""INSERT INTO pipeline_work_items
                (pipeline_run_id,economic_event_id,symbol,stage,status)
                SELECT %s,'event-' || lpad(i::text,4,'0'),'NVDA','collect','SUCCEEDED'
                FROM generate_series(1,501) i""", (run,))
        try:
            first = service.detail(run)
            self.assertEqual(first.work_items_page.total, 501)
            self.assertTrue(first.work_items_page.has_more)
            self.assertEqual(first.checks_page.total, 0)
            self.assertIsNone(first.run.input_count)
            self.assertEqual(first.run.status, "RUNNING")
            second = service.detail(run, work_offset=100)
            self.assertFalse({x.economic_event_id for x in first.work_items} &
                             {x.economic_event_id for x in second.work_items})
            with psycopg.connect(url) as db:
                db.execute("""INSERT INTO pipeline_run_checks
                    (pipeline_run_id,economic_event_id,symbol,stage,check_name,
                     status,alert_status,checked_at)
                    SELECT %s,'event-' || lpad(i::text,4,'0'),'NVDA','collect',
                           'coverage','WARN','NONE',now()
                    FROM generate_series(1,501) i""", (run,))
            checks_first = service.detail(run)
            checks_last = service.detail(run, work_offset=500, check_offset=500)
            self.assertEqual(checks_first.checks_page.total, 501)
            self.assertTrue(checks_first.checks_page.has_more)
            self.assertEqual(len(checks_first.checks), 100)
            self.assertEqual(len(checks_last.checks), 1)
            self.assertFalse(checks_last.checks_page.has_more)
            self.assertEqual(len(checks_last.work_items), 1)
            self.assertFalse(checks_last.work_items_page.has_more)
            self.assertEqual(checks_last.run.work_item_count, 501)
            self.assertEqual(checks_last.run.warning_check_count, 501)
            beyond = service.detail(run, work_offset=1000, check_offset=1000)
            self.assertEqual(beyond.work_items, [])
            self.assertEqual(beyond.checks, [])
            self.assertEqual(beyond.checks_page.total, 501)
        finally:
            with psycopg.connect(url) as db:
                db.execute("DELETE FROM pipeline_run_checks WHERE pipeline_run_id=%s", (run,))
                db.execute("DELETE FROM pipeline_work_items WHERE pipeline_run_id=%s", (run,))
                db.execute("DELETE FROM pipeline_runs WHERE pipeline_run_id=%s", (run,))
