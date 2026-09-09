"""Canonical research identity; use a disposable database, never research storage."""

import os
import unittest
from pathlib import Path
from uuid import uuid4
from decimal import Decimal

import psycopg

from src.event_strategy_backtest import calculate_and_store
from src.serving_repository import PostgresServingRepository


@unittest.skipUnless(os.environ.get("RESEARCH_TEST_DATABASE_URL"), "requires isolated research test DB")
class ResearchIdentityTest(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["RESEARCH_TEST_DATABASE_URL"]
        self.event = "identity-test-" + uuid4().hex
        self.repo = PostgresServingRepository(self.url)
        with psycopg.connect(self.url) as conn:
            for name in ("001_market_bars.sql", "002_macro_event_analysis.sql",
                         "004_pipeline_experiments.sql", "007_event_strategy_results.sql"):
                conn.execute(Path("db/migrations", name).read_text())
            conn.execute("""INSERT INTO economic_events
                (economic_event_id,event_type,reference_period,released_at,
                 original_timezone,release_source,release_source_url)
                VALUES (%s,'TEST',%s,'2026-08-12T12:30:00Z','UTC','fixture','fixture')""",
                (self.event, self.event))
            for source, feed, symbol, value in (
                ("alpaca", "sip", "NVDA", "1"),
                ("alpaca", "iex", "NVDA", "9"),
                ("other", "sip", "NVDA", "20"),
                ("alpaca", "iex", "OTHER", "9"),
            ):
                for window in ("PRE_60M", "POST_60M"):
                    conn.execute("""INSERT INTO macro_event_impacts
                        (impact_id,economic_event_id,symbol,source,feed,session_scope,
                         window_name,window_start,window_end,open_price,close_price,
                         return_pct,coverage_status,analysis_version)
                        VALUES (%s,%s,%s,%s,%s,'extended',%s,
                        '2026-08-12T12:30:00Z','2026-08-12T13:30:00Z',100,101,%s,
                        'COMPLETE','multi_event_sip_v1')""",
                        (uuid4().hex, self.event, symbol, source, feed, window, Decimal(value)))

    def tearDown(self):
        with psycopg.connect(self.url) as conn:
            for table in ("event_strategy_results", "macro_event_impacts", "economic_events"):
                conn.execute(f"DELETE FROM {table} WHERE economic_event_id=%s", (self.event,))

    def test_detail_and_symbols_ignore_noncanonical_provenance(self):
        self.assertEqual(self.repo.list_symbols(self.event), ["NVDA"])
        impacts = self.repo.get_impacts(self.event, "NVDA")
        self.assertEqual(len(impacts), 2)
        self.assertEqual({item.return_pct for item in impacts}, {Decimal("1")})

    def test_strategy_pairs_only_canonical_pre_and_post(self):
        result = calculate_and_store(self.url, event_ids=[self.event])
        self.assertEqual(result["rows"], 1)
        self.assertEqual(Decimal(result["mean_net_return_pct"]), Decimal("0.9"))
        self.assertEqual(self.repo.get_strategy_result(self.event, "NVDA").net_return_pct,
                         Decimal("0.9"))

    def test_overview_impact_total_ignores_noncanonical_rows(self):
        before = self.repo.get_overview_metrics().impact_rows
        with psycopg.connect(self.url) as conn:
            conn.execute("DELETE FROM macro_event_impacts WHERE economic_event_id=%s AND source='other'", (self.event,))
        self.assertEqual(self.repo.get_overview_metrics().impact_rows, before)
