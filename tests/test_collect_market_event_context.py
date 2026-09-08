import unittest

from scripts import collect_market_event_context
from scripts.evidence import export_multi_event_summary
from src.market_context_backfill import MarketContextResult


class MarketEventContextManifestTest(unittest.TestCase):
    def test_manifest_keeps_collection_and_observed_coverage_separate(self) -> None:
        serializer = getattr(
            collect_market_event_context,
            "market_context_manifest_item",
            None,
        )
        self.assertIsNotNone(
            serializer,
            "production must expose a serializer for the corrected coverage contract",
        )
        if serializer is None:
            return

        result = MarketContextResult(
            event_id="CPI|2026-07|2026-08-12T12:30:00Z",
            symbol="NVDA",
            session_1m_rows=178,
            derived_3m_rows=61,
            derived_5m_rows=37,
            derived_3m_partial_rows=2,
            derived_5m_partial_rows=2,
            daily_rows=15,
            daily_before=7,
            daily_event=1,
            daily_after=7,
            session_collection_status="COMPLETE",
            daily_collection_status="COMPLETE",
            overall_collection_status="COMPLETE",
            session_coverage_status="PARTIAL",
            daily_coverage_status="COMPLETE",
            derived_3m_coverage_status="PARTIAL",
            derived_5m_coverage_status="PARTIAL",
            overall_coverage_status="PARTIAL",
            pages=3,
            fallback_used=False,
        )

        self.assertEqual(
            serializer(result),
            {
                "event_id": "CPI|2026-07|2026-08-12T12:30:00Z",
                "symbol": "NVDA",
                "session_rows": 178,
                "daily_rows": 15,
                "daily_before": 7,
                "daily_event": 1,
                "daily_after": 7,
                "session_collection_status": "COMPLETE",
                "daily_collection_status": "COMPLETE",
                "overall_collection_status": "COMPLETE",
                "session_coverage_status": "PARTIAL",
                "daily_coverage_status": "COMPLETE",
                "derived_3m_coverage_status": "PARTIAL",
                "derived_5m_coverage_status": "PARTIAL",
                "overall_coverage_status": "PARTIAL",
            },
        )

    def test_public_summary_reports_each_coverage_semantic_independently(self) -> None:
        summarizer = getattr(export_multi_event_summary, "coverage_status_counts", None)
        self.assertIsNotNone(
            summarizer,
            "public evidence must summarize the corrected coverage contract",
        )
        if summarizer is None:
            return

        records = [
            {
                "session_collection_status": "COMPLETE",
                "daily_collection_status": "COMPLETE",
                "overall_collection_status": "COMPLETE",
                "session_coverage_status": "PARTIAL",
                "daily_coverage_status": "COMPLETE",
                "derived_3m_coverage_status": "PARTIAL",
                "derived_5m_coverage_status": "PARTIAL",
                "overall_coverage_status": "PARTIAL",
            },
            {
                "session_collection_status": "COMPLETE",
                "daily_collection_status": "PARTIAL",
                "overall_collection_status": "PARTIAL",
                "session_coverage_status": "COMPLETE",
                "daily_coverage_status": "PARTIAL",
                "derived_3m_coverage_status": "COMPLETE",
                "derived_5m_coverage_status": "COMPLETE",
                "overall_coverage_status": "FUTURE_SESSION_UNAVAILABLE",
            },
        ]

        self.assertEqual(
            summarizer(records),
            {
                "collection": {
                    "session": {"COMPLETE": 2},
                    "daily": {"COMPLETE": 1, "PARTIAL": 1},
                    "overall": {"COMPLETE": 1, "PARTIAL": 1},
                },
                "observed_bar_coverage": {
                    "session_1m": {"COMPLETE": 1, "PARTIAL": 1},
                    "daily": {"COMPLETE": 1, "PARTIAL": 1},
                    "derived_3m": {"COMPLETE": 1, "PARTIAL": 1},
                    "derived_5m": {"COMPLETE": 1, "PARTIAL": 1},
                    "overall": {
                        "FUTURE_SESSION_UNAVAILABLE": 1,
                        "PARTIAL": 1,
                    },
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
