import unittest

import src.platform_contracts as contracts


class PlatformContractsTest(unittest.TestCase):
    def setUp(self) -> None:
        required = (
            "AnalysisEligibility",
            "CollectionStatus",
            "CoverageStatus",
            "MarketStatus",
            "QualityAssessment",
            "REACTION_METRIC_VERSION",
            "ReactionMetric",
        )
        missing = [name for name in required if not hasattr(contracts, name)]
        self.assertEqual(missing, [], f"missing platform contracts: {missing}")

    def test_successful_collection_can_be_closed_and_not_applicable(self) -> None:
        assessment = contracts.QualityAssessment(
            collection_status=contracts.CollectionStatus.SUCCEEDED,
            market_status=contracts.MarketStatus.CLOSED,
            coverage_status=contracts.CoverageStatus.NOT_APPLICABLE,
            analysis_eligibility=contracts.AnalysisEligibility.INELIGIBLE,
            reason="verified market holiday",
        )

        self.assertTrue(assessment.collection_succeeded)
        self.assertFalse(assessment.analysis_allowed)
        self.assertEqual(assessment.coverage_status.value, "NOT_APPLICABLE")

    def test_failed_collection_cannot_be_analysis_eligible(self) -> None:
        with self.assertRaisesRegex(ValueError, "failed collection"):
            contracts.QualityAssessment(
                collection_status=contracts.CollectionStatus.FAILED,
                market_status=contracts.MarketStatus.OPEN,
                coverage_status=contracts.CoverageStatus.EMPTY,
                analysis_eligibility=contracts.AnalysisEligibility.ELIGIBLE,
                reason="provider timeout",
            )

    def test_target_reaction_vocabulary_covers_intraday_and_session_anchors(self) -> None:
        self.assertEqual(contracts.REACTION_METRIC_VERSION, "event_session_reaction_v1")
        self.assertEqual(
            {metric.value for metric in contracts.ReactionMetric},
            {
                "PRE_EVENT_DRIFT_60M",
                "POST_EVENT_RETURN_5M",
                "POST_EVENT_RETURN_15M",
                "POST_EVENT_RETURN_30M",
                "POST_EVENT_RETURN_60M",
                "POST_EVENT_RETURN_TO_SESSION_CLOSE",
                "POST_EVENT_RETURN_TO_NEXT_SESSION_CLOSE",
            },
        )


if __name__ == "__main__":
    unittest.main()
