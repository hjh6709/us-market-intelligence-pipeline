import unittest
from datetime import UTC, datetime

import src.platform_contracts as contracts


class PlatformContractsTest(unittest.TestCase):
    def test_good_friday_has_no_observations_and_metric_is_not_applicable(self) -> None:
        assessment = contracts.QualityAssessment(
            work_item_outcome=contracts.WorkItemOutcome.SUCCEEDED,
            market_interval_type=contracts.MarketIntervalType.CLOSED,
            coverage_status=contracts.CoverageStatus.NO_OBSERVATIONS,
            analysis_eligibility=contracts.AnalysisEligibility.NOT_APPLICABLE,
            reason_code=contracts.ReasonCode.MARKET_CLOSED,
            reason_detail="Good Friday",
        )

        self.assertTrue(assessment.work_item_succeeded)
        self.assertFalse(assessment.analysis_allowed)

    def test_provider_safety_lag_is_availability_not_coverage(self) -> None:
        assessment = contracts.QualityAssessment(
            work_item_outcome=contracts.WorkItemOutcome.DATA_NOT_AVAILABLE,
            market_interval_type=contracts.MarketIntervalType.REGULAR,
            coverage_status=contracts.CoverageStatus.NO_OBSERVATIONS,
            analysis_eligibility=contracts.AnalysisEligibility.NOT_YET_MATURE,
            reason_code=contracts.ReasonCode.PROVIDER_SAFETY_LAG,
            reason_detail="provider publishes after a safety delay",
            eligible_at=datetime(2026, 9, 11, 20, tzinfo=UTC),
        )

        self.assertEqual(assessment.work_item_outcome.value, "DATA_NOT_AVAILABLE")
        self.assertEqual(assessment.coverage_status.value, "NO_OBSERVATIONS")

    def test_sparse_premarket_can_remain_eligible(self) -> None:
        assessment = contracts.QualityAssessment(
            work_item_outcome=contracts.WorkItemOutcome.SUCCEEDED,
            market_interval_type=contracts.MarketIntervalType.EXTENDED,
            coverage_status=contracts.CoverageStatus.SPARSE_EXPECTED,
            analysis_eligibility=contracts.AnalysisEligibility.ELIGIBLE,
            reason_code=contracts.ReasonCode.SPARSE_EXTENDED_HOURS,
            reason_detail="valid premarket prints",
        )

        self.assertTrue(assessment.analysis_allowed)

    def test_future_s_plus_7_is_not_yet_mature(self) -> None:
        eligible_at = datetime(2026, 9, 21, 20, tzinfo=UTC)

        self.assertEqual(
            contracts.metric_maturity(eligible_at, as_of=datetime(2026, 9, 10, tzinfo=UTC)),
            contracts.AnalysisEligibility.NOT_YET_MATURE,
        )
        self.assertEqual(
            contracts.metric_maturity(eligible_at, as_of=eligible_at),
            contracts.AnalysisEligibility.ELIGIBLE,
        )

    def test_failed_work_item_cannot_be_analysis_eligible(self) -> None:
        with self.assertRaisesRegex(ValueError, "failed work item"):
            contracts.QualityAssessment(
                work_item_outcome=contracts.WorkItemOutcome.FAILED,
                market_interval_type=contracts.MarketIntervalType.REGULAR,
                coverage_status=contracts.CoverageStatus.PARTIAL,
                analysis_eligibility=contracts.AnalysisEligibility.ELIGIBLE,
                reason_code=contracts.ReasonCode.PROVIDER_TIMEOUT,
                reason_detail="timeout",
            )

    def test_reaction_vocabulary_and_definitions_are_complete(self) -> None:
        expected = {
            "POST_1M",
            "POST_5M",
            "POST_15M",
            "POST_30M",
            "POST_60M",
            "RELEASE_TO_OPEN",
            "OPEN_GAP",
            "OPEN_30M",
            "OPEN_60M",
            "EVENT_TO_CLOSE",
            "SESSION_RETURN_S0",
            "S0_CLOSE_TO_S+1_CLOSE",
            "S0_CLOSE_TO_S+3_CLOSE",
            "S0_CLOSE_TO_S+7_CLOSE",
            "EVENT_VOLUME_RATIO",
            "OPEN_60M_VOLUME_RATIO",
            "EVENT_REALIZED_VOL",
            "EVENT_VOLATILITY_RATIO",
        }

        self.assertEqual(contracts.REACTION_METRIC_VERSION, "event_session_reaction_v2")
        self.assertEqual({metric.value for metric in contracts.ReactionMetric}, expected)
        self.assertEqual(set(contracts.REACTION_METRIC_DEFINITIONS), set(contracts.ReactionMetric))
        for definition in contracts.REACTION_METRIC_DEFINITIONS.values():
            with self.subTest(metric=definition.metric):
                self.assertEqual(definition.contract_version, contracts.REACTION_METRIC_VERSION)
                self.assertTrue(definition.category)
                self.assertTrue(definition.anchor_marker)
                self.assertTrue(definition.start_endpoint)
                self.assertTrue(definition.end_endpoint)
                self.assertTrue(definition.start_price)
                self.assertTrue(definition.end_price)
                self.assertTrue(definition.session_clipping)
                self.assertGreaterEqual(definition.endpoint_tolerance_seconds, 0)
                self.assertTrue(definition.maturity_rule)
                self.assertTrue(definition.applicability_rule)

    def test_pre_event_drift_is_context_not_reaction(self) -> None:
        self.assertNotIn("PRE_EVENT_DRIFT_60M", {item.value for item in contracts.ReactionMetric})
        self.assertEqual(
            contracts.ContextMetric.PRE_EVENT_DRIFT_60M.value,
            "PRE_EVENT_DRIFT_60M",
        )


if __name__ == "__main__":
    unittest.main()
