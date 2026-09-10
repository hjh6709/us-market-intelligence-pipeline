import unittest
from datetime import UTC, datetime

import src.platform_contracts as contracts
from src.trading_sessions import ReleasePhase


class PlatformContractsTest(unittest.TestCase):
    def test_T51_closed_market_post_metrics_are_not_applicable(self) -> None:
        post_metrics = (
            contracts.ReactionMetric.POST_1M,
            contracts.ReactionMetric.POST_5M,
            contracts.ReactionMetric.POST_15M,
            contracts.ReactionMetric.POST_30M,
            contracts.ReactionMetric.POST_60M,
        )

        for metric in post_metrics:
            with self.subTest(metric=metric):
                window = contracts.resolve_reaction_window(
                    metric,
                    marker_at=datetime(2026, 8, 15, 14, 0, tzinfo=UTC),
                    release_phase=ReleasePhase.MARKET_CLOSED,
                    s0_open=datetime(2026, 8, 17, 13, 30, tzinfo=UTC),
                    s0_close=datetime(2026, 8, 17, 20, 0, tzinfo=UTC),
                )

                self.assertEqual(
                    window.analysis_eligibility,
                    contracts.AnalysisEligibility.NOT_APPLICABLE,
                )
                self.assertIsNone(window.start_at)
                self.assertIsNone(window.end_at)

    def test_T39_late_regular_post_metric_cannot_cross_session_close(self) -> None:
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.POST_30M,
            marker_at=datetime(2026, 8, 12, 19, 50, tzinfo=UTC),
            release_phase=ReleasePhase.REGULAR_SESSION,
            s0_open=datetime(2026, 8, 12, 13, 30, tzinfo=UTC),
            s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
        )

        self.assertEqual(
            window.analysis_eligibility,
            contracts.AnalysisEligibility.NOT_APPLICABLE,
        )
        self.assertIsNone(window.end_at)

    def test_T40_arbitrary_release_phase_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "release_phase"):
            contracts.resolve_reaction_window(
                contracts.ReactionMetric.EVENT_TO_CLOSE,
                marker_at=datetime(2026, 8, 12, 19, 50, tzinfo=UTC),
                release_phase="POSTMARKET",
                s0_open=datetime(2026, 8, 12, 13, 30, tzinfo=UTC),
                s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
            )

    def test_T41_no_observations_cannot_be_eligible(self) -> None:
        with self.assertRaisesRegex(ValueError, "no observations.*eligible"):
            contracts.QualityAssessment(
                contracts.WorkItemOutcome.SUCCEEDED,
                contracts.MarketIntervalType.REGULAR,
                contracts.CoverageStatus.NO_OBSERVATIONS,
                contracts.AnalysisEligibility.ELIGIBLE,
            )

    def test_T42_abnormal_quality_state_requires_reason(self) -> None:
        with self.assertRaisesRegex(ValueError, "abnormal.*reason"):
            contracts.QualityAssessment(
                contracts.WorkItemOutcome.FAILED,
                contracts.MarketIntervalType.REGULAR,
                contracts.CoverageStatus.PARTIAL,
                contracts.AnalysisEligibility.INSUFFICIENT_DATA,
            )

    def test_reaction_contract_uses_typed_price_and_activity_definitions(self) -> None:
        post = contracts.PRICE_REACTION_METRIC_DEFINITIONS[
            contracts.ReactionMetric.POST_5M
        ]
        release_to_open = contracts.PRICE_REACTION_METRIC_DEFINITIONS[
            contracts.ReactionMetric.RELEASE_TO_OPEN
        ]
        realized = contracts.ACTIVITY_METRIC_DEFINITIONS[
            contracts.ReactionMetric.EVENT_REALIZED_VOL
        ]

        self.assertIsInstance(post.category, contracts.MetricCategory)
        self.assertIsInstance(post.start_endpoint, contracts.EndpointType)
        self.assertIs(post.start_price, contracts.PriceField.CLOSE)
        self.assertIs(post.clipping_policy, contracts.ClippingPolicy.NO_CROSS_SESSION_FILL)
        self.assertIsNone(post.pre_open_reference_max_age_seconds)
        self.assertGreater(release_to_open.pre_open_reference_max_age_seconds, 0)
        self.assertEqual(realized.formula, "sqrt(sum(log_return^2))")
        self.assertIs(realized.annualization, contracts.Annualization.NONE)
        self.assertFalse(hasattr(realized, "start_price"))

    def test_T23_post_market_event_to_close_is_not_applicable(self) -> None:
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.EVENT_TO_CLOSE,
            marker_at=datetime(2026, 8, 12, 20, 30, tzinfo=UTC),
            release_phase=ReleasePhase.POST_MARKET,
            s0_open=datetime(2026, 8, 13, 13, 30, tzinfo=UTC),
            s0_close=datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        )

        self.assertEqual(
            window.analysis_eligibility,
            contracts.AnalysisEligibility.NOT_APPLICABLE,
        )
        self.assertIsNone(window.start_at)
        self.assertIsNone(window.end_at)

    def test_T24_0830_post_5m_uses_0829_close_to_0834_close(self) -> None:
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.POST_5M,
            marker_at=datetime(2026, 8, 12, 12, 30, tzinfo=UTC),
            release_phase=ReleasePhase.PRE_MARKET,
            s0_open=datetime(2026, 8, 12, 13, 30, tzinfo=UTC),
            s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
        )

        self.assertEqual(window.start_at, datetime(2026, 8, 12, 12, 29, tzinfo=UTC))
        self.assertEqual(window.end_at, datetime(2026, 8, 12, 12, 34, tzinfo=UTC))
        self.assertEqual((window.start_price, window.end_price), ("CLOSE", "CLOSE"))

    def test_T25_release_to_open_ends_at_last_valid_pre_open_close(self) -> None:
        reference = datetime(2026, 8, 12, 13, 29, tzinfo=UTC)
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.RELEASE_TO_OPEN,
            marker_at=datetime(2026, 8, 12, 12, 30, tzinfo=UTC),
            release_phase=ReleasePhase.PRE_MARKET,
            s0_open=datetime(2026, 8, 12, 13, 30, tzinfo=UTC),
            s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
            pre_open_reference_at=reference,
        )

        self.assertEqual(window.start_at, datetime(2026, 8, 12, 12, 29, tzinfo=UTC))
        self.assertEqual(window.end_at, reference)
        self.assertEqual((window.start_price, window.end_price), ("CLOSE", "CLOSE"))

    def test_T26_open_gap_uses_pre_open_close_to_0930_open(self) -> None:
        reference = datetime(2026, 8, 12, 13, 29, tzinfo=UTC)
        open_at = datetime(2026, 8, 12, 13, 30, tzinfo=UTC)
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.OPEN_GAP,
            marker_at=datetime(2026, 8, 12, 12, 30, tzinfo=UTC),
            release_phase=ReleasePhase.PRE_MARKET,
            s0_open=open_at,
            s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
            pre_open_reference_at=reference,
        )

        self.assertEqual((window.start_at, window.end_at), (reference, open_at))
        self.assertEqual((window.start_price, window.end_price), ("CLOSE", "OPEN"))

    def test_T27_open_30m_uses_0930_open_to_0959_close(self) -> None:
        open_at = datetime(2026, 8, 12, 13, 30, tzinfo=UTC)
        window = contracts.resolve_reaction_window(
            contracts.ReactionMetric.OPEN_30M,
            marker_at=datetime(2026, 8, 12, 12, 30, tzinfo=UTC),
            release_phase=ReleasePhase.PRE_MARKET,
            s0_open=open_at,
            s0_close=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
        )

        self.assertEqual(window.start_at, open_at)
        self.assertEqual(window.end_at, datetime(2026, 8, 12, 13, 59, tzinfo=UTC))
        self.assertEqual((window.start_price, window.end_price), ("OPEN", "CLOSE"))

    def test_T28_marker_id_is_part_of_reaction_identity(self) -> None:
        statement = contracts.ReactionIdentity(
            economic_event_marker_id="fomc:statement:v1",
            symbol="SPY",
            reaction_metric=contracts.ReactionMetric.POST_5M,
        )
        press = contracts.ReactionIdentity(
            economic_event_marker_id="fomc:press:v1",
            symbol="SPY",
            reaction_metric=contracts.ReactionMetric.POST_5M,
        )

        self.assertNotEqual(statement, press)
        self.assertEqual(statement.reaction_metric, press.reaction_metric)

    def test_T29_normal_quality_success_without_reason_is_valid(self) -> None:
        assessment = contracts.QualityAssessment(
            work_item_outcome=contracts.WorkItemOutcome.SUCCEEDED,
            market_interval_type=contracts.MarketIntervalType.REGULAR,
            coverage_status=contracts.CoverageStatus.COMPLETE,
            analysis_eligibility=contracts.AnalysisEligibility.ELIGIBLE,
            reason_code=None,
            reason_detail=None,
        )

        self.assertTrue(assessment.analysis_allowed)

    def test_T30_data_not_available_with_complete_coverage_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "data-not-available.*complete"):
            contracts.QualityAssessment(
                work_item_outcome=contracts.WorkItemOutcome.DATA_NOT_AVAILABLE,
                market_interval_type=contracts.MarketIntervalType.REGULAR,
                coverage_status=contracts.CoverageStatus.COMPLETE,
                analysis_eligibility=contracts.AnalysisEligibility.INSUFFICIENT_DATA,
                reason_code=contracts.ReasonCode.SOURCE_NOT_PUBLISHED,
                reason_detail="not published",
            )

    def test_T31_skipped_work_item_cannot_be_eligible(self) -> None:
        with self.assertRaisesRegex(ValueError, "skipped work item"):
            contracts.QualityAssessment(
                work_item_outcome=contracts.WorkItemOutcome.SKIPPED,
                market_interval_type=contracts.MarketIntervalType.REGULAR,
                coverage_status=contracts.CoverageStatus.NO_OBSERVATIONS,
                analysis_eligibility=contracts.AnalysisEligibility.ELIGIBLE,
                reason_code=contracts.ReasonCode.NOT_YET_MATURE,
                reason_detail="deferred",
            )

    def test_quality_rejects_remaining_non_negotiable_invalid_states(self) -> None:
        with self.assertRaisesRegex(ValueError, "data-not-available.*eligible"):
            contracts.QualityAssessment(
                contracts.WorkItemOutcome.DATA_NOT_AVAILABLE,
                contracts.MarketIntervalType.REGULAR,
                contracts.CoverageStatus.NO_OBSERVATIONS,
                contracts.AnalysisEligibility.ELIGIBLE,
                contracts.ReasonCode.PROVIDER_SAFETY_LAG,
                "lag",
            )
        with self.assertRaisesRegex(ValueError, "requires eligible_at"):
            contracts.QualityAssessment(
                contracts.WorkItemOutcome.SUCCEEDED,
                contracts.MarketIntervalType.REGULAR,
                contracts.CoverageStatus.PARTIAL,
                contracts.AnalysisEligibility.NOT_YET_MATURE,
                contracts.ReasonCode.NOT_YET_MATURE,
                "future endpoint",
            )
        with self.assertRaisesRegex(ValueError, "eligible_at cannot be in the future"):
            contracts.QualityAssessment(
                contracts.WorkItemOutcome.SUCCEEDED,
                contracts.MarketIntervalType.REGULAR,
                contracts.CoverageStatus.COMPLETE,
                contracts.AnalysisEligibility.ELIGIBLE,
                None,
                None,
                eligible_at=datetime(2026, 8, 13, tzinfo=UTC),
                assessed_at=datetime(2026, 8, 12, tzinfo=UTC),
            )

    def test_early_close_is_not_a_market_interval_type(self) -> None:
        self.assertNotIn("EARLY_CLOSE", {item.value for item in contracts.MarketIntervalType})

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
        for definition in contracts.PRICE_REACTION_METRIC_DEFINITIONS.values():
            with self.subTest(metric=definition.metric):
                self.assertEqual(definition.contract_version, contracts.REACTION_METRIC_VERSION)
                self.assertIsInstance(definition.category, contracts.MetricCategory)
                self.assertIsInstance(definition.anchor_marker, contracts.EndpointType)
                self.assertIsInstance(definition.start_endpoint, contracts.EndpointType)
                self.assertIsInstance(definition.end_endpoint, contracts.EndpointType)
                self.assertIsInstance(definition.start_price, contracts.PriceField)
                self.assertIsInstance(definition.end_price, contracts.PriceField)
                self.assertIsInstance(definition.clipping_policy, contracts.ClippingPolicy)
                self.assertGreaterEqual(definition.endpoint_tolerance_seconds, 0)
                self.assertTrue(definition.maturity_rule)
                self.assertTrue(definition.applicability_rule)
        for definition in contracts.ACTIVITY_METRIC_DEFINITIONS.values():
            with self.subTest(metric=definition.metric):
                self.assertEqual(definition.contract_version, contracts.REACTION_METRIC_VERSION)
                self.assertIs(definition.category, contracts.MetricCategory.ACTIVITY)
                self.assertIsInstance(definition.calculation_kind, contracts.CalculationKind)
                self.assertTrue(definition.formula)
                self.assertIsInstance(definition.annualization, contracts.Annualization)
                self.assertFalse(hasattr(definition, "start_price"))

    def test_pre_event_drift_is_context_not_reaction(self) -> None:
        self.assertNotIn("PRE_EVENT_DRIFT_60M", {item.value for item in contracts.ReactionMetric})
        self.assertEqual(
            contracts.ContextMetric.PRE_EVENT_DRIFT_60M.value,
            "PRE_EVENT_DRIFT_60M",
        )


if __name__ == "__main__":
    unittest.main()
