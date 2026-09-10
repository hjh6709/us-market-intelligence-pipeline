"""Stable legacy identities and corrected target foundation contracts."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

MARKET_SOURCE = "alpaca"
MARKET_FEED = "sip"
ANALYSIS_VERSION = "multi_event_sip_v1"
STRATEGY_NAME = "pre60_momentum_post60"
STRATEGY_VERSION = "v1"

# Legacy PRE_60M/POST_* rows continue to use ANALYSIS_VERSION. They are not
# compatibility-renamed into this target metric contract.
REACTION_METRIC_VERSION = "event_session_reaction_v2"


class PipelineRunOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    NO_WORK = "NO_WORK"


class WorkItemOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DATA_NOT_AVAILABLE = "DATA_NOT_AVAILABLE"
    SKIPPED = "SKIPPED"


class SessionDayType(StrEnum):
    REGULAR = "REGULAR"
    EARLY_CLOSE = "EARLY_CLOSE"
    CLOSED = "CLOSED"


class MarketIntervalType(StrEnum):
    REGULAR = "REGULAR"
    EXTENDED = "EXTENDED"
    EARLY_CLOSE = "EARLY_CLOSE"
    CLOSED = "CLOSED"


class CoverageStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    SPARSE_EXPECTED = "SPARSE_EXPECTED"
    NO_OBSERVATIONS = "NO_OBSERVATIONS"


class AnalysisEligibility(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    NOT_YET_MATURE = "NOT_YET_MATURE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReasonCode(StrEnum):
    PROVIDER_TIMEOUT = "PROVIDER_TIMEOUT"
    PROVIDER_SAFETY_LAG = "PROVIDER_SAFETY_LAG"
    MARKET_CLOSED = "MARKET_CLOSED"
    SOURCE_NOT_PUBLISHED = "SOURCE_NOT_PUBLISHED"
    PAGINATION_INCOMPLETE = "PAGINATION_INCOMPLETE"
    INSUFFICIENT_BARS = "INSUFFICIENT_BARS"
    SPARSE_EXTENDED_HOURS = "SPARSE_EXTENDED_HOURS"
    STALE_REFERENCE = "STALE_REFERENCE"
    NOT_YET_MATURE = "NOT_YET_MATURE"
    INVALID_SCHEMA = "INVALID_SCHEMA"
    CONFLICTING_SOURCE_FACT = "CONFLICTING_SOURCE_FACT"


class ReactionMetric(StrEnum):
    POST_1M = "POST_1M"
    POST_5M = "POST_5M"
    POST_15M = "POST_15M"
    POST_30M = "POST_30M"
    POST_60M = "POST_60M"
    RELEASE_TO_OPEN = "RELEASE_TO_OPEN"
    OPEN_GAP = "OPEN_GAP"
    OPEN_30M = "OPEN_30M"
    OPEN_60M = "OPEN_60M"
    EVENT_TO_CLOSE = "EVENT_TO_CLOSE"
    SESSION_RETURN_S0 = "SESSION_RETURN_S0"
    S0_CLOSE_TO_S_PLUS_1_CLOSE = "S0_CLOSE_TO_S+1_CLOSE"
    S0_CLOSE_TO_S_PLUS_3_CLOSE = "S0_CLOSE_TO_S+3_CLOSE"
    S0_CLOSE_TO_S_PLUS_7_CLOSE = "S0_CLOSE_TO_S+7_CLOSE"
    EVENT_VOLUME_RATIO = "EVENT_VOLUME_RATIO"
    OPEN_60M_VOLUME_RATIO = "OPEN_60M_VOLUME_RATIO"
    EVENT_REALIZED_VOL = "EVENT_REALIZED_VOL"
    EVENT_VOLATILITY_RATIO = "EVENT_VOLATILITY_RATIO"


class ContextMetric(StrEnum):
    PRE_EVENT_DRIFT_60M = "PRE_EVENT_DRIFT_60M"


@dataclass(frozen=True)
class ReactionMetricDefinition:
    metric: ReactionMetric
    category: str
    anchor_marker: str
    start_endpoint: str
    end_endpoint: str
    start_price: str
    end_price: str
    session_clipping: str
    endpoint_tolerance_seconds: int
    maturity_rule: str
    applicability_rule: str
    contract_version: str = REACTION_METRIC_VERSION


def _definition(
    metric: ReactionMetric,
    category: str,
    anchor: str,
    start: str,
    end: str,
    start_price: str = "CLOSE",
    end_price: str = "CLOSE",
    clipping: str = "NO_CROSS_SESSION_FILL",
    tolerance: int = 60,
    maturity: str = "END_ENDPOINT_AVAILABLE",
    applicability: str = "REQUIRES_BOTH_ENDPOINTS",
) -> ReactionMetricDefinition:
    return ReactionMetricDefinition(
        metric=metric,
        category=category,
        anchor_marker=anchor,
        start_endpoint=start,
        end_endpoint=end,
        start_price=start_price,
        end_price=end_price,
        session_clipping=clipping,
        endpoint_tolerance_seconds=tolerance,
        maturity_rule=maturity,
        applicability_rule=applicability,
    )


REACTION_METRIC_DEFINITIONS = {
    metric: definition
    for metric, definition in (
        (ReactionMetric.POST_1M, _definition(ReactionMetric.POST_1M, "ANNOUNCEMENT", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "MARKER_PLUS_1M_BAR_END")),
        (ReactionMetric.POST_5M, _definition(ReactionMetric.POST_5M, "ANNOUNCEMENT", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "MARKER_PLUS_5M_BAR_END")),
        (ReactionMetric.POST_15M, _definition(ReactionMetric.POST_15M, "ANNOUNCEMENT", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "MARKER_PLUS_15M_BAR_END")),
        (ReactionMetric.POST_30M, _definition(ReactionMetric.POST_30M, "ANNOUNCEMENT", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "MARKER_PLUS_30M_BAR_END")),
        (ReactionMetric.POST_60M, _definition(ReactionMetric.POST_60M, "ANNOUNCEMENT", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "MARKER_PLUS_60M_BAR_END")),
        (ReactionMetric.RELEASE_TO_OPEN, _definition(ReactionMetric.RELEASE_TO_OPEN, "SESSION_OPEN", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "S0_REGULAR_OPEN", end_price="OPEN", applicability="PRIMARY_MARKER_BEFORE_S0_OPEN")),
        (ReactionMetric.OPEN_GAP, _definition(ReactionMetric.OPEN_GAP, "SESSION_OPEN", "S0_OPEN", "LAST_VALID_PRE_OPEN_REFERENCE", "FIRST_S0_REGULAR_BAR", end_price="OPEN", applicability="VALID_PRE_OPEN_REFERENCE_REQUIRED")),
        (ReactionMetric.OPEN_30M, _definition(ReactionMetric.OPEN_30M, "SESSION_OPEN", "S0_OPEN", "FIRST_S0_REGULAR_BAR", "S0_OPEN_PLUS_30M_BAR_END", start_price="OPEN")),
        (ReactionMetric.OPEN_60M, _definition(ReactionMetric.OPEN_60M, "SESSION_OPEN", "S0_OPEN", "FIRST_S0_REGULAR_BAR", "S0_OPEN_PLUS_60M_BAR_END", start_price="OPEN")),
        (ReactionMetric.EVENT_TO_CLOSE, _definition(ReactionMetric.EVENT_TO_CLOSE, "SESSION", "PRIMARY", "LAST_COMPLETED_BAR_BEFORE_MARKER", "S0_REGULAR_CLOSE", applicability="PRIMARY_MARKER_NOT_AFTER_S0_CLOSE")),
        (ReactionMetric.SESSION_RETURN_S0, _definition(ReactionMetric.SESSION_RETURN_S0, "SESSION", "S0_OPEN", "S0_REGULAR_OPEN", "S0_REGULAR_CLOSE", start_price="OPEN")),
        (ReactionMetric.S0_CLOSE_TO_S_PLUS_1_CLOSE, _definition(ReactionMetric.S0_CLOSE_TO_S_PLUS_1_CLOSE, "PERSISTENCE", "S0_CLOSE", "S0_REGULAR_CLOSE", "S_PLUS_1_REGULAR_CLOSE", maturity="S_PLUS_1_CLOSE_AVAILABLE")),
        (ReactionMetric.S0_CLOSE_TO_S_PLUS_3_CLOSE, _definition(ReactionMetric.S0_CLOSE_TO_S_PLUS_3_CLOSE, "PERSISTENCE", "S0_CLOSE", "S0_REGULAR_CLOSE", "S_PLUS_3_REGULAR_CLOSE", maturity="S_PLUS_3_CLOSE_AVAILABLE")),
        (ReactionMetric.S0_CLOSE_TO_S_PLUS_7_CLOSE, _definition(ReactionMetric.S0_CLOSE_TO_S_PLUS_7_CLOSE, "PERSISTENCE", "S0_CLOSE", "S0_REGULAR_CLOSE", "S_PLUS_7_REGULAR_CLOSE", maturity="S_PLUS_7_CLOSE_AVAILABLE")),
        (ReactionMetric.EVENT_VOLUME_RATIO, _definition(ReactionMetric.EVENT_VOLUME_RATIO, "ACTIVITY", "PRIMARY", "EVENT_WINDOW_VOLUME", "REFERENCE_WINDOW_VOLUME", start_price="VOLUME", end_price="VOLUME", applicability="REFERENCE_WINDOW_REQUIRED")),
        (ReactionMetric.OPEN_60M_VOLUME_RATIO, _definition(ReactionMetric.OPEN_60M_VOLUME_RATIO, "ACTIVITY", "S0_OPEN", "S0_OPEN_60M_VOLUME", "REFERENCE_OPEN_60M_VOLUME", start_price="VOLUME", end_price="VOLUME", applicability="REFERENCE_WINDOW_REQUIRED")),
        (ReactionMetric.EVENT_REALIZED_VOL, _definition(ReactionMetric.EVENT_REALIZED_VOL, "ACTIVITY", "PRIMARY", "EVENT_WINDOW_RETURNS", "EVENT_WINDOW_RETURNS", start_price="RETURNS", end_price="REALIZED_VOL", applicability="MINIMUM_RETURN_COUNT_REQUIRED")),
        (ReactionMetric.EVENT_VOLATILITY_RATIO, _definition(ReactionMetric.EVENT_VOLATILITY_RATIO, "ACTIVITY", "PRIMARY", "EVENT_REALIZED_VOL", "REFERENCE_REALIZED_VOL", start_price="REALIZED_VOL", end_price="REALIZED_VOL", applicability="REFERENCE_WINDOW_REQUIRED")),
    )
}


@dataclass(frozen=True)
class QualityAssessment:
    work_item_outcome: WorkItemOutcome
    market_interval_type: MarketIntervalType
    coverage_status: CoverageStatus
    analysis_eligibility: AnalysisEligibility
    reason_code: ReasonCode
    reason_detail: str
    eligible_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.reason_detail.strip():
            raise ValueError("quality assessment requires reason_detail")
        if (
            self.work_item_outcome is WorkItemOutcome.FAILED
            and self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
        ):
            raise ValueError("failed work item cannot be analysis eligible")
        if (
            self.analysis_eligibility is AnalysisEligibility.NOT_YET_MATURE
            and self.eligible_at is None
        ):
            raise ValueError("not-yet-mature analysis requires eligible_at")
        if self.eligible_at is not None and (
            self.eligible_at.tzinfo is None or self.eligible_at.utcoffset() is None
        ):
            raise ValueError("eligible_at must be timezone-aware")

    @property
    def work_item_succeeded(self) -> bool:
        return self.work_item_outcome is WorkItemOutcome.SUCCEEDED

    @property
    def analysis_allowed(self) -> bool:
        return self.analysis_eligibility is AnalysisEligibility.ELIGIBLE


def metric_maturity(
    eligible_at: datetime,
    *,
    as_of: datetime,
) -> AnalysisEligibility:
    if eligible_at.tzinfo is None or eligible_at.utcoffset() is None:
        raise ValueError("eligible_at must be timezone-aware")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return (
        AnalysisEligibility.ELIGIBLE
        if as_of >= eligible_at
        else AnalysisEligibility.NOT_YET_MATURE
    )
