"""Stable legacy identities and corrected target foundation contracts."""

from dataclasses import dataclass
from datetime import datetime, timedelta
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
        (ReactionMetric.POST_1M, _definition(ReactionMetric.POST_1M, "ANNOUNCEMENT", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "MARKER_MINUTE_BAR_CLOSE")),
        (ReactionMetric.POST_5M, _definition(ReactionMetric.POST_5M, "ANNOUNCEMENT", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "MARKER_PLUS_4M_BAR_CLOSE")),
        (ReactionMetric.POST_15M, _definition(ReactionMetric.POST_15M, "ANNOUNCEMENT", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "MARKER_PLUS_14M_BAR_CLOSE")),
        (ReactionMetric.POST_30M, _definition(ReactionMetric.POST_30M, "ANNOUNCEMENT", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "MARKER_PLUS_29M_BAR_CLOSE")),
        (ReactionMetric.POST_60M, _definition(ReactionMetric.POST_60M, "ANNOUNCEMENT", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "MARKER_PLUS_59M_BAR_CLOSE")),
        (ReactionMetric.RELEASE_TO_OPEN, _definition(ReactionMetric.RELEASE_TO_OPEN, "SESSION_OPEN", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "LAST_VALID_PRE_OPEN_REFERENCE", applicability="PRE_MARKET_WITH_VALID_PRE_OPEN_REFERENCE")),
        (ReactionMetric.OPEN_GAP, _definition(ReactionMetric.OPEN_GAP, "SESSION_OPEN", "S0_OPEN", "LAST_VALID_PRE_OPEN_REFERENCE", "FIRST_S0_REGULAR_BAR", end_price="OPEN", applicability="VALID_PRE_OPEN_REFERENCE_REQUIRED")),
        (ReactionMetric.OPEN_30M, _definition(ReactionMetric.OPEN_30M, "SESSION_OPEN", "S0_OPEN", "FIRST_S0_REGULAR_BAR_OPEN", "S0_OPEN_PLUS_29M_BAR_CLOSE", start_price="OPEN")),
        (ReactionMetric.OPEN_60M, _definition(ReactionMetric.OPEN_60M, "SESSION_OPEN", "S0_OPEN", "FIRST_S0_REGULAR_BAR_OPEN", "S0_OPEN_PLUS_59M_BAR_CLOSE", start_price="OPEN")),
        (ReactionMetric.EVENT_TO_CLOSE, _definition(ReactionMetric.EVENT_TO_CLOSE, "SESSION", "SELECTED_MARKER", "MARKER_MINUS_1M_BAR_CLOSE", "S0_REGULAR_CLOSE", applicability="PRE_MARKET_OR_REGULAR_SESSION_ONLY")),
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
class ReactionIdentity:
    economic_event_marker_id: str
    symbol: str
    reaction_metric: ReactionMetric
    metric_contract_version: str = REACTION_METRIC_VERSION

    def __post_init__(self) -> None:
        if not self.economic_event_marker_id.strip() or not self.symbol.strip():
            raise ValueError("reaction identity requires marker and symbol")
        if self.metric_contract_version != REACTION_METRIC_VERSION:
            raise ValueError("reaction identity requires the canonical metric contract")


@dataclass(frozen=True)
class ReactionWindow:
    start_at: datetime | None
    end_at: datetime | None
    start_price: str | None
    end_price: str | None
    analysis_eligibility: AnalysisEligibility


def resolve_reaction_window(
    metric: ReactionMetric,
    *,
    marker_at: datetime,
    release_phase: str,
    s0_open: datetime,
    s0_close: datetime,
    pre_open_reference_at: datetime | None = None,
) -> ReactionWindow:
    """Resolve exact one-minute endpoint semantics without reading market data."""
    instants = (marker_at, s0_open, s0_close)
    if any(item.tzinfo is None or item.utcoffset() is None for item in instants):
        raise ValueError("reaction window instants must be timezone-aware")
    if s0_open >= s0_close:
        raise ValueError("S0 open must precede S0 close")
    if pre_open_reference_at is not None and (
        pre_open_reference_at.tzinfo is None
        or pre_open_reference_at.utcoffset() is None
    ):
        raise ValueError("pre-open reference must be timezone-aware")

    post_minutes = {
        ReactionMetric.POST_1M: 0,
        ReactionMetric.POST_5M: 4,
        ReactionMetric.POST_15M: 14,
        ReactionMetric.POST_30M: 29,
        ReactionMetric.POST_60M: 59,
    }
    if metric in post_minutes:
        return ReactionWindow(
            marker_at - timedelta(minutes=1),
            marker_at + timedelta(minutes=post_minutes[metric]),
            "CLOSE",
            "CLOSE",
            AnalysisEligibility.ELIGIBLE,
        )
    if metric is ReactionMetric.EVENT_TO_CLOSE:
        if release_phase in {"POST_MARKET", "MARKET_CLOSED"}:
            return ReactionWindow(
                None, None, None, None, AnalysisEligibility.NOT_APPLICABLE
            )
        return ReactionWindow(
            marker_at - timedelta(minutes=1),
            s0_close,
            "CLOSE",
            "CLOSE",
            AnalysisEligibility.ELIGIBLE,
        )
    if metric in {ReactionMetric.RELEASE_TO_OPEN, ReactionMetric.OPEN_GAP}:
        if release_phase != "PRE_MARKET" or pre_open_reference_at is None:
            return ReactionWindow(
                None, None, None, None, AnalysisEligibility.NOT_APPLICABLE
            )
        if not marker_at <= pre_open_reference_at < s0_open:
            raise ValueError("pre-open reference must be between marker and S0 open")
        if metric is ReactionMetric.RELEASE_TO_OPEN:
            return ReactionWindow(
                marker_at - timedelta(minutes=1),
                pre_open_reference_at,
                "CLOSE",
                "CLOSE",
                AnalysisEligibility.ELIGIBLE,
            )
        return ReactionWindow(
            pre_open_reference_at,
            s0_open,
            "CLOSE",
            "OPEN",
            AnalysisEligibility.ELIGIBLE,
        )
    if metric in {ReactionMetric.OPEN_30M, ReactionMetric.OPEN_60M}:
        offset = 29 if metric is ReactionMetric.OPEN_30M else 59
        end_at = s0_open + timedelta(minutes=offset)
        if end_at > s0_close:
            return ReactionWindow(
                None, None, None, None, AnalysisEligibility.NOT_APPLICABLE
            )
        return ReactionWindow(
            s0_open,
            end_at,
            "OPEN",
            "CLOSE",
            AnalysisEligibility.ELIGIBLE,
        )
    raise ValueError(f"exact instant resolution is not defined for {metric.value}")


@dataclass(frozen=True)
class QualityAssessment:
    work_item_outcome: WorkItemOutcome
    market_interval_type: MarketIntervalType
    coverage_status: CoverageStatus
    analysis_eligibility: AnalysisEligibility
    reason_code: ReasonCode | None = None
    reason_detail: str | None = None
    eligible_at: datetime | None = None
    assessed_at: datetime | None = None

    def __post_init__(self) -> None:
        if (self.reason_code is None) != (self.reason_detail is None):
            raise ValueError("reason_code and reason_detail must be supplied together")
        if self.reason_detail is not None and not self.reason_detail.strip():
            raise ValueError("quality assessment reason_detail must not be empty")
        if (
            self.work_item_outcome is WorkItemOutcome.FAILED
            and self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
        ):
            raise ValueError("failed work item cannot be analysis eligible")
        if (
            self.work_item_outcome is WorkItemOutcome.DATA_NOT_AVAILABLE
            and self.coverage_status is CoverageStatus.COMPLETE
        ):
            raise ValueError("data-not-available work item cannot have complete coverage")
        if (
            self.work_item_outcome is WorkItemOutcome.DATA_NOT_AVAILABLE
            and self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
        ):
            raise ValueError("data-not-available work item cannot be eligible")
        if (
            self.work_item_outcome is WorkItemOutcome.SKIPPED
            and self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
        ):
            raise ValueError("skipped work item cannot be eligible")
        if (
            self.analysis_eligibility is AnalysisEligibility.NOT_YET_MATURE
            and self.eligible_at is None
        ):
            raise ValueError("not-yet-mature analysis requires eligible_at")
        if self.eligible_at is not None and (
            self.eligible_at.tzinfo is None or self.eligible_at.utcoffset() is None
        ):
            raise ValueError("eligible_at must be timezone-aware")
        if self.assessed_at is not None and (
            self.assessed_at.tzinfo is None or self.assessed_at.utcoffset() is None
        ):
            raise ValueError("assessed_at must be timezone-aware")
        if (
            self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
            and self.eligible_at is not None
        ):
            if self.assessed_at is None:
                raise ValueError("eligible assessment with eligible_at requires assessed_at")
            if self.eligible_at > self.assessed_at:
                raise ValueError("eligible_at cannot be in the future for eligible analysis")

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
