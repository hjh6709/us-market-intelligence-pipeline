"""Stable identities and additive target contracts shared by platform layers."""

from dataclasses import dataclass
from enum import StrEnum

MARKET_SOURCE = "alpaca"
MARKET_FEED = "sip"
ANALYSIS_VERSION = "multi_event_sip_v1"
STRATEGY_NAME = "pre60_momentum_post60"
STRATEGY_VERSION = "v1"

REACTION_METRIC_VERSION = "event_session_reaction_v1"


class CollectionStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class MarketStatus(StrEnum):
    OPEN = "OPEN"
    EARLY_CLOSE = "EARLY_CLOSE"
    CLOSED = "CLOSED"


class CoverageStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    EMPTY = "EMPTY"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AnalysisEligibility(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"
    NOT_EVALUATED = "NOT_EVALUATED"


class ReactionMetric(StrEnum):
    PRE_EVENT_DRIFT_60M = "PRE_EVENT_DRIFT_60M"
    POST_EVENT_RETURN_5M = "POST_EVENT_RETURN_5M"
    POST_EVENT_RETURN_15M = "POST_EVENT_RETURN_15M"
    POST_EVENT_RETURN_30M = "POST_EVENT_RETURN_30M"
    POST_EVENT_RETURN_60M = "POST_EVENT_RETURN_60M"
    POST_EVENT_RETURN_TO_SESSION_CLOSE = "POST_EVENT_RETURN_TO_SESSION_CLOSE"
    POST_EVENT_RETURN_TO_NEXT_SESSION_CLOSE = (
        "POST_EVENT_RETURN_TO_NEXT_SESSION_CLOSE"
    )


@dataclass(frozen=True)
class QualityAssessment:
    collection_status: CollectionStatus
    market_status: MarketStatus
    coverage_status: CoverageStatus
    analysis_eligibility: AnalysisEligibility
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("quality assessment requires a reason")
        if (
            self.collection_status is CollectionStatus.FAILED
            and self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
        ):
            raise ValueError("failed collection cannot be analysis eligible")
        if (
            self.market_status is MarketStatus.CLOSED
            and self.coverage_status is not CoverageStatus.NOT_APPLICABLE
        ):
            raise ValueError("closed market coverage must be NOT_APPLICABLE")

    @property
    def collection_succeeded(self) -> bool:
        return self.collection_status is CollectionStatus.SUCCEEDED

    @property
    def analysis_allowed(self) -> bool:
        return self.analysis_eligibility is AnalysisEligibility.ELIGIBLE
