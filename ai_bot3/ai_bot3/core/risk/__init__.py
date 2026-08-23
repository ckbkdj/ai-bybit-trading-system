"""Capital-preservation policy for research and ticket issuance."""

from .capital_preservation import (
    CapitalPreservationConfig,
    CapitalState,
    RiskDecision,
    TradeProposal,
    evaluate_trade_proposal,
)

__all__ = [
    "CapitalPreservationConfig",
    "CapitalState",
    "RiskDecision",
    "TradeProposal",
    "evaluate_trade_proposal",
]
