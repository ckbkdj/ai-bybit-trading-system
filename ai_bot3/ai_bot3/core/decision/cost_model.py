from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostEstimate:
    expected_return_bps: float
    fee_bps: float
    slippage_bps: float
    funding_bps: float
    model_error_buffer_bps: float

    @property
    def after_cost_bps(self) -> float:
        return (
            self.expected_return_bps
            - self.fee_bps
            - self.slippage_bps
            - self.funding_bps
            - self.model_error_buffer_bps
        )


@dataclass(frozen=True)
class CostModel:
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 5.5
    default_slippage_bps: float = 6.0
    default_funding_bps: float = 2.0
    minimum_model_error_bps: float = 10.0

    def estimate(
        self,
        gross_edge_bps: float,
        *,
        order_type: str,
        expected_mae_bps: float | None,
    ) -> CostEstimate:
        fee = self.maker_fee_bps if order_type.upper() == "LIMIT" else self.taker_fee_bps
        error_buffer = max(
            self.minimum_model_error_bps,
            float(expected_mae_bps or 0.0) * 0.15,
        )
        return CostEstimate(
            expected_return_bps=float(gross_edge_bps),
            fee_bps=fee,
            slippage_bps=self.default_slippage_bps,
            funding_bps=self.default_funding_bps,
            model_error_buffer_bps=error_buffer,
        )
