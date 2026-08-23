from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Iterable

import numpy as np

from contracts.execution_label_v1 import ExecutionAwareLabel

from .cost_model import CostEstimate, CostModel


@dataclass(frozen=True)
class ExecutionCostObservation:
    label: ExecutionAwareLabel
    order_type: str
    notional_bucket: str
    spread_bucket: str
    depth_bucket: str
    session: str
    regime: str
    fee_tier: str


class ReceiptCalibratedCostModel:
    """Versioned empirical cost estimates; sparse segments fall back conservatively."""

    def __init__(
        self,
        observations: Iterable[ExecutionCostObservation],
        *,
        minimum_segment_samples: int = 30,
        fallback: CostModel | None = None,
    ):
        self.observations = tuple(observations)
        self.minimum_segment_samples = max(10, int(minimum_segment_samples))
        self.fallback = fallback or CostModel()
        payload = [
            {
                **asdict(item),
                "label": item.label.model_dump(mode="json"),
            }
            for item in self.observations
        ]
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        self.model_version = f"receipt-cost-v1-{hashlib.sha256(canonical.encode()).hexdigest()[:16]}"

    def estimate(
        self,
        gross_edge_bps: float,
        *,
        symbol: str,
        side: str,
        order_type: str,
        notional_bucket: str,
        spread_bucket: str,
        depth_bucket: str,
        session: str,
        regime: str,
        fee_tier: str,
        expected_mae_bps: float | None = None,
    ) -> CostEstimate:
        segment = [
            item
            for item in self.observations
            if item.label.symbol == symbol.upper()
            and item.label.side == side.upper()
            and item.order_type.upper() == order_type.upper()
            and item.notional_bucket == notional_bucket
            and item.spread_bucket == spread_bucket
            and item.depth_bucket == depth_bucket
            and item.session == session
            and item.regime == regime
            and item.fee_tier == fee_tier
        ]
        if len(segment) < self.minimum_segment_samples:
            return self.fallback.estimate(
                gross_edge_bps,
                order_type=order_type,
                expected_mae_bps=expected_mae_bps,
            )
        fees = np.array([item.label.fee_bps for item in segment], dtype=float)
        slippage = np.array([item.label.slippage_bps for item in segment], dtype=float)
        funding = np.array([item.label.funding_bps for item in segment], dtype=float)
        return CostEstimate(
            expected_return_bps=float(gross_edge_bps),
            fee_bps=float(np.quantile(fees, 0.75)),
            slippage_bps=float(np.quantile(slippage, 0.75)),
            funding_bps=float(np.quantile(funding, 0.75)),
            model_error_buffer_bps=max(
                self.fallback.minimum_model_error_bps,
                float(expected_mae_bps or 0) * 0.15,
            ),
        )
