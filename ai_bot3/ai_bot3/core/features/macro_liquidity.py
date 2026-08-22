from __future__ import annotations

from typing import Mapping

from .state_graph import MarketStateScore, StateInput, aggregate_state


def usd_liquidity_state(
    normalized_inputs: Mapping[str, StateInput], *, expected_factor_count: int = 6
) -> MarketStateScore:
    """Combine Fed balance sheet, RRP, TGA, real yield, DXY and credit inputs with trained signs."""
    return aggregate_state("usd_liquidity_score", normalized_inputs, expected_factor_count)
