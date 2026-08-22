from __future__ import annotations

from typing import Mapping

from .state_graph import MarketStateScore, StateInput, aggregate_state


def china_growth_liquidity_state(
    normalized_inputs: Mapping[str, StateInput], *, expected_factor_count: int = 8
) -> MarketStateScore:
    return aggregate_state("china_growth_liquidity_score", normalized_inputs, expected_factor_count)
