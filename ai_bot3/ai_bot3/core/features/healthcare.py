from __future__ import annotations

from typing import Mapping

from .state_graph import MarketStateScore, StateInput, aggregate_state


def healthcare_defensive_state(
    normalized_inputs: Mapping[str, StateInput], *, expected_factor_count: int = 5
) -> MarketStateScore:
    return aggregate_state("healthcare_defensive_score", normalized_inputs, expected_factor_count)
