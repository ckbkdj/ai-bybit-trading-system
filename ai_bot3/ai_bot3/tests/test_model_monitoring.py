from __future__ import annotations

import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.model_monitoring import factor_group_scores, scaled_feature_ood_score, source_is_reliable


class _MinMaxScaler:
    feature_range = (0.0, 1.0)


def test_scaled_ood_is_zero_inside_training_range_and_fail_closed_outside():
    inside = scaled_feature_ood_score(np.array([[0.1, 0.5, 0.9]]), _MinMaxScaler())
    outside = scaled_feature_ood_score(np.array([[0.1, 2.0, 0.9]]), _MinMaxScaler())
    invalid = scaled_feature_ood_score(np.array([[np.nan]]), _MinMaxScaler())
    assert inside.score == 0.0
    assert outside.score > 0.35
    assert invalid.score == 1.0


def test_source_reliability_and_factor_scores_are_explicit_and_bounded():
    assert source_is_reliable("ok", 5)
    assert not source_is_reliable("degraded", 5)
    assert not source_is_reliable("ok", None)
    scores = factor_group_scores(
        {"funding_acceleration": 9, "liquidation_imbalance": -8, "news_sentiment": 0.2},
        factor_bias=3,
        llm_signal=-4,
    )
    assert all(-1.0 <= score <= 1.0 for score in scores.values())

