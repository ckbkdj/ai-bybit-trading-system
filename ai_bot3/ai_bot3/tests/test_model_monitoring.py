from __future__ import annotations

import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.model_monitoring import (
    factor_group_scores,
    population_stability_index,
    predictive_health_metrics,
    quantile_wasserstein_distance,
    scaled_feature_ood_score,
    source_is_reliable,
)


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


def test_distribution_and_predictive_health_are_separate_metrics():
    reference = np.linspace(-1, 1, 1000)
    shifted = reference + 0.5
    assert population_stability_index(reference, shifted) > 0
    assert quantile_wasserstein_distance(reference, shifted) > 0.49
    metrics = predictive_health_metrics(
        [[0.8, 0.2], [0.3, 0.7], [0.6, 0.4]],
        [0, 1, 1],
        residuals=[0.1, -0.2, 0.05],
        conformal_contains=[True, True, False],
        regimes=["risk_on", "risk_on", "risk_off"],
    )
    assert 0 <= metrics["expected_calibration_error"] <= 1
    assert metrics["brier_score"] > 0
    assert metrics["conformal_coverage"] == 2 / 3
    assert metrics["regime_sample_coverage"]["risk_on"] == 2
