from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import mean
from typing import Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class AblationResult:
    factor_group: str
    metric: str
    baseline_mean: float
    augmented_mean: float
    mean_improvement: float
    bootstrap_lower_mean_improvement: float
    bootstrap_confidence: float
    bootstrap_samples: int
    improved_fold_ratio: float
    worst_fold_improvement: float
    retained: bool


def compare_factor_groups(
    baseline_folds: Sequence[Mapping[str, float]],
    augmented_folds: Mapping[str, Sequence[Mapping[str, float]]],
    *,
    primary_metric: str,
    higher_is_better: bool,
    minimum_mean_improvement: float = 0.0,
    minimum_improved_fold_ratio: float = 0.6,
    minimum_worst_fold_improvement: float = -0.002,
    bootstrap_confidence: float = 0.95,
    bootstrap_samples: int = 5000,
    bootstrap_seed: int = 20260823,
) -> tuple[AblationResult, ...]:
    """Compare paired OOS folds and retain only statistically stable improvements."""
    if not baseline_folds:
        raise ValueError("baseline folds are required")
    if not 0 <= minimum_improved_fold_ratio <= 1:
        raise ValueError("minimum_improved_fold_ratio must be in [0, 1]")
    if not 0.90 <= bootstrap_confidence < 1.0:
        raise ValueError("bootstrap confidence must be in [0.90, 1.0)")
    if bootstrap_samples < 2000:
        raise ValueError("bootstrap samples cannot be below 2000")
    try:
        baseline = [float(fold[primary_metric]) for fold in baseline_folds]
    except KeyError as exc:
        raise ValueError(f"baseline is missing primary metric: {primary_metric}") from exc
    output = []
    direction = 1.0 if higher_is_better else -1.0
    for group, folds in augmented_folds.items():
        if len(folds) != len(baseline):
            raise ValueError(f"factor group {group} does not use the same folds as baseline")
        try:
            augmented = [float(fold[primary_metric]) for fold in folds]
        except KeyError as exc:
            raise ValueError(f"factor group {group} is missing {primary_metric}") from exc
        improvements = [direction * (candidate - base) for base, candidate in zip(baseline, augmented)]
        mean_improvement = mean(improvements)
        improved_ratio = sum(value > 0 for value in improvements) / len(improvements)
        values = np.asarray(improvements, dtype=float)
        block_length = max(1, int(math.ceil(math.sqrt(len(values)))))
        block_count = int(math.ceil(len(values) / block_length))
        group_seed = bootstrap_seed + sum(
            (position + 1) * ord(character)
            for position, character in enumerate(str(group))
        )
        rng = np.random.default_rng(group_seed)
        sampled_means = np.empty(bootstrap_samples, dtype=float)
        offsets = np.arange(block_length)
        for sample in range(bootstrap_samples):
            starts = rng.integers(0, len(values), size=block_count)
            positions = ((starts[:, None] + offsets) % len(values)).reshape(-1)
            sampled_means[sample] = float(values[positions[: len(values)]].mean())
        bootstrap_lower = float(
            np.quantile(sampled_means, 1.0 - bootstrap_confidence, method="lower")
        )
        output.append(
            AblationResult(
                factor_group=group,
                metric=primary_metric,
                baseline_mean=mean(baseline),
                augmented_mean=mean(augmented),
                mean_improvement=mean_improvement,
                bootstrap_lower_mean_improvement=bootstrap_lower,
                bootstrap_confidence=bootstrap_confidence,
                bootstrap_samples=bootstrap_samples,
                improved_fold_ratio=improved_ratio,
                worst_fold_improvement=min(improvements),
                retained=(
                    mean_improvement >= minimum_mean_improvement
                    and bootstrap_lower > minimum_mean_improvement
                    and improved_ratio >= minimum_improved_fold_ratio
                    and min(improvements) >= minimum_worst_fold_improvement
                ),
            )
        )
    return tuple(output)
