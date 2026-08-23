from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Mapping, Sequence


@dataclass(frozen=True)
class AblationResult:
    factor_group: str
    metric: str
    baseline_mean: float
    augmented_mean: float
    mean_improvement: float
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
) -> tuple[AblationResult, ...]:
    """Compare each group on identical walk-forward folds; never pool in-sample rows."""
    if not baseline_folds:
        raise ValueError("baseline folds are required")
    if not 0 <= minimum_improved_fold_ratio <= 1:
        raise ValueError("minimum_improved_fold_ratio must be in [0, 1]")
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
        output.append(
            AblationResult(
                factor_group=group,
                metric=primary_metric,
                baseline_mean=mean(baseline),
                augmented_mean=mean(augmented),
                mean_improvement=mean_improvement,
                improved_fold_ratio=improved_ratio,
                worst_fold_improvement=min(improvements),
                retained=(
                    mean_improvement >= minimum_mean_improvement
                    and improved_ratio >= minimum_improved_fold_ratio
                    and min(improvements) >= minimum_worst_fold_improvement
                ),
            )
        )
    return tuple(output)
