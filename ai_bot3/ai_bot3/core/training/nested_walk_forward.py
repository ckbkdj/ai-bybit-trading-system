from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from core.models.two_stage import (
    TwoStageAlphaModel,
    TwoStageConfig,
    TwoStagePrediction,
    prediction_gate_diagnostics,
)


@dataclass(frozen=True)
class NestedModelSelection:
    model: TwoStageAlphaModel
    selected_config: TwoStageConfig
    candidate_results: tuple[Mapping[str, object], ...]
    audit: Mapping[str, object]


def _config_id(config: TwoStageConfig) -> str:
    payload = json.dumps(asdict(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _selected_rows(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
) -> pd.DataFrame:
    candidates = frame.copy().reset_index(drop=True)
    candidates["_prediction"] = list(predictions)
    selected: list[pd.Series] = []
    group_columns = [column for column in ("symbol", "decision_at") if column in candidates]
    groups = candidates.groupby(group_columns, sort=True) if group_columns else [("all", candidates)]
    for _, group in groups:
        qualified: list[tuple[float, float, pd.Series]] = []
        for _, row in group.iterrows():
            prediction = row["_prediction"]
            side = str(row.get("side", "BUY")).upper()
            direction_ok = (
                side == "BUY" and prediction.p_up >= prediction.p_down
            ) or (
                side == "SELL" and prediction.p_down >= prediction.p_up
            )
            if (
                prediction.decision == "TRADE"
                and prediction.lower_bound_net_edge > 0
                and direction_ok
            ):
                qualified.append(
                    (
                        prediction.meta_trade_probability,
                        prediction.lower_bound_net_edge,
                        row,
                    )
                )
        if qualified:
            selected.append(max(qualified, key=lambda item: (item[0], item[1]))[2])
    return pd.DataFrame(selected)


class NestedWalkForwardSelector:
    """Tune on inner walk-forward OOS only, then fit once for an outer OOS fold."""

    def __init__(
        self,
        candidate_configs: Sequence[TwoStageConfig],
        *,
        inner_folds: int = 3,
        first_validation_fraction: float = 0.55,
    ) -> None:
        if not candidate_configs:
            raise ValueError("at least one candidate configuration is required")
        if inner_folds < 2 or not 0.4 <= first_validation_fraction <= 0.8:
            raise ValueError("invalid nested walk-forward configuration")
        self.candidate_configs = tuple(candidate_configs)
        self.inner_folds = inner_folds
        self.first_validation_fraction = first_validation_fraction

    def _folds(self, frame: pd.DataFrame) -> tuple[tuple[np.ndarray, np.ndarray], ...]:
        if "decision_at" not in frame or "label_available_at" not in frame:
            raise ValueError("nested walk-forward requires PIT decision and label timestamps")
        decision = pd.to_datetime(frame["decision_at"], utc=True, errors="coerce")
        label_available = pd.to_datetime(frame["label_available_at"], utc=True, errors="coerce")
        if decision.isna().any() or label_available.isna().any():
            raise ValueError("nested walk-forward timestamps are invalid")
        unique_times = decision.drop_duplicates().sort_values().reset_index(drop=True)
        first = int(math.floor(len(unique_times) * self.first_validation_fraction))
        chunks = np.array_split(unique_times.iloc[first:].to_numpy(), self.inner_folds)
        folds: list[tuple[np.ndarray, np.ndarray]] = []
        for chunk in chunks:
            if len(chunk) == 0:
                continue
            start = pd.Timestamp(chunk[0])
            end = pd.Timestamp(chunk[-1])
            if start.tzinfo is None:
                start = start.tz_localize("UTC")
                end = end.tz_localize("UTC")
            train = np.flatnonzero(((decision < start) & (label_available < start)).to_numpy())
            validation = np.flatnonzero(((decision >= start) & (decision <= end)).to_numpy())
            if len(train) >= 50 and len(validation) >= 10:
                folds.append((train, validation))
        if len(folds) < 2:
            raise ValueError("insufficient rows for nested walk-forward selection")
        return tuple(folds)

    def select_and_fit(
        self,
        outer_train: pd.DataFrame,
        feature_columns: Sequence[str],
    ) -> NestedModelSelection:
        data = outer_train.copy().reset_index(drop=True)
        folds = self._folds(data)
        candidate_results: list[dict[str, object]] = []
        for config in self.candidate_configs:
            fold_results: list[dict[str, object]] = []
            for fold_number, (train_positions, validation_positions) in enumerate(folds, start=1):
                train = data.iloc[train_positions]
                validation = data.iloc[validation_positions]
                model = TwoStageAlphaModel(config).fit(train, feature_columns)
                predictions = model.predict(validation)
                gate_diagnostics = prediction_gate_diagnostics(
                    validation,
                    predictions,
                    meta_threshold=config.meta_trade_probability,
                )
                selected = _selected_rows(validation, predictions)
                if selected.empty:
                    net_utility = -1.0
                    mean_utility = -1.0
                    downside = 1.0
                    trade_count = 0
                else:
                    net = pd.to_numeric(selected["net_return"], errors="raise").to_numpy(float)
                    mae = pd.to_numeric(selected["mae"], errors="raise").to_numpy(float)
                    utility = net - config.tail_penalty * mae
                    net_utility = float(utility.sum())
                    mean_utility = float(utility.mean())
                    downside = float(np.mean(np.minimum(utility, 0.0) ** 2) ** 0.5)
                    trade_count = len(selected)
                fold_results.append(
                    {
                        "fold": fold_number,
                        "train_rows": len(train_positions),
                        "inner_oos_rows": len(validation_positions),
                        "trade_count": trade_count,
                        "prediction_gate": gate_diagnostics,
                        "net_utility": net_utility,
                        "mean_utility": mean_utility,
                        "downside_rms": downside,
                    }
                )
            positive_folds = sum(result["net_utility"] > 0 for result in fold_results)
            score = float(
                np.mean(
                    [result["mean_utility"] - result["downside_rms"] for result in fold_results]
                )
            )
            candidate_results.append(
                {
                    "config_id": _config_id(config),
                    "config": asdict(config),
                    "selection_score": score,
                    "positive_inner_fold_ratio": positive_folds / len(fold_results),
                    "inner_folds": fold_results,
                    "outer_oos_rows_seen": 0,
                }
            )
        selected_result = max(
            candidate_results,
            key=lambda result: (float(result["selection_score"]), str(result["config_id"])),
        )
        selected_config = next(
            config
            for config in self.candidate_configs
            if _config_id(config) == selected_result["config_id"]
        )
        final_model = TwoStageAlphaModel(selected_config).fit(data, feature_columns)
        return NestedModelSelection(
            model=final_model,
            selected_config=selected_config,
            candidate_results=tuple(candidate_results),
            audit={
                "selection_data": "inner_walk_forward_oos_only",
                "outer_oos_used_for_selection": False,
                "inner_fold_count": len(folds),
                "selected_config_id": selected_result["config_id"],
                "outer_train_rows": len(data),
            },
        )


__all__: Sequence[str] = ("NestedModelSelection", "NestedWalkForwardSelector")
