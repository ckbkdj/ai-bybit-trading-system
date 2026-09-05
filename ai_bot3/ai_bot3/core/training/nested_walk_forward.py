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
    _purge_embargo_seconds,
    prediction_gate_diagnostics,
)


@dataclass(frozen=True)
class NestedModelSelection:
    model: TwoStageAlphaModel
    selected_config: TwoStageConfig
    oof_score_threshold: float | None
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
    if not selected:
        return pd.DataFrame()

    # Inner-OOS model selection must score executable signal paths, not every
    # overlapping label as if capital could open a new same-symbol position at
    # each decision.  The label availability timestamp is a conservative exit
    # boundary when the panel does not carry an explicit exit timestamp.
    candidates = pd.DataFrame(selected)
    required = {"decision_at", "label_available_at"}
    if missing := sorted(required.difference(candidates.columns)):
        raise ValueError(f"nested selection is missing holding timestamps: {missing}")
    candidates["_decision_at"] = pd.to_datetime(
        candidates["decision_at"], utc=True, errors="coerce"
    )
    candidates["_holding_end"] = pd.to_datetime(
        candidates.get("exit_at", candidates["label_available_at"]),
        utc=True,
        errors="coerce",
    )
    if candidates[["_decision_at", "_holding_end"]].isna().any().any():
        raise ValueError("nested selection holding timestamps are invalid")
    if (candidates["_holding_end"] <= candidates["_decision_at"]).any():
        raise ValueError("nested selection holding interval must be positive")

    accepted: list[pd.Series] = []
    symbol_groups = (
        candidates.groupby("symbol", sort=True)
        if "symbol" in candidates
        else [("__portfolio__", candidates)]
    )
    for _, symbol_rows in symbol_groups:
        active_until: pd.Timestamp | None = None
        for _, row in symbol_rows.sort_values("_decision_at").iterrows():
            if active_until is not None and row["_decision_at"] < active_until:
                continue
            accepted.append(row)
            active_until = row["_holding_end"]
    return pd.DataFrame(accepted).drop(
        columns=["_decision_at", "_holding_end", "_prediction"], errors="ignore"
    )


def _direction_consistent_scores(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
    *,
    tail_penalty: float,
) -> list[float]:
    """Return one causal ranking score per symbol/decision opportunity."""

    if len(frame) != len(predictions):
        raise ValueError("prediction and score calibration frame lengths differ")
    candidates = frame.copy().reset_index(drop=True)
    candidates["_prediction"] = list(predictions)
    group_columns = [column for column in ("symbol", "decision_at") if column in candidates]
    groups = candidates.groupby(group_columns, sort=True) if group_columns else [("all", candidates)]
    scores: list[float] = []
    for _, group in groups:
        paired: list[float] = []
        for _, row in group.iterrows():
            prediction = row["_prediction"]
            side = str(row.get("side", "BUY")).upper()
            direction_ok = (
                side == "BUY" and prediction.p_up >= prediction.p_down
            ) or (
                side == "SELL" and prediction.p_down >= prediction.p_up
            )
            if direction_ok:
                paired.append(
                    float(
                        prediction.expected_net_return
                        - tail_penalty * prediction.expected_mae
                    )
                )
        if paired:
            scores.append(max(paired))
    return scores


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

    def _folds(
        self, frame: pd.DataFrame
    ) -> tuple[tuple[tuple[np.ndarray, np.ndarray], ...], int, int]:
        if "decision_at" not in frame or "label_available_at" not in frame:
            raise ValueError("nested walk-forward requires PIT decision and label timestamps")
        decision = pd.to_datetime(frame["decision_at"], utc=True, errors="coerce")
        label_available = pd.to_datetime(frame["label_available_at"], utc=True, errors="coerce")
        if decision.isna().any() or label_available.isna().any():
            raise ValueError("nested walk-forward timestamps are invalid")
        unique_times = decision.drop_duplicates().sort_values().reset_index(drop=True)
        first = int(math.floor(len(unique_times) * self.first_validation_fraction))
        chunks = np.array_split(unique_times.iloc[first:].to_numpy(), self.inner_folds)
        purge_sec, embargo_sec = _purge_embargo_seconds(frame)
        folds: list[tuple[np.ndarray, np.ndarray]] = []
        previous_validation_end: pd.Timestamp | None = None
        for chunk in chunks:
            if len(chunk) == 0:
                continue
            start = pd.Timestamp(chunk[0])
            end = pd.Timestamp(chunk[-1])
            if start.tzinfo is None:
                start = start.tz_localize("UTC")
                end = end.tz_localize("UTC")
            if previous_validation_end is not None:
                start = max(
                    start,
                    previous_validation_end + pd.Timedelta(seconds=embargo_sec),
                )
            if start > end:
                continue
            train_cutoff = start - pd.Timedelta(seconds=purge_sec)
            train = np.flatnonzero(
                ((decision < train_cutoff) & (label_available < start)).to_numpy()
            )
            validation = np.flatnonzero(((decision >= start) & (decision <= end)).to_numpy())
            if len(train) >= 50 and len(validation) >= 10:
                folds.append((train, validation))
                previous_validation_end = end
        if len(folds) < 2:
            raise ValueError("insufficient rows for nested walk-forward selection")
        return tuple(folds), purge_sec, embargo_sec

    def select_and_fit(
        self,
        outer_train: pd.DataFrame,
        feature_columns: Sequence[str],
        *,
        score_calibration_quantile: float | None = None,
        score_calibration_tail_penalty: float = 0.0,
    ) -> NestedModelSelection:
        # The selector never mutates the caller's frame. Keeping the original
        # indexed view avoids duplicating a full multi-horizon training fold.
        if score_calibration_quantile is not None and not (
            0.0 < score_calibration_quantile < 1.0
        ):
            raise ValueError("score calibration quantile must be strictly between 0 and 1")
        if score_calibration_tail_penalty < 0:
            raise ValueError("score calibration tail penalty cannot be negative")
        data = outer_train
        folds, purge_sec, embargo_sec = self._folds(data)
        candidate_results: list[dict[str, object]] = []
        for config in self.candidate_configs:
            fold_results: list[dict[str, object]] = []
            oof_scores: list[float] = []
            for fold_number, (train_positions, validation_positions) in enumerate(folds, start=1):
                train = data.iloc[train_positions]
                validation = data.iloc[validation_positions]
                model = TwoStageAlphaModel(config).fit(train, feature_columns)
                predictions = model.predict(validation)
                if score_calibration_quantile is not None:
                    oof_scores.extend(
                        _direction_consistent_scores(
                            validation,
                            predictions,
                            tail_penalty=score_calibration_tail_penalty,
                        )
                    )
                gate_diagnostics = prediction_gate_diagnostics(
                    validation,
                    predictions,
                    meta_threshold=config.meta_trade_probability,
                )
                selected = _selected_rows(validation, predictions)
                gate_diagnostics["non_overlapping_selected_decisions"] = len(selected)
                gate_diagnostics["overlap_policy"] = (
                    "one_active_position_per_symbol_until_exit_or_label_available_at"
                )
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
                        "train_decision_end": pd.to_datetime(
                            data.iloc[train_positions]["decision_at"], utc=True
                        ).max().isoformat(),
                        "train_label_available_max": pd.to_datetime(
                            data.iloc[train_positions]["label_available_at"], utc=True
                        ).max().isoformat(),
                        "validation_start": pd.to_datetime(
                            data.iloc[validation_positions]["decision_at"], utc=True
                        ).min().isoformat(),
                        "validation_end": pd.to_datetime(
                            data.iloc[validation_positions]["decision_at"], utc=True
                        ).max().isoformat(),
                        "purge_sec": purge_sec,
                        "embargo_sec": embargo_sec,
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
            score_calibration = None
            if score_calibration_quantile is not None:
                if not oof_scores:
                    raise ValueError(
                        "inner OOS score calibration produced no direction-consistent scores"
                    )
                score_calibration = {
                    "source": "inner_walk_forward_oos_predictions_only",
                    "quantile": score_calibration_quantile,
                    "tail_penalty": score_calibration_tail_penalty,
                    "observation_count": len(oof_scores),
                    "threshold": float(
                        np.quantile(
                            np.asarray(oof_scores, dtype=float),
                            score_calibration_quantile,
                            method="higher",
                        )
                    ),
                }
            candidate_results.append(
                {
                    "config_id": _config_id(config),
                    "config": asdict(config),
                    "selection_score": score,
                    "positive_inner_fold_ratio": positive_folds / len(fold_results),
                    "inner_folds": fold_results,
                    "oof_score_calibration": score_calibration,
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
        selected_calibration = selected_result.get("oof_score_calibration")
        selected_threshold = (
            float(selected_calibration["threshold"])
            if isinstance(selected_calibration, Mapping)
            else None
        )
        return NestedModelSelection(
            model=final_model,
            selected_config=selected_config,
            oof_score_threshold=selected_threshold,
            candidate_results=tuple(candidate_results),
            audit={
                "selection_data": "inner_walk_forward_oos_only",
                "outer_oos_used_for_selection": False,
                "inner_oos_overlap_policy": (
                    "one_active_position_per_symbol_until_exit_or_label_available_at"
                ),
                "inner_fold_count": len(folds),
                "inner_purge_sec": purge_sec,
                "inner_embargo_sec": embargo_sec,
                "selected_config_id": selected_result["config_id"],
                "score_calibration": selected_calibration,
                "outer_train_rows": len(data),
            },
        )


__all__: Sequence[str] = ("NestedModelSelection", "NestedWalkForwardSelector")
