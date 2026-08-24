from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from core.model_monitoring import DistributionCheck, scaled_feature_range_guard_score


@dataclass(frozen=True)
class TwoStageConfig:
    direction_iterations: int = 300
    meta_iterations: int = 300
    learning_rate: float = 0.03
    l2: float = 0.02
    ridge: float = 1.0
    tail_penalty: float = 0.50
    meta_trade_probability: float = 0.55
    uncertainty_quantile: float = 0.95
    oof_folds: int = 4
    minimum_oof_train_rows: int = 30
    minimum_oof_validation_rows: int = 10
    expectancy_calibration_bins: int = 10
    minimum_expectancy_clusters: int = 20
    expectancy_lcb_z: float = 1.96
    symbol_head_ridge_multiplier: float = 5.0
    symbol_head_l2: float = 0.10
    minimum_symbol_head_rows: int = 100

    def __post_init__(self) -> None:
        if min(self.direction_iterations, self.meta_iterations) <= 0:
            raise ValueError("model iterations must be positive")
        if not 0 < self.learning_rate <= 1 or min(self.l2, self.ridge, self.tail_penalty) < 0:
            raise ValueError("invalid optimizer or penalty configuration")
        if not 0.5 <= self.meta_trade_probability < 1:
            raise ValueError("meta trade threshold must be at least 0.5")
        if not 0.5 < self.uncertainty_quantile < 1:
            raise ValueError("uncertainty quantile must be in (0.5, 1)")
        if self.oof_folds < 2 or min(
            self.minimum_oof_train_rows,
            self.minimum_oof_validation_rows,
            self.expectancy_calibration_bins,
            self.minimum_expectancy_clusters,
        ) <= 0:
            raise ValueError("OOF folds and minimum row counts are invalid")
        if self.expectancy_lcb_z < 1.645:
            raise ValueError("expectancy lower bound must be at least one-sided 95%")
        if min(self.symbol_head_ridge_multiplier, self.symbol_head_l2) < 0:
            raise ValueError("symbol-head regularization cannot be negative")
        if self.minimum_symbol_head_rows < 50:
            raise ValueError("symbol residual heads require at least 50 rows")


@dataclass(frozen=True)
class TwoStagePrediction:
    p_down: float
    p_flat: float
    p_up: float
    expected_net_return: float
    return_p10: float
    return_p50: float
    return_p90: float
    expected_mae: float
    expected_mfe: float
    uncertainty: float
    meta_trade_probability: float
    lower_bound_net_edge: float
    decision: str
    release_stage: str = "rejected"
    model_family: str = "profitability_two_stage"


def prediction_gate_diagnostics(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
    *,
    meta_threshold: float,
) -> dict[str, object]:
    """Explain exactly where paired action alternatives are filtered."""

    if len(frame) != len(predictions):
        raise ValueError("prediction and frame lengths differ")
    if not 0 <= meta_threshold <= 1:
        raise ValueError("meta threshold must be a probability")
    data = frame.reset_index(drop=True)
    sides = data.get("side", pd.Series("BUY", index=data.index)).astype(str).str.upper()
    meta = np.asarray([item.meta_trade_probability for item in predictions], dtype=float)
    lower = np.asarray([item.lower_bound_net_edge for item in predictions], dtype=float)
    p_up = np.asarray([item.p_up for item in predictions], dtype=float)
    p_down = np.asarray([item.p_down for item in predictions], dtype=float)
    direction = ((sides == "BUY").to_numpy() & (p_up >= p_down)) | (
        (sides == "SELL").to_numpy() & (p_down >= p_up)
    )
    meta_pass = meta >= meta_threshold
    lower_pass = lower > 0
    qualified = meta_pass & lower_pass & direction
    group_columns = [column for column in ("symbol", "decision_at") if column in data]
    if group_columns:
        candidate_decisions = int(data[group_columns].drop_duplicates().shape[0])
        selected_decisions = int(
            data.loc[qualified, group_columns].drop_duplicates().shape[0]
        )
    else:
        candidate_decisions = len(data)
        selected_decisions = int(qualified.sum())
    return {
        "paired_action_rows": len(data),
        "candidate_decisions": candidate_decisions,
        "meta_threshold": meta_threshold,
        "meta_pass_rows": int(meta_pass.sum()),
        "positive_expectancy_lcb_rows": int(lower_pass.sum()),
        "direction_consistent_rows": int(direction.sum()),
        "all_gate_pass_rows": int(qualified.sum()),
        "selected_decisions": selected_decisions,
        "maximum_meta_trade_probability": float(meta.max()) if len(meta) else None,
        "maximum_lower_bound_net_edge": float(lower.max()) if len(lower) else None,
    }


class _Encoder:
    def __init__(self) -> None:
        self.numeric: list[str] = []
        self.categorical: list[str] = []
        self.categories: dict[str, list[str]] = {}
        self.mean: np.ndarray | None = None
        self.scale: np.ndarray | None = None

    def fit(self, frame: pd.DataFrame, feature_columns: Sequence[str]) -> np.ndarray:
        missing = [column for column in feature_columns if column not in frame]
        if missing:
            raise ValueError(f"missing feature columns: {missing}")
        self.numeric = [
            column for column in feature_columns if pd.api.types.is_numeric_dtype(frame[column])
        ]
        self.categorical = [column for column in feature_columns if column not in self.numeric]
        self.categories = {
            column: sorted(frame[column].astype(str).fillna("<missing>").unique().tolist())
            for column in self.categorical
        }
        raw = self._raw(frame)
        self.mean = np.empty(raw.shape[1], dtype=float)
        self.scale = np.empty(raw.shape[1], dtype=float)
        for index in range(raw.shape[1]):
            column = raw[:, index]
            finite = np.isfinite(column)
            if finite.any():
                self.mean[index] = float(column[finite].mean())
                deviation = column[finite] - self.mean[index]
                self.scale[index] = float(np.sqrt(np.mean(deviation * deviation)))
            else:
                self.mean[index] = 0.0
                self.scale[index] = 1.0
            if not np.isfinite(self.scale[index]) or self.scale[index] <= 1e-12:
                self.scale[index] = 1.0
        return self._standardize_in_place(raw)

    def _raw(self, frame: pd.DataFrame) -> np.ndarray:
        width = len(self.numeric) + sum(
            len(self.categories[column]) for column in self.categorical
        )
        if width == 0:
            raise ValueError("at least one model feature is required")
        raw = np.empty((len(frame), width), dtype=float)
        offset = 0
        for column in self.numeric:
            raw[:, offset] = pd.to_numeric(
                frame[column], errors="coerce"
            ).to_numpy(dtype=float, copy=False)
            offset += 1
        for column in self.categorical:
            values = frame[column].astype(str).fillna("<missing>")
            for category in self.categories[column]:
                raw[:, offset] = (values == category).to_numpy(
                    dtype=float, copy=False
                )
                offset += 1
        return raw

    def _standardize_in_place(self, raw: np.ndarray) -> np.ndarray:
        if self.mean is None or self.scale is None:
            raise RuntimeError("encoder is not fitted")
        for index in range(raw.shape[1]):
            column = raw[:, index]
            invalid = ~np.isfinite(column)
            if invalid.any():
                column[invalid] = self.mean[index]
            column -= self.mean[index]
            column /= self.scale[index]
        return raw

    def transform(self, frame: pd.DataFrame) -> np.ndarray:
        if self.mean is None or self.scale is None:
            raise RuntimeError("encoder is not fitted")
        raw = self._raw(frame)
        return self._standardize_in_place(raw)

    def state(self) -> dict[str, object]:
        return {
            "numeric": self.numeric,
            "categorical": self.categorical,
            "categories": self.categories,
            "mean": self.mean.tolist() if self.mean is not None else [],
            "scale": self.scale.tolist() if self.scale is not None else [],
        }

    @classmethod
    def from_state(cls, state: Mapping[str, object]) -> "_Encoder":
        encoder = cls()
        encoder.numeric = [str(value) for value in state["numeric"]]
        encoder.categorical = [str(value) for value in state["categorical"]]
        encoder.categories = {
            str(key): [str(value) for value in values]
            for key, values in dict(state["categories"]).items()
        }
        encoder.mean = np.asarray(state["mean"], dtype=float)
        encoder.scale = np.asarray(state["scale"], dtype=float)
        return encoder


def _softmax(values: np.ndarray) -> np.ndarray:
    shifted = values - values.max(axis=1, keepdims=True)
    exp = np.exp(np.clip(shifted, -50, 50))
    return exp / exp.sum(axis=1, keepdims=True)


def _sigmoid(values: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(values, -40, 40)))


def _purge_embargo_seconds(frame: pd.DataFrame) -> tuple[int, int]:
    """Resolve the fixed horizon and its auditable purge/embargo interval."""

    if "horizon_sec" in frame:
        horizons = pd.to_numeric(frame["horizon_sec"], errors="coerce").dropna().unique()
        if len(horizons) != 1 or float(horizons[0]) <= 0:
            raise ValueError("model training must contain exactly one positive horizon_sec")
        purge_sec = int(round(float(horizons[0])))
    else:
        # Small library callers may omit horizon_sec.  Their observed label
        # latency is the only conservative PIT duration available; production
        # pooled panels always carry the explicit fixed horizon contract.
        decision = pd.to_datetime(frame["decision_at"], utc=True, errors="coerce")
        available = pd.to_datetime(
            frame["label_available_at"], utc=True, errors="coerce"
        )
        delays = (available - decision).dt.total_seconds()
        if delays.isna().any() or (delays <= 0).any():
            raise ValueError("cannot infer a positive purge from label timestamps")
        purge_sec = max(1, int(math.ceil(float(delays.max()))))
    return purge_sec, int(round(purge_sec * 0.25))


def _fit_ridge(x: np.ndarray, y: np.ndarray, penalty: float) -> np.ndarray:
    width = x.shape[1] + 1
    gram = np.empty((width, width), dtype=float)
    column_sums = x.sum(axis=0)
    gram[0, 0] = len(x)
    gram[0, 1:] = column_sums
    gram[1:, 0] = column_sums
    gram[1:, 1:] = x.T @ x
    right_hand_side = np.empty(width, dtype=float)
    right_hand_side[0] = y.sum()
    right_hand_side[1:] = x.T @ y
    regularizer = np.eye(width) * penalty
    regularizer[0, 0] = 0.0
    return np.linalg.solve(gram + regularizer, right_hand_side)


def _ridge_predict(x: np.ndarray, weights: np.ndarray) -> np.ndarray:
    return weights[0] + x @ weights[1:]


def _linear_predict(x: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Apply scalar or multi-output weights without allocating an intercept column."""

    return weights[0] + x @ weights[1:]


class TwoStageAlphaModel:
    """Pooled direction/distribution model followed by a TRADE/NO_TRADE model.

    It deliberately exposes no candidate/live switch.  The release stage is
    always rejected until an external profitability gate evaluates an untouched
    lockbox and signs a release manifest.
    """

    REQUIRED_LABELS = ("net_return", "mae", "mfe")
    ACTION_INTERACTION_PREFIX = "__side_x__"

    def __init__(self, config: TwoStageConfig | None = None) -> None:
        self.config = config or TwoStageConfig()
        self.encoder = _Encoder()
        self.direction_encoder = _Encoder()
        self.feature_columns: list[str] = []
        self.action_interaction_columns: list[str] = []
        self.direction_feature_columns: list[str] = []
        self.direction_weights: np.ndarray | None = None
        self.net_weights: np.ndarray | None = None
        self.mae_weights: np.ndarray | None = None
        self.mfe_weights: np.ndarray | None = None
        self.meta_weights: np.ndarray | None = None
        self.symbol_net_weights: dict[str, np.ndarray] = {}
        self.symbol_mae_weights: dict[str, np.ndarray] = {}
        self.symbol_mfe_weights: dict[str, np.ndarray] = {}
        self.symbol_direction_weights: dict[str, np.ndarray] = {}
        self.residual_quantiles = np.zeros(3, dtype=float)
        self.conformal_lower_residual = 0.0
        self.expectancy_cutpoints = np.zeros(0, dtype=float)
        self.expectancy_means = np.zeros(1, dtype=float)
        self.expectancy_lower_bounds = np.full(1, -1e12, dtype=float)
        self.expectancy_calibration: list[dict[str, object]] = []
        self.training_audit: dict[str, object] = {}
        self.fitted = False
        self.release_stage = "rejected"

    @staticmethod
    def _symbols(frame: pd.DataFrame) -> np.ndarray | None:
        if "symbol" not in frame:
            return None
        return frame["symbol"].astype(str).str.upper().to_numpy()

    def _fit_symbol_regression_heads(
        self,
        x: np.ndarray,
        symbols: np.ndarray | None,
        targets: np.ndarray,
        global_weights: np.ndarray,
    ) -> dict[str, np.ndarray]:
        """Fit regularized per-symbol corrections around the pooled model."""

        if symbols is None:
            return {}
        residual = targets - _ridge_predict(x, global_weights)
        heads: dict[str, np.ndarray] = {}
        penalty = self.config.ridge * self.config.symbol_head_ridge_multiplier
        for symbol in sorted(set(symbols.tolist())):
            positions = np.flatnonzero(symbols == symbol)
            if len(positions) < self.config.minimum_symbol_head_rows:
                continue
            heads[str(symbol)] = _fit_ridge(x[positions], residual[positions], penalty)
        return heads

    def _fit_symbol_direction_heads(
        self,
        direction_x: np.ndarray,
        symbols: np.ndarray | None,
        targets: np.ndarray,
    ) -> dict[str, np.ndarray]:
        if symbols is None or self.direction_weights is None:
            return {}
        global_logits = _linear_predict(direction_x, self.direction_weights)
        heads: dict[str, np.ndarray] = {}
        for symbol in sorted(set(symbols.tolist())):
            positions = np.flatnonzero(symbols == symbol)
            if len(positions) < self.config.minimum_symbol_head_rows:
                continue
            local_x = direction_x[positions]
            local_global = global_logits[positions]
            weights = np.zeros_like(self.direction_weights)
            for _ in range(max(20, self.config.direction_iterations // 2)):
                errors = _softmax(local_global + _linear_predict(local_x, weights))
                errors[np.arange(len(positions)), targets[positions]] -= 1.0
                gradient = np.empty_like(weights)
                gradient[0] = errors.mean(axis=0)
                gradient[1:] = local_x.T @ errors / len(positions)
                gradient[1:] += self.config.symbol_head_l2 * weights[1:]
                weights -= self.config.learning_rate * gradient
            heads[str(symbol)] = weights
        return heads

    @staticmethod
    def _apply_symbol_regression_heads(
        values: np.ndarray,
        x: np.ndarray,
        symbols: np.ndarray | None,
        heads: Mapping[str, np.ndarray],
    ) -> np.ndarray:
        output = values.copy()
        if symbols is None:
            return output
        for symbol, weights in heads.items():
            positions = np.flatnonzero(symbols == symbol)
            if len(positions):
                output[positions] += _ridge_predict(x[positions], weights)
        return output

    def _action_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Add signed numeric interactions for action-conditional outcomes.

        Direction is a property of the market state and remains side-free. Net
        return, MAE and MFE are properties of a proposed BUY/SELL action, so a
        mere side intercept cannot represent an edge whose sign reverses with
        the action.
        """

        if not self.action_interaction_columns:
            return frame
        if "side" not in frame:
            raise ValueError("side is required by the action interaction contract")
        output = frame.loc[:, self.feature_columns].copy()
        side = frame["side"].astype(str).str.upper()
        if not side.isin({"BUY", "SELL"}).all():
            raise ValueError("side must be BUY or SELL")
        sign = side.map({"BUY": 1.0, "SELL": -1.0}).to_numpy(float)
        for column in self.action_interaction_columns:
            values = pd.to_numeric(frame[column], errors="coerce").to_numpy(float)
            output[f"{self.ACTION_INTERACTION_PREFIX}{column}"] = values * sign
        return output

    @staticmethod
    def _labels(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        missing = [column for column in TwoStageAlphaModel.REQUIRED_LABELS if column not in frame]
        if missing:
            raise ValueError(f"missing training labels: {missing}")
        net = pd.to_numeric(frame["net_return"], errors="raise").to_numpy(float)
        mae = np.maximum(0.0, pd.to_numeric(frame["mae"], errors="raise").to_numpy(float))
        mfe = np.maximum(0.0, pd.to_numeric(frame["mfe"], errors="raise").to_numpy(float))
        if "direction_label" in frame:
            normalized = frame["direction_label"].astype(str).str.lower()
            mapping = {"down": 0, "short": 0, "flat": 1, "up": 2, "long": 2}
            if not normalized.isin(mapping).all():
                raise ValueError("direction_label must be down/flat/up")
            direction = normalized.map(mapping).to_numpy(int)
        else:
            direction = np.where(net > 1e-12, 2, np.where(net < -1e-12, 0, 1)).astype(int)
        return net, mae, mfe, direction

    def _fit_level_one_only(
        self,
        frame: pd.DataFrame,
        feature_columns: Sequence[str],
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        self.feature_columns = list(feature_columns)
        self.action_interaction_columns = (
            [
                column
                for column in self.feature_columns
                if column != "side" and pd.api.types.is_numeric_dtype(frame[column])
            ]
            if "side" in self.feature_columns
            else []
        )
        action_columns = self.feature_columns + [
            f"{self.ACTION_INTERACTION_PREFIX}{column}"
            for column in self.action_interaction_columns
        ]
        x = self.encoder.fit(self._action_frame(frame), action_columns)
        net, mae, mfe, _ = self._labels(frame)

        # Direction is a property of the market decision, not of the proposed
        # order side.  The paired BUY/SELL rows are alternative actions and
        # must not be counted as two independent direction observations.
        self.direction_feature_columns = [
            column for column in self.feature_columns if column != "side"
        ]
        direction_frame = frame.drop_duplicates(
            [column for column in ("symbol", "decision_at") if column in frame],
            keep="first",
        ).reset_index(drop=True)
        _, _, _, direction = self._labels(direction_frame)
        direction_x = self.direction_encoder.fit(
            direction_frame, self.direction_feature_columns
        )
        self.direction_weights = np.zeros((direction_x.shape[1] + 1, 3), dtype=float)
        for _ in range(self.config.direction_iterations):
            errors = _softmax(_linear_predict(direction_x, self.direction_weights))
            errors[np.arange(len(direction_x)), direction] -= 1.0
            gradient = np.empty_like(self.direction_weights)
            gradient[0] = errors.mean(axis=0)
            gradient[1:] = direction_x.T @ errors / len(direction_x)
            gradient[1:] += self.config.l2 * self.direction_weights[1:]
            self.direction_weights -= self.config.learning_rate * gradient
        self.symbol_direction_weights = self._fit_symbol_direction_heads(
            direction_x,
            self._symbols(direction_frame),
            direction,
        )

        # Level one predicts the actual after-cost net return.  Tail penalties
        # belong in the meta utility, not in a field named expected_net_return.
        self.net_weights = _fit_ridge(x, net, self.config.ridge)
        self.mae_weights = _fit_ridge(x, mae, self.config.ridge)
        self.mfe_weights = _fit_ridge(x, mfe, self.config.ridge)
        action_symbols = self._symbols(frame)
        self.symbol_net_weights = self._fit_symbol_regression_heads(
            x, action_symbols, net, self.net_weights
        )
        self.symbol_mae_weights = self._fit_symbol_regression_heads(
            x, action_symbols, mae, self.mae_weights
        )
        self.symbol_mfe_weights = self._fit_symbol_regression_heads(
            x, action_symbols, mfe, self.mfe_weights
        )
        return x, net, mae, mfe

    def _oof_level_one(
        self,
        frame: pd.DataFrame,
        feature_columns: Sequence[str],
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[dict[str, object]]]:
        if "decision_at" not in frame or "label_available_at" not in frame:
            raise ValueError(
                "decision_at and label_available_at are required for leakage-safe OOF training"
            )
        decision = pd.to_datetime(frame["decision_at"], utc=True, errors="coerce")
        label_available = pd.to_datetime(frame["label_available_at"], utc=True, errors="coerce")
        if decision.isna().any() or label_available.isna().any():
            raise ValueError("OOF timestamps contain invalid values")
        if (label_available <= decision).any():
            raise ValueError("label_available_at must be strictly after decision_at")
        unique_times = decision.drop_duplicates().sort_values().reset_index(drop=True)
        if len(unique_times) < self.config.oof_folds + 2:
            raise ValueError("too few unique timestamps for OOF training")
        first_validation = max(1, int(math.floor(len(unique_times) * 0.40)))
        validation_times = np.array_split(
            unique_times.iloc[first_validation:].to_numpy(), self.config.oof_folds
        )
        purge_sec, embargo_sec = _purge_embargo_seconds(frame)
        probabilities = np.full((len(frame), 3), np.nan, dtype=float)
        expected_net = np.full(len(frame), np.nan, dtype=float)
        expected_mae = np.full(len(frame), np.nan, dtype=float)
        expected_mfe = np.full(len(frame), np.nan, dtype=float)
        audit: list[dict[str, object]] = []
        previous_validation_end: pd.Timestamp | None = None
        for fold_number, times in enumerate(validation_times, start=1):
            if len(times) == 0:
                continue
            validation_start = pd.Timestamp(times[0])
            validation_end = pd.Timestamp(times[-1])
            if previous_validation_end is not None:
                validation_start = max(
                    validation_start,
                    previous_validation_end + timedelta(seconds=embargo_sec),
                )
            if validation_start > validation_end:
                continue
            train_cutoff = validation_start - timedelta(seconds=purge_sec)
            train_mask = (decision < train_cutoff) & (
                label_available < validation_start
            )
            validation_mask = (decision >= validation_start) & (decision <= validation_end)
            train_positions = np.flatnonzero(train_mask.to_numpy())
            validation_positions = np.flatnonzero(validation_mask.to_numpy())
            if len(train_positions) < self.config.minimum_oof_train_rows:
                continue
            if len(validation_positions) < self.config.minimum_oof_validation_rows:
                continue
            local = TwoStageAlphaModel(self.config)
            local._fit_level_one_only(frame.iloc[train_positions], feature_columns)
            validation_x = local.encoder.transform(
                local._action_frame(frame.iloc[validation_positions])
            )
            validation_direction_x = local.direction_encoder.transform(
                frame.iloc[validation_positions]
            )
            fold_probabilities, fold_net, fold_mae, fold_mfe = local._level_one(
                validation_x,
                validation_direction_x,
                symbols=local._symbols(frame.iloc[validation_positions]),
            )
            probabilities[validation_positions] = fold_probabilities
            expected_net[validation_positions] = fold_net
            expected_mae[validation_positions] = fold_mae
            expected_mfe[validation_positions] = fold_mfe
            audit.append(
                {
                    "fold": fold_number,
                    "train_rows": len(train_positions),
                    "validation_rows": len(validation_positions),
                    "train_label_available_max": label_available.iloc[train_positions].max().isoformat(),
                    "train_decision_max": decision.iloc[train_positions].max().isoformat(),
                    "validation_start": validation_start.isoformat(),
                    "validation_end": validation_end.isoformat(),
                    "purge_sec": purge_sec,
                    "embargo_sec": embargo_sec,
                }
            )
            previous_validation_end = validation_end
        valid = np.isfinite(expected_net)
        minimum_oof_rows = max(20, self.config.minimum_oof_validation_rows)
        if int(valid.sum()) < minimum_oof_rows or len(audit) < 2:
            raise ValueError("insufficient leakage-safe OOF rows for level-two training")
        return probabilities, expected_net, expected_mae, expected_mfe, valid, audit

    @staticmethod
    def _compose_meta_features(
        probabilities: np.ndarray,
        expected_net: np.ndarray,
        mae: np.ndarray,
        mfe: np.ndarray,
    ) -> np.ndarray:
        entropy = -np.sum(
            probabilities * np.log(np.clip(probabilities, 1e-12, 1.0)), axis=1
        ) / math.log(3.0)
        return np.column_stack([probabilities, expected_net, mae, mfe, entropy])

    def _fit_expectancy_calibration(
        self,
        expected_net: np.ndarray,
        actual_net: np.ndarray,
        decision_at: pd.Series,
    ) -> None:
        """Calibrate a lower confidence bound for conditional mean net edge.

        Return quantiles describe the noisy single-trade outcome distribution.
        They are not a confidence interval for conditional expectancy.  This
        calibration bins leakage-safe OOF predictions, aggregates actual net
        returns by UTC day, and computes a conservative 95% lower confidence
        bound across those time clusters.
        """

        quantiles = np.linspace(0.0, 1.0, self.config.expectancy_calibration_bins + 1)
        raw_edges = np.quantile(expected_net, quantiles)
        cutpoints = np.unique(raw_edges[1:-1])
        assignments = np.searchsorted(cutpoints, expected_net, side="right")
        days = pd.to_datetime(decision_at, utc=True, errors="raise").dt.floor("D")
        means: list[float] = []
        lower_bounds: list[float] = []
        evidence: list[dict[str, object]] = []
        for bin_index in range(len(cutpoints) + 1):
            mask = assignments == bin_index
            clustered = pd.DataFrame(
                {"day": days.to_numpy()[mask], "net_return": actual_net[mask]}
            ).groupby("day", sort=True)["net_return"].mean()
            row_count = int(mask.sum())
            cluster_count = len(clustered)
            mean = float(clustered.mean()) if cluster_count else float("nan")
            if cluster_count >= self.config.minimum_expectancy_clusters:
                standard_error = float(clustered.std(ddof=1) / math.sqrt(cluster_count))
                lower = mean - self.config.expectancy_lcb_z * standard_error
            else:
                standard_error = None
                lower = -1e12
            means.append(mean)
            lower_bounds.append(lower)
            evidence.append(
                {
                    "bin": bin_index,
                    "predicted_net_min": float(expected_net[mask].min()) if row_count else None,
                    "predicted_net_max": float(expected_net[mask].max()) if row_count else None,
                    "row_count": row_count,
                    "utc_day_cluster_count": cluster_count,
                    "actual_mean_net_return": mean,
                    "cluster_standard_error": standard_error,
                    "lower_bound_net_edge": lower if lower > -1e11 else None,
                }
            )
        self.expectancy_cutpoints = np.asarray(cutpoints, dtype=float)
        self.expectancy_means = np.asarray(means, dtype=float)
        self.expectancy_lower_bounds = np.asarray(lower_bounds, dtype=float)
        self.expectancy_calibration = evidence

    def _expectancy_lower_bound(self, expected_net: np.ndarray) -> np.ndarray:
        assignments = np.searchsorted(
            self.expectancy_cutpoints, expected_net, side="right"
        )
        return self.expectancy_lower_bounds[assignments]

    def fit(self, frame: pd.DataFrame, feature_columns: Sequence[str]) -> "TwoStageAlphaModel":
        if len(frame) < 50:
            raise ValueError("at least 50 pooled observations are required")
        data = frame
        self.action_interaction_columns = (
            [
                column
                for column in feature_columns
                if column != "side" and pd.api.types.is_numeric_dtype(data[column])
            ]
            if "side" in feature_columns
            else []
        )
        net, mae, _, _ = self._labels(data)
        (
            oof_probabilities,
            oof_net,
            oof_mae,
            oof_mfe,
            oof_valid,
            oof_audit,
        ) = self._oof_level_one(data, feature_columns)
        meta_x = self._compose_meta_features(
            oof_probabilities[oof_valid],
            oof_net[oof_valid],
            oof_mae[oof_valid],
            oof_mfe[oof_valid],
        )
        utility = net[oof_valid] - self.config.tail_penalty * mae[oof_valid]
        trade_target = (utility > 0).astype(float)
        self.meta_weights = np.zeros(meta_x.shape[1] + 1, dtype=float)
        sample_weight = 1.0 + np.minimum(10.0, np.abs(utility) / max(float(np.median(np.abs(utility))) if len(utility) else 1.0, 1e-12))
        for _ in range(self.config.meta_iterations):
            probability = _sigmoid(_linear_predict(meta_x, self.meta_weights))
            errors = (probability - trade_target) * sample_weight
            gradient = np.empty_like(self.meta_weights)
            gradient[0] = errors.mean()
            gradient[1:] = meta_x.T @ errors / len(meta_x)
            gradient[1:] += self.config.l2 * self.meta_weights[1:]
            self.meta_weights -= self.config.learning_rate * gradient
        residuals = net[oof_valid] - oof_net[oof_valid]
        self.residual_quantiles = np.quantile(residuals, [0.10, 0.50, 0.90])
        self.conformal_lower_residual = float(
            np.quantile(residuals, 1.0 - self.config.uncertainty_quantile, method="lower")
        )
        self._fit_expectancy_calibration(
            oof_net[oof_valid],
            net[oof_valid],
            data.loc[oof_valid, "decision_at"].reset_index(drop=True),
        )
        self.training_audit = {
            "level_two_training_source": "out_of_fold_level_one",
            "return_calibration_source": "out_of_fold_residuals",
            "calibration_method": "oof_residual_quantiles_conformal_and_clustered_expectancy_lcb",
            "direction_training_side_feature": False,
            "direction_training_paired_decisions_deduplicated": True,
            "action_outcome_side_interactions": list(
                self.action_interaction_columns
            ),
            "pooled_model_structure": "global_pooled_model_plus_regularized_symbol_residual_heads",
            "symbol_outcome_heads": sorted(self.symbol_net_weights),
            "symbol_direction_heads": sorted(self.symbol_direction_weights),
            "symbol_head_outer_oos_validation_required": True,
            "expected_net_target": "after_cost_net_return",
            "lower_bound_edge_method": "oof_predicted_net_bins_with_utc_day_clustered_95pct_lcb",
            "expectancy_calibration": self.expectancy_calibration,
            "pit_label_cutoff_enforced": True,
            "training_rows": len(data),
            "oof_rows": int(oof_valid.sum()),
            "oof_fold_count": len(oof_audit),
            "oof_folds": oof_audit,
        }
        self.encoder = _Encoder()
        self.direction_encoder = _Encoder()
        self._fit_level_one_only(data, feature_columns)
        self.training_audit["symbol_outcome_heads"] = sorted(
            self.symbol_net_weights
        )
        self.training_audit["symbol_direction_heads"] = sorted(
            self.symbol_direction_weights
        )
        self.fitted = True
        self.release_stage = "rejected"
        return self

    def _level_one(
        self,
        x: np.ndarray,
        direction_x: np.ndarray,
        *,
        symbols: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        if any(value is None for value in (self.direction_weights, self.net_weights, self.mae_weights, self.mfe_weights)):
            raise RuntimeError("model is not fitted")
        direction_logits = _linear_predict(direction_x, self.direction_weights)
        if symbols is not None:
            for symbol, weights in self.symbol_direction_weights.items():
                positions = np.flatnonzero(symbols == symbol)
                if len(positions):
                    direction_logits[positions] += _linear_predict(
                        direction_x[positions], weights
                    )
        probabilities = _softmax(direction_logits)
        expected_net = _ridge_predict(x, self.net_weights)
        mae = _ridge_predict(x, self.mae_weights)
        mfe = _ridge_predict(x, self.mfe_weights)
        expected_net = self._apply_symbol_regression_heads(
            expected_net, x, symbols, self.symbol_net_weights
        )
        mae = np.maximum(
            0.0,
            self._apply_symbol_regression_heads(
                mae, x, symbols, self.symbol_mae_weights
            ),
        )
        mfe = np.maximum(
            0.0,
            self._apply_symbol_regression_heads(
                mfe, x, symbols, self.symbol_mfe_weights
            ),
        )
        return probabilities, expected_net, mae, mfe

    def _meta_features(
        self,
        x: np.ndarray,
        direction_x: np.ndarray,
        *,
        symbols: np.ndarray | None = None,
    ) -> np.ndarray:
        probabilities, expected_net, mae, mfe = self._level_one(
            x, direction_x, symbols=symbols
        )
        return self._compose_meta_features(probabilities, expected_net, mae, mfe)

    def predict(self, frame: pd.DataFrame) -> list[TwoStagePrediction]:
        if not self.fitted or self.meta_weights is None:
            raise RuntimeError("model is not fitted")
        x = self.encoder.transform(self._action_frame(frame))
        direction_x = self.direction_encoder.transform(frame)
        symbols = self._symbols(frame)
        probabilities, expected_net, mae, mfe = self._level_one(
            x, direction_x, symbols=symbols
        )
        meta_x = self._compose_meta_features(
            probabilities, expected_net, mae, mfe
        )
        trade_probability = _sigmoid(_linear_predict(meta_x, self.meta_weights))
        entropy = meta_x[:, -1]
        p10 = expected_net + self.residual_quantiles[0]
        p50 = expected_net + self.residual_quantiles[1]
        p90 = expected_net + self.residual_quantiles[2]
        lower_expectancy = self._expectancy_lower_bound(expected_net)
        output: list[TwoStagePrediction] = []
        for index in range(len(frame)):
            lower_edge = float(lower_expectancy[index])
            decision = (
                "TRADE"
                if trade_probability[index] >= self.config.meta_trade_probability and lower_edge > 0
                else "NO_TRADE"
            )
            output.append(
                TwoStagePrediction(
                    p_down=float(probabilities[index, 0]),
                    p_flat=float(probabilities[index, 1]),
                    p_up=float(probabilities[index, 2]),
                    expected_net_return=float(expected_net[index]),
                    return_p10=float(p10[index]),
                    return_p50=float(p50[index]),
                    return_p90=float(p90[index]),
                    expected_mae=float(mae[index]),
                    expected_mfe=float(mfe[index]),
                    uncertainty=float(entropy[index]),
                    meta_trade_probability=float(trade_probability[index]),
                    lower_bound_net_edge=lower_edge,
                    decision=decision,
                    release_stage=self.release_stage,
                )
            )
        return output

    def feature_range_guard(self, frame: pd.DataFrame) -> DistributionCheck:
        """Evaluate Level-1 inputs in the fitted standardized feature spaces."""

        if not self.fitted:
            raise RuntimeError("model is not fitted")
        action_values = self.encoder.transform(self._action_frame(frame))
        direction_values = self.direction_encoder.transform(frame)
        combined = np.concatenate((action_values, direction_values), axis=1)
        return scaled_feature_range_guard_score(combined, scaler=None)

    def save(self, path: Path) -> None:
        if not self.fitted:
            raise RuntimeError("cannot save an unfitted model")
        payload = {
            "model_family": "profitability_two_stage",
            "release_stage": "rejected",
            "config": asdict(self.config),
            "feature_columns": self.feature_columns,
            "action_interaction_columns": self.action_interaction_columns,
            "encoder": self.encoder.state(),
            "direction_feature_columns": self.direction_feature_columns,
            "direction_encoder": self.direction_encoder.state(),
            "direction_weights": self.direction_weights.tolist(),
            "net_weights": self.net_weights.tolist(),
            "mae_weights": self.mae_weights.tolist(),
            "mfe_weights": self.mfe_weights.tolist(),
            "meta_weights": self.meta_weights.tolist(),
            "symbol_net_weights": {
                symbol: weights.tolist()
                for symbol, weights in self.symbol_net_weights.items()
            },
            "symbol_mae_weights": {
                symbol: weights.tolist()
                for symbol, weights in self.symbol_mae_weights.items()
            },
            "symbol_mfe_weights": {
                symbol: weights.tolist()
                for symbol, weights in self.symbol_mfe_weights.items()
            },
            "symbol_direction_weights": {
                symbol: weights.tolist()
                for symbol, weights in self.symbol_direction_weights.items()
            },
            "residual_quantiles": self.residual_quantiles.tolist(),
            "conformal_lower_residual": self.conformal_lower_residual,
            "expectancy_cutpoints": self.expectancy_cutpoints.tolist(),
            "expectancy_means": self.expectancy_means.tolist(),
            "expectancy_lower_bounds": self.expectancy_lower_bounds.tolist(),
            "expectancy_calibration": self.expectancy_calibration,
            "training_audit": self.training_audit,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        temporary.replace(path)

    @classmethod
    def load(cls, path: Path) -> "TwoStageAlphaModel":
        payload = json.loads(path.read_text(encoding="utf-8"))
        model = cls(TwoStageConfig(**payload["config"]))
        model.feature_columns = [str(value) for value in payload["feature_columns"]]
        model.action_interaction_columns = [
            str(value) for value in payload.get("action_interaction_columns", [])
        ]
        model.encoder = _Encoder.from_state(payload["encoder"])
        model.direction_feature_columns = [
            str(value)
            for value in payload.get("direction_feature_columns", model.feature_columns)
        ]
        model.direction_encoder = _Encoder.from_state(
            payload.get("direction_encoder", payload["encoder"])
        )
        for name in (
            "direction_weights",
            "net_weights",
            "mae_weights",
            "mfe_weights",
            "meta_weights",
            "residual_quantiles",
        ):
            setattr(model, name, np.asarray(payload[name], dtype=float))
        for name in (
            "symbol_net_weights",
            "symbol_mae_weights",
            "symbol_mfe_weights",
            "symbol_direction_weights",
        ):
            setattr(
                model,
                name,
                {
                    str(symbol): np.asarray(weights, dtype=float)
                    for symbol, weights in dict(payload.get(name, {})).items()
                },
            )
        model.conformal_lower_residual = float(
            payload.get("conformal_lower_residual", model.residual_quantiles[0])
        )
        model.expectancy_cutpoints = np.asarray(
            payload.get("expectancy_cutpoints", []), dtype=float
        )
        model.expectancy_means = np.asarray(
            payload.get("expectancy_means", [0.0]), dtype=float
        )
        model.expectancy_lower_bounds = np.asarray(
            payload.get("expectancy_lower_bounds", [-1e12]), dtype=float
        )
        model.expectancy_calibration = list(payload.get("expectancy_calibration", []))
        model.training_audit = dict(
            payload.get(
                "training_audit",
                {
                    "level_two_training_source": "legacy_unknown",
                    "return_calibration_source": "legacy_unknown",
                    "pit_label_cutoff_enforced": False,
                },
            )
        )
        model.fitted = True
        model.release_stage = "rejected"
        return model


__all__: Sequence[str] = (
    "TwoStageAlphaModel",
    "TwoStageConfig",
    "TwoStagePrediction",
    "prediction_gate_diagnostics",
)
