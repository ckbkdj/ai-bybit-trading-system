from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


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
            self.minimum_oof_train_rows, self.minimum_oof_validation_rows
        ) <= 0:
            raise ValueError("OOF folds and minimum row counts are invalid")


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
        self.mean = np.nanmean(raw, axis=0)
        self.scale = np.nanstd(raw, axis=0)
        self.mean = np.where(np.isfinite(self.mean), self.mean, 0.0)
        self.scale = np.where(np.isfinite(self.scale) & (self.scale > 1e-12), self.scale, 1.0)
        return self.transform(frame)

    def _raw(self, frame: pd.DataFrame) -> np.ndarray:
        blocks: list[np.ndarray] = []
        if self.numeric:
            numeric = frame[self.numeric].apply(pd.to_numeric, errors="coerce").to_numpy(float)
            blocks.append(numeric)
        for column in self.categorical:
            values = frame[column].astype(str).fillna("<missing>")
            categories = self.categories[column]
            blocks.append(np.column_stack([(values == category).to_numpy(float) for category in categories]))
        if not blocks:
            raise ValueError("at least one model feature is required")
        return np.column_stack(blocks)

    def transform(self, frame: pd.DataFrame) -> np.ndarray:
        if self.mean is None or self.scale is None:
            raise RuntimeError("encoder is not fitted")
        raw = self._raw(frame)
        raw = np.where(np.isfinite(raw), raw, self.mean)
        return (raw - self.mean) / self.scale

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


def _fit_ridge(x: np.ndarray, y: np.ndarray, penalty: float) -> np.ndarray:
    design = np.column_stack([np.ones(len(x)), x])
    regularizer = np.eye(design.shape[1]) * penalty
    regularizer[0, 0] = 0.0
    return np.linalg.solve(design.T @ design + regularizer, design.T @ y)


def _ridge_predict(x: np.ndarray, weights: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones(len(x)), x]) @ weights


class TwoStageAlphaModel:
    """Pooled direction/distribution model followed by a TRADE/NO_TRADE model.

    It deliberately exposes no candidate/live switch.  The release stage is
    always rejected until an external profitability gate evaluates an untouched
    lockbox and signs a release manifest.
    """

    REQUIRED_LABELS = ("net_return", "mae", "mfe")

    def __init__(self, config: TwoStageConfig | None = None) -> None:
        self.config = config or TwoStageConfig()
        self.encoder = _Encoder()
        self.feature_columns: list[str] = []
        self.direction_weights: np.ndarray | None = None
        self.net_weights: np.ndarray | None = None
        self.mae_weights: np.ndarray | None = None
        self.mfe_weights: np.ndarray | None = None
        self.meta_weights: np.ndarray | None = None
        self.residual_quantiles = np.zeros(3, dtype=float)
        self.conformal_lower_residual = 0.0
        self.training_audit: dict[str, object] = {}
        self.fitted = False
        self.release_stage = "rejected"

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
        x = self.encoder.fit(frame, self.feature_columns)
        net, mae, mfe, direction = self._labels(frame)
        self.direction_weights = np.zeros((x.shape[1] + 1, 3), dtype=float)
        design = np.column_stack([np.ones(len(x)), x])
        targets = np.eye(3)[direction]
        tail_weights = 1.0 + self.config.tail_penalty * mae / max(float(np.median(mae[mae > 0])) if (mae > 0).any() else 1.0, 1e-12)
        tail_weights = np.clip(tail_weights, 1.0, 10.0)
        for _ in range(self.config.direction_iterations):
            probabilities = _softmax(design @ self.direction_weights)
            gradient = design.T @ ((probabilities - targets) * tail_weights[:, None]) / len(x)
            gradient[1:] += self.config.l2 * self.direction_weights[1:]
            self.direction_weights -= self.config.learning_rate * gradient

        self.net_weights = _fit_ridge(x, net - self.config.tail_penalty * mae, self.config.ridge)
        self.mae_weights = _fit_ridge(x, mae, self.config.ridge)
        self.mfe_weights = _fit_ridge(x, mfe, self.config.ridge)
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
        probabilities = np.full((len(frame), 3), np.nan, dtype=float)
        expected_net = np.full(len(frame), np.nan, dtype=float)
        expected_mae = np.full(len(frame), np.nan, dtype=float)
        expected_mfe = np.full(len(frame), np.nan, dtype=float)
        audit: list[dict[str, object]] = []
        for fold_number, times in enumerate(validation_times, start=1):
            if len(times) == 0:
                continue
            validation_start = pd.Timestamp(times[0])
            validation_end = pd.Timestamp(times[-1])
            train_mask = (decision < validation_start) & (label_available < validation_start)
            validation_mask = (decision >= validation_start) & (decision <= validation_end)
            train_positions = np.flatnonzero(train_mask.to_numpy())
            validation_positions = np.flatnonzero(validation_mask.to_numpy())
            if len(train_positions) < self.config.minimum_oof_train_rows:
                continue
            if len(validation_positions) < self.config.minimum_oof_validation_rows:
                continue
            local = TwoStageAlphaModel(self.config)
            local._fit_level_one_only(frame.iloc[train_positions], feature_columns)
            validation_x = local.encoder.transform(frame.iloc[validation_positions])
            fold_probabilities, fold_net, fold_mae, fold_mfe = local._level_one(validation_x)
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
                    "validation_start": validation_start.isoformat(),
                    "validation_end": validation_end.isoformat(),
                }
            )
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

    def fit(self, frame: pd.DataFrame, feature_columns: Sequence[str]) -> "TwoStageAlphaModel":
        if len(frame) < 50:
            raise ValueError("at least 50 pooled observations are required")
        data = frame.copy().reset_index(drop=True)
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
        meta_design = np.column_stack([np.ones(len(meta_x)), meta_x])
        utility = net[oof_valid] - self.config.tail_penalty * mae[oof_valid]
        trade_target = (utility > 0).astype(float)
        self.meta_weights = np.zeros(meta_design.shape[1], dtype=float)
        sample_weight = 1.0 + np.minimum(10.0, np.abs(utility) / max(float(np.median(np.abs(utility))) if len(utility) else 1.0, 1e-12))
        for _ in range(self.config.meta_iterations):
            probability = _sigmoid(meta_design @ self.meta_weights)
            gradient = meta_design.T @ ((probability - trade_target) * sample_weight) / len(meta_x)
            gradient[1:] += self.config.l2 * self.meta_weights[1:]
            self.meta_weights -= self.config.learning_rate * gradient
        residuals = net[oof_valid] - oof_net[oof_valid]
        self.residual_quantiles = np.quantile(residuals, [0.10, 0.50, 0.90])
        self.conformal_lower_residual = float(
            np.quantile(residuals, 1.0 - self.config.uncertainty_quantile, method="lower")
        )
        self.training_audit = {
            "level_two_training_source": "out_of_fold_level_one",
            "return_calibration_source": "out_of_fold_residuals",
            "calibration_method": "oof_residual_quantiles_and_one_sided_conformal",
            "pit_label_cutoff_enforced": True,
            "training_rows": len(data),
            "oof_rows": int(oof_valid.sum()),
            "oof_fold_count": len(oof_audit),
            "oof_folds": oof_audit,
        }
        self.encoder = _Encoder()
        self._fit_level_one_only(data, feature_columns)
        self.fitted = True
        self.release_stage = "rejected"
        return self

    def _level_one(self, x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        if any(value is None for value in (self.direction_weights, self.net_weights, self.mae_weights, self.mfe_weights)):
            raise RuntimeError("model is not fitted")
        design = np.column_stack([np.ones(len(x)), x])
        probabilities = _softmax(design @ self.direction_weights)
        expected_net = _ridge_predict(x, self.net_weights)
        mae = np.maximum(0.0, _ridge_predict(x, self.mae_weights))
        mfe = np.maximum(0.0, _ridge_predict(x, self.mfe_weights))
        return probabilities, expected_net, mae, mfe

    def _meta_features(self, x: np.ndarray) -> np.ndarray:
        probabilities, expected_net, mae, mfe = self._level_one(x)
        return self._compose_meta_features(probabilities, expected_net, mae, mfe)

    def predict(self, frame: pd.DataFrame) -> list[TwoStagePrediction]:
        if not self.fitted or self.meta_weights is None:
            raise RuntimeError("model is not fitted")
        x = self.encoder.transform(frame)
        probabilities, expected_net, mae, mfe = self._level_one(x)
        meta_x = self._meta_features(x)
        trade_probability = _sigmoid(np.column_stack([np.ones(len(meta_x)), meta_x]) @ self.meta_weights)
        entropy = meta_x[:, -1]
        conformal_lower = expected_net + self.conformal_lower_residual
        p10 = expected_net + self.residual_quantiles[0]
        p50 = expected_net + self.residual_quantiles[1]
        p90 = expected_net + self.residual_quantiles[2]
        output: list[TwoStagePrediction] = []
        for index in range(len(frame)):
            lower_edge = float(min(p10[index], conformal_lower[index]))
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

    def save(self, path: Path) -> None:
        if not self.fitted:
            raise RuntimeError("cannot save an unfitted model")
        payload = {
            "model_family": "profitability_two_stage",
            "release_stage": "rejected",
            "config": asdict(self.config),
            "feature_columns": self.feature_columns,
            "encoder": self.encoder.state(),
            "direction_weights": self.direction_weights.tolist(),
            "net_weights": self.net_weights.tolist(),
            "mae_weights": self.mae_weights.tolist(),
            "mfe_weights": self.mfe_weights.tolist(),
            "meta_weights": self.meta_weights.tolist(),
            "residual_quantiles": self.residual_quantiles.tolist(),
            "conformal_lower_residual": self.conformal_lower_residual,
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
        model.encoder = _Encoder.from_state(payload["encoder"])
        for name in (
            "direction_weights",
            "net_weights",
            "mae_weights",
            "mfe_weights",
            "meta_weights",
            "residual_quantiles",
        ):
            setattr(model, name, np.asarray(payload[name], dtype=float))
        model.conformal_lower_residual = float(
            payload.get("conformal_lower_residual", model.residual_quantiles[0])
        )
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


__all__: Sequence[str] = ("TwoStageAlphaModel", "TwoStageConfig", "TwoStagePrediction")
