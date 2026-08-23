from __future__ import annotations

import numpy as np
import pandas as pd


TECHNICAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "ret_1",
    "ret_2",
    "ret_3",
    "ret_6",
    "ret_12",
    "ret_24",
    "range_pct",
    "body_pct",
    "volume_zscore",
    "momentum_vol_ratio",
    "ma_gap_8_24",
)


def engineer_profitability_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build the shared causal OHLCV feature contract for train and inference."""

    required = {"open", "high", "low", "close", "volume"}
    if missing := sorted(required.difference(frame.columns)):
        raise ValueError(f"profitability features missing OHLCV columns: {missing}")
    data = frame.copy()
    for column in required:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    close = data["close"].replace(0, np.nan)
    volume = data["volume"].clip(lower=0.0)
    for window in (1, 2, 3, 6, 12, 24):
        data[f"ret_{window}"] = close.pct_change(window)
    data["range_pct"] = (data["high"] - data["low"]) / close
    data["body_pct"] = (data["close"] - data["open"]) / data["open"].replace(0, np.nan)
    data["volume_zscore"] = (
        (volume - volume.rolling(48, min_periods=8).mean())
        / (volume.rolling(48, min_periods=8).std() + 1e-12)
    )
    data["volatility"] = close.pct_change().rolling(24, min_periods=8).std()
    data["liquidity"] = close * volume
    data["momentum_vol_ratio"] = data["ret_12"] / (data["volatility"] + 1e-12)
    data["ma_gap_8_24"] = (
        close.rolling(8, min_periods=4).mean()
        / (close.rolling(24, min_periods=8).mean() + 1e-12)
        - 1.0
    )
    return data.replace([np.inf, -np.inf], np.nan)


__all__ = ("TECHNICAL_FEATURE_COLUMNS", "engineer_profitability_features")
