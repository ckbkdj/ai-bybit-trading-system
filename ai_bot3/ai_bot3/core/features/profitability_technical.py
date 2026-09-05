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

LEGACY_BRAIN_FEATURE_COLUMNS: tuple[str, ...] = (
    "log_volume",
    "upper_wick_pct",
    "lower_wick_pct",
    "ma_gap_8_21",
    "ma_gap_21_55",
    "rsi_14",
    "macd_line_pct",
    "macd_hist_pct",
    "bollinger_position_20",
    "atr_pct_14",
    "realized_vol_12",
    "realized_vol_24",
    "trend_strength_12",
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
    data["log_volume"] = np.log1p(volume)
    data["upper_wick_pct"] = (
        data["high"] - np.maximum(data["open"], data["close"])
    ) / close
    data["lower_wick_pct"] = (
        np.minimum(data["open"], data["close"]) - data["low"]
    ) / close
    data["ma_gap_8_21"] = (
        close.rolling(8, min_periods=4).mean()
        / (close.rolling(21, min_periods=8).mean() + 1e-12)
        - 1.0
    )
    data["ma_gap_21_55"] = (
        close.rolling(21, min_periods=8).mean()
        / (close.rolling(55, min_periods=16).mean() + 1e-12)
        - 1.0
    )
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    average_gain = gain.ewm(alpha=1.0 / 14.0, adjust=False, min_periods=14).mean()
    average_loss = loss.ewm(alpha=1.0 / 14.0, adjust=False, min_periods=14).mean()
    relative_strength = average_gain / (average_loss + 1e-12)
    data["rsi_14"] = 100.0 - 100.0 / (1.0 + relative_strength)
    ema_12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema_12 - ema_26
    macd_signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    data["macd_line_pct"] = macd / close
    data["macd_hist_pct"] = (macd - macd_signal) / close
    rolling_mean = close.rolling(20, min_periods=20).mean()
    rolling_std = close.rolling(20, min_periods=20).std()
    lower_band = rolling_mean - 2.0 * rolling_std
    upper_band = rolling_mean + 2.0 * rolling_std
    data["bollinger_position_20"] = (close - lower_band) / (
        upper_band - lower_band + 1e-12
    )
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - previous_close).abs(),
            (data["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    data["atr_pct_14"] = (
        true_range.ewm(alpha=1.0 / 14.0, adjust=False, min_periods=14).mean()
        / close
    )
    returns = close.pct_change()
    data["realized_vol_12"] = returns.rolling(12, min_periods=6).std()
    data["realized_vol_24"] = returns.rolling(24, min_periods=8).std()
    data["trend_strength_12"] = data["ret_12"] / (
        data["realized_vol_24"] + 1e-12
    )
    return data.replace([np.inf, -np.inf], np.nan)


__all__ = (
    "LEGACY_BRAIN_FEATURE_COLUMNS",
    "TECHNICAL_FEATURE_COLUMNS",
    "engineer_profitability_features",
)
