"""3年K线训练与防未来函数约束测试。"""

from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent


def test_config_uses_about_three_year_kline_limits():
    cfg = yaml.safe_load((ROOT / "config.yml").read_text(encoding="utf-8"))
    assert int(cfg["general"]["cache_days"]) >= 1095
    expected = {
        "scalping": ("3m", 365 * 3 * 24 * 20),
        "mid_short": ("15m", 365 * 3 * 24 * 4),
        "trend": ("2h", 365 * 3 * 12),
        "trend_swing": ("4h", 365 * 3 * 6),
        "swing": ("1d", 365 * 3),
    }
    for mode, (tf, min_rows) in expected.items():
        got_tf, limit, _window = cfg["modes"][mode]
        assert got_tf == tf
        assert int(limit) >= min_rows


def test_lstm_training_dataset_is_chronological_and_next_close_aligned():
    tree = ast.parse((ROOT / "core" / "trainer3.py").read_text(encoding="utf-8"))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    ts_calls = [
        n for n in calls
        if isinstance(n.func, ast.Attribute)
        and n.func.attr == "timeseries_dataset_from_array"
    ]
    assert ts_calls, "timeseries_dataset_from_array call not found"
    call = ts_calls[0]
    kwargs = {kw.arg: kw.value for kw in call.keywords}
    assert isinstance(kwargs.get("shuffle"), ast.Constant)
    assert kwargs["shuffle"].value is False
    assert ast.unparse(kwargs["data"]) == "X_scaled[:-1]"
    assert ast.unparse(kwargs["targets"]) == "y_scaled[window:]"


def test_brain_features_are_shifted_for_training_to_avoid_future_leakage():
    try:
        from core.brain_model import build_brain_features
    except ModuleNotFoundError as exc:
        import pytest
        pytest.skip(f"brain_model optional dep missing: {exc.name}")

    idx = pd.date_range("2024-01-01", periods=90, freq="h")
    close = pd.Series(np.linspace(100, 130, len(idx)), index=idx)
    df = pd.DataFrame({
        "open": close.shift(1).fillna(close.iloc[0]),
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": np.linspace(1000, 2000, len(idx)),
    }, index=idx)
    cfg = {"brain_model": {"historical_kline_only": True, "anti_leakage_shift_features": 1}}
    shifted = build_brain_features(df, "scalping", "BTCUSDT", cfg=cfg)
    unshifted = build_brain_features(df, "scalping", "BTCUSDT", market_snapshot={}, cfg=cfg)
    # ret_1 at t in shifted training features equals unshifted ret_1 from t-1.
    assert np.isclose(shifted["ret_1"].iloc[-1], unshifted["ret_1"].iloc[-2])
    for col in ["snap_funding_rate", "snap_long_short_ratio", "snap_liquidation_imbalance"]:
        assert float(shifted[col].abs().max()) == 0.0


def test_trainer_declares_kline_only_no_news_policy():
    text = (ROOT / "core" / "trainer3.py").read_text(encoding="utf-8")
    assert "no_news_kline_only_training" in text
    assert "ohlcv_kline_only" in text
    assert "disabled_for_training_anti_leakage" in text
    assert "ret_1" in text and "atr_pct" in text and "boll_pos" in text
