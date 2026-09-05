"""3年K线训练与防未来函数约束测试。"""

from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
import pytest

ROOT = Path(__file__).resolve().parent.parent


def test_repository_training_policy_uses_about_three_year_kline_limits():
    policy_path = ROOT / "tests" / "fixtures" / "training_policy.yml"
    cfg = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
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
    assert ast.unparse(kwargs["data"]) == "X_scaled[:boundary.train_end - 1]"
    assert ast.unparse(kwargs["targets"]) == "y_scaled[window:boundary.train_end]"
    fit_calls = [
        n for n in calls
        if isinstance(n.func, ast.Attribute) and n.func.attr == "fit"
    ]
    model_fit = next(n for n in fit_calls if isinstance(n.func.value, ast.Name) and n.func.value.id == "model")
    assert ast.unparse(model_fit.args[0]) == "train_ds"
    fit_kwargs = {kw.arg: kw.value for kw in model_fit.keywords}
    assert ast.unparse(fit_kwargs["validation_data"]) == "validation_ds"


def test_brain_kline_features_have_identical_lag_in_training_and_inference():
    try:
        from core.brain_model import build_brain_features
    except ModuleNotFoundError as exc:
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
    training = build_brain_features(df, "scalping", "BTCUSDT", cfg=cfg)
    inference = build_brain_features(df, "scalping", "BTCUSDT", market_snapshot={}, cfg=cfg)
    # Merely supplying a live snapshot cannot change the OHLCV time alignment.
    for col in ["ret_1", "ret_12", "volume_zscore", "atr_pct", "trend_strength"]:
        assert np.allclose(training[col], inference[col])
    for col in ["snap_funding_rate", "snap_long_short_ratio", "snap_liquidation_imbalance"]:
        assert float(training[col].abs().max()) == 0.0

    live_cfg = {"brain_model": {"historical_kline_only": False, "anti_leakage_shift_features": 1}}
    live = build_brain_features(
        df,
        "scalping",
        "BTCUSDT",
        market_snapshot={"funding_rate": 0.001, "long_short_ratio": 1.2},
        cfg=live_cfg,
    )
    assert np.allclose(training["ret_1"], live["ret_1"])
    assert np.isclose(live["snap_funding_rate"].iloc[-1], 0.001)


def test_trainer_declares_kline_only_no_news_policy():
    text = (ROOT / "core" / "trainer3.py").read_text(encoding="utf-8")
    assert "no_news_kline_only_training" in text
    assert "ohlcv_kline_only" in text
    assert "disabled_for_training_anti_leakage" in text
    assert "ret_1" in text and "atr_pct" in text and "boll_pos" in text


def test_online_calibration_is_applied_after_real_model_return_exists():
    inferencer = (ROOT / "core" / "inferencer3_fixed.py").read_text(encoding="utf-8")
    portfolio = (ROOT / "core" / "portfolio3_3_fixed.py").read_text(encoding="utf-8")
    assert "tentative_return" not in inferencer
    assert 'float(result.get("predicted_return") or 0.0)' in portfolio


def test_live_feature_contract_rejects_missing_trained_columns():
    from core.kline_feature_store import FeatureContractError, select_persisted_features

    frame = pd.DataFrame({"ret_1": [0.1], "atr_pct": [0.02]})
    with pytest.raises(FeatureContractError, match="missing 1 trained features"):
        select_persisted_features(frame, ["ret_1", "atr_pct", "mtf_15m_ret_1"])
