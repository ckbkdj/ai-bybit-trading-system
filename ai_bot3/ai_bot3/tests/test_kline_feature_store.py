from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.kline_feature_store import KlineFeatureStore, ModeSpec


def cfg():
    return {
        "general": {"symbols": ["BTCUSDT", "ETHUSDT"], "cache_days": 1098},
        "modes": {
            "scalping": ["5m", 1000, 32],
            "mid": ["15m", 1000, 32],
            "hour": ["1h", 1000, 32],
        },
        "training": {"multi_timeframe": {"enabled": True}, "min_new_rows_for_retrain": 3},
        "brain_model": {"horizons": {"scalping": 1, "mid": 2, "hour": 1}},
    }


def raw(start: str, periods: int, freq: str, base: float = 100.0):
    ts = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    close = base + np.arange(periods, dtype=float)
    return pd.DataFrame({
        "ts": ts,
        "open": close - 0.2,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 1000 + np.arange(periods, dtype=float),
    })


def spec_for(store, symbol="BTCUSDT", mode="scalping"):
    return store.spec_for(symbol, mode)


def test_per_symbol_timeframe_isolation(tmp_path):
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", cfg())
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 20, "5min", 100))
    store.upsert_raw_frame("BTCUSDT", "15m", raw("2024-01-01", 10, "15min", 200))
    store.upsert_raw_frame("ETHUSDT", "5m", raw("2024-01-01", 7, "5min", 300))
    assert len(store.load_raw_frame("BTCUSDT", "5m")) == 20
    assert len(store.load_raw_frame("BTCUSDT", "15m")) == 10
    assert len(store.load_raw_frame("ETHUSDT", "5m")) == 7


def test_signatures_differ_by_mode_and_timeframe(tmp_path):
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", cfg())
    for tf, freq in [("5m", "5min"), ("15m", "15min"), ("1h", "1h")]:
        store.upsert_raw_frame("BTCUSDT", tf, raw("2024-01-01", 80, freq))
    sigs = []
    for mode in ["scalping", "mid", "hour"]:
        s = spec_for(store, mode=mode)
        for tf in s.timeframe_set:
            store.update_enhanced_kline("BTCUSDT", tf, s)
        sigs.append(store.build_mode_dataset(s).signature.digest())
    assert len(set(sigs)) == 3


def test_multi_timeframe_asof_uses_only_closed_higher_candle(tmp_path):
    c = cfg(); c["modes"] = {"scalping": ["5m", 100, 8], "hour": ["1h", 100, 8]}
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01 09:00", 40, "5min", 100))
    # 1h open 09:00 closes 10:00; 10:00 closes 11:00. 10:05 base must use close_time 10:00, not 11:00.
    store.upsert_raw_frame("BTCUSDT", "1h", raw("2024-01-01 09:00", 3, "1h", 1000))
    s = spec_for(store, mode="scalping")
    for tf in s.timeframe_set:
        store.update_enhanced_kline("BTCUSDT", tf, s)
    ff = store.build_feature_frame(s, include_mtf=True)
    target_ms = int(pd.Timestamp("2024-01-01 10:05", tz="UTC").timestamp() * 1000)
    row = ff.loc[ff["open_time"] == target_ms].iloc[0]
    assert row["mtf_1h_source_close_time"] <= row["close_time"]
    assert row["mtf_1h_source_close_time"] == int(pd.Timestamp("2024-01-01 10:00", tz="UTC").timestamp() * 1000)


def test_shifted_feature_and_future_label_alignment(tmp_path):
    c = cfg(); c["training"]["multi_timeframe"]["enabled"] = False
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 90, "5min", 100))
    s = spec_for(store, mode="scalping")
    store.update_enhanced_kline("BTCUSDT", "5m", s)
    unshifted = store.load_enhanced_frame("BTCUSDT", "5m", s)
    ds = store.build_mode_dataset(s)
    first = ds.df.iloc[0]
    prev_rows = unshifted[unshifted["close_time"] < first["close_time"]]
    assert len(prev_rows) > 0
    assert np.isclose(first["ret_1"], prev_rows.iloc[-1]["ret_1"], equal_nan=False)
    next_close = ds.df.iloc[1]["close"]
    assert np.isclose(first["future_return"], next_close / first["close"] - 1.0)


def test_forbidden_context_columns_rejected(tmp_path):
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", cfg())
    with pytest.raises(ValueError):
        store._validate_feature_columns(["ret_1", "news_sentiment"])


def test_should_train_skips_unchanged_signature_with_existing_model(tmp_path):
    c = cfg(); c["training"]["multi_timeframe"]["enabled"] = False
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 90, "5min", 100))
    s = spec_for(store, mode="scalping")
    store.update_enhanced_kline("BTCUSDT", "5m", s)
    built = store.build_mode_dataset(s)
    model = tmp_path / "model.keras"; model.write_text("x")
    store.record_model("lstm", s, built.signature, str(model), "trained", "ok")
    should, reason = store.should_train("lstm", s, built.signature, str(model), new_rows=0)
    assert should is False
    assert reason == "skipped_same_signature"


def test_enhanced_upsert_does_not_duplicate(tmp_path):
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", cfg())
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 30, "5min", 100))
    s = spec_for(store, mode="scalping")
    store.update_enhanced_kline("BTCUSDT", "5m", s)
    store.update_enhanced_kline("BTCUSDT", "5m", s)
    with sqlite3.connect(tmp_path / "kfs.sqlite") as con:
        n = con.execute("SELECT COUNT(*) FROM enhanced_kline WHERE symbol='BTCUSDT' AND timeframe='5m'").fetchone()[0]
    assert n == 30


def test_enhanced_tail_overlap_recomputes_only_tail_after_initial_build(tmp_path):
    c = cfg(); c["training"]["multi_timeframe"]["enabled"] = False; c["training"]["feature_overlap_rows"] = 10
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 100, "5min", 100))
    s = spec_for(store, mode="scalping")
    first_upsert = store.update_enhanced_kline("BTCUSDT", "5m", s)
    assert first_upsert == 100
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01 08:20", 3, "5min", 200))
    second_upsert = store.update_enhanced_kline("BTCUSDT", "5m", s)
    # overlap_rows = max(feature_overlap_rows=10, lookback_window=32, 256)+1 => 257,
    # but only 103 raw rows exist, so all are recomputed. Use a larger initial set to assert bounded tail.
    assert second_upsert == 103


def test_enhanced_tail_overlap_bounded_with_large_history(tmp_path):
    c = cfg(); c["training"]["multi_timeframe"]["enabled"] = False; c["training"]["feature_overlap_rows"] = 10
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 400, "5min", 100))
    s = spec_for(store, mode="scalping")
    assert store.update_enhanced_kline("BTCUSDT", "5m", s) == 400
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-02 09:20", 5, "5min", 600))
    second = store.update_enhanced_kline("BTCUSDT", "5m", s)
    assert 5 < second < 400
    with sqlite3.connect(tmp_path / "kfs.sqlite") as con:
        n = con.execute("SELECT COUNT(*) FROM enhanced_kline WHERE symbol='BTCUSDT' AND timeframe='5m'").fetchone()[0]
        meta = con.execute("SELECT overlap_rows FROM enhanced_update_meta WHERE symbol='BTCUSDT' AND timeframe='5m'").fetchone()
    assert n == 405
    assert meta[0] >= 257


def test_build_mode_dataset_excludes_raw_ohlcv_from_feature_columns(tmp_path):
    c = cfg(); c["training"]["multi_timeframe"]["enabled"] = False
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c)
    store.upsert_raw_frame("BTCUSDT", "5m", raw("2024-01-01", 90, "5min", 100))
    s = spec_for(store, mode="scalping")
    store.update_enhanced_kline("BTCUSDT", "5m", s)
    built = store.build_mode_dataset(s)
    forbidden = {"open", "high", "low", "close", "volume", "open_time", "close_time", "future_return"}
    assert not forbidden.intersection(built.feature_columns)


class FakeFetcher:
    def __init__(self):
        self.calls = []
    async def get_ohlcv(self, symbol, timeframe, limit):
        self.calls.append(("full", symbol, timeframe, limit))
        return raw("2024-01-01", 20, "5min" if timeframe == "5m" else "1h", 100)
    async def get_ohlcv_incremental(self, symbol, timeframe, since_open_time_ms, limit=1500):
        self.calls.append(("incremental", symbol, timeframe, since_open_time_ms, limit))
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])


@pytest.mark.asyncio
async def test_update_for_mode_first_full_then_incremental_fetch(tmp_path):
    c = cfg(); c["modes"] = {"scalping": ["5m", 100, 8]}; c["training"]["multi_timeframe"]["enabled"] = False
    fetcher = FakeFetcher()
    store = KlineFeatureStore(tmp_path / "kfs.sqlite", c, fetcher=fetcher)
    s = spec_for(store, mode="scalping")
    await store.update_for_mode(s)
    await store.update_for_mode(s)
    assert fetcher.calls[0][0] == "full"
    assert fetcher.calls[1][0] == "incremental"