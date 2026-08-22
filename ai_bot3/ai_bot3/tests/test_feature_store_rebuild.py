from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "rebuild_kline_feature_store.py"
SPEC = importlib.util.spec_from_file_location("rebuild_kline_feature_store", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def test_rebuild_copies_raw_and_recomputes_features_without_touching_source():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "damaged.sqlite3"
        target_path = root / "rebuilt.sqlite3"
        with closing(sqlite3.connect(source)) as connection:
            connection.execute(
                """CREATE TABLE raw_kline(
                    symbol TEXT,timeframe TEXT,source TEXT,open_time INTEGER,close_time INTEGER,
                    open REAL,high REAL,low REAL,close REAL,volume REAL,fetched_at TEXT)"""
            )
            rows = [
                (
                    "BTCUSDT", "3m", "binance", index * 180_000,
                    (index + 1) * 180_000, 100 + index, 102 + index,
                    99 + index, 101 + index, 10 + index, "2026-01-01T00:00:00Z",
                )
                for index in range(80)
            ]
            connection.executemany(
                "INSERT INTO raw_kline VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows
            )
            connection.commit()

        cfg = {
            "general": {"symbols": ["BTCUSDT"], "cache_days": 30},
            "modes": {"scalping": ["3m", 80, 20]},
            "brain_model": {"horizons": {"scalping": 1}},
            "training": {"multi_timeframe": {"enabled": False}},
        }
        store = MODULE.KlineFeatureStore(target_path, cfg, source="binance")
        copied, failures = MODULE.copy_raw_store(source, store, batch_size=17)
        enhanced = MODULE.recompute_features(store)

        assert failures == []
        assert copied[0]["rows_written"] == 80
        assert enhanced[0]["rows_written"] == 80
        assert MODULE.quick_check(source) == ["ok"]
        assert MODULE.quick_check(target_path) == ["ok"]
