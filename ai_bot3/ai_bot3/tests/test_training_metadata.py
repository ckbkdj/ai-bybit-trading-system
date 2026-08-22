"""训练元数据落盘 / 读取 / 合并到预测结果的语义测试（同步、合成数据）。"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.result_manager import ResultManager


def test_save_and_load_training_metadata_roundtrip():
    """训练元数据先落盘，再被 ResultManager 读出且字段保留。"""
    with tempfile.TemporaryDirectory() as tmp:
        rm = ResultManager(Path(tmp))
        meta = {
            "symbol": "BTCUSDT",
            "mode": "scalping",
            "training_started_at": "2026-05-06T00:00:00",
            "training_finished_at": "2026-05-06T00:01:23",
            "training_duration_sec": 83.0,
            "training_mode_time": {
                "timeframe": "3m", "window": 180, "horizon": 1, "samples": 500
            },
            "validation_direction_acc": 0.55,
            "validation_rmse_return": 0.0007,
            "feature_columns": ["open", "close", "news_sentiment"],
            "news_training_summary": {
                "feature_columns": ["news_sentiment", "fear_greed_score"],
                "feature_count": 2,
                "weight_policy": "adaptive_context_weights(completeness gating)",
            },
        }
        rm.save_training_metadata("BTCUSDT", "scalping", meta)
        loaded = rm.load_training_metadata("BTCUSDT", "scalping")
        assert loaded is not None
        assert loaded["training_duration_sec"] == 83.0
        assert loaded["training_mode_time"]["window"] == 180
        assert loaded["news_training_summary"]["feature_count"] == 2


def test_save_result_merges_training_metadata():
    """save_result 应把同 symbol/mode 的训练元数据自动合并到预测 JSON。"""
    with tempfile.TemporaryDirectory() as tmp:
        rm = ResultManager(Path(tmp))
        # 先落盘训练元数据
        rm.save_training_metadata("ETHUSDT", "trend", {
            "training_duration_sec": 12.5,
            "training_mode_time": {"timeframe": "2h", "window": 240, "horizon": 1, "samples": 800},
        })
        # 再保存预测结果
        pred = {
            "trend": "up", "pred": 3500.0, "last": 3450.0,
            "ci": [3490.0, 3510.0], "score": 0.8, "rmse": 5.0,
            "predicted_return": 0.014,
        }
        asyncio.run(rm.save_result("ETHUSDT", "trend", pred))
        # 读回检查
        path = Path(tmp) / "ETHUSDT_trend.json"
        assert path.exists()
        merged = json.loads(path.read_text())
        assert merged["training_metadata"]["training_duration_sec"] == 12.5
        assert merged["training_metadata"]["training_mode_time"]["timeframe"] == "2h"
        # 默认补齐字段
        assert "raw_trend" in merged
        assert "calibrated_trend" in merged


def test_get_latest_results_skips_training_files():
    """聚合最新预测时，应跳过 _training.json 训练元数据文件。"""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # 一份预测
        (d / "BTC_scalping.json").write_text(json.dumps({
            "trend": "up", "pred": 1.0, "last": 0.9, "score": 0.5, "rmse": 0.01,
        }))
        # 同名训练元数据，不应进入聚合
        (d / "BTC_scalping_training.json").write_text(json.dumps({
            "training_duration_sec": 9.9,
        }))
        rm = ResultManager(d)
        latest = rm.get_latest_results()
        assert "BTC" in latest
        assert "scalping" in latest["BTC"]["details"]
        # 聚合维度不会出现 _training 后缀的伪 mode
        assert "scalping_training" not in latest["BTC"]["details"]
