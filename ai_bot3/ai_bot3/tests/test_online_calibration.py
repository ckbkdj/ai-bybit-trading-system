"""core/online_calibration.py 测试。"""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.online_calibration import OnlinePredictionCalibrator


def _cfg(db_path: Path):
    return {
        "online_learning": {
            "enabled": True,
            "db_path": str(db_path),
            "lookback": 100,
            "min_samples": 3,
            "base_threshold": 0.001,
        }
    }


def test_record_settle_calibrate_cycle():
    with tempfile.TemporaryDirectory() as tmp:
        cal = OnlinePredictionCalibrator(_cfg(Path(tmp) / "ol.sqlite3"))
        # 记录三条预测：方向都偏正，但实际偏负 -> bias 应被学到
        for _ in range(3):
            cal.record("BTCUSDT", "3m", "scalping", 0.005, 100.0, 60)
            cal._conn.execute(
                "UPDATE predictions SET settle_at = strftime('%s','now') - 1 WHERE settled=0"
            )
        cal._conn.commit()

        def actual(symbol, tf, last_price, settle_at):
            return -0.003

        n = cal.settle_due(actual)
        assert n == 3
        adj = cal.calibrate("BTCUSDT", "3m", "scalping", 0.005, 100.0)
        assert "raw_predicted_return" in adj
        assert "calibrated_predicted_return" in adj
        assert "online_learning" in adj
        # 校准后 calibrated_predicted_return 应小于 raw（因为 bias 是正的）
        assert adj["calibrated_predicted_return"] <= adj["raw_predicted_return"]
        cal.close()


def test_disabled_returns_passthrough():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = _cfg(Path(tmp) / "ol.sqlite3")
        cfg["online_learning"]["enabled"] = False
        cal = OnlinePredictionCalibrator(cfg)
        adj = cal.calibrate("BTCUSDT", "3m", "scalping", 0.01, 100.0)
        # 禁用时 raw == calibrated
        assert abs(adj["raw_predicted_return"] - adj["calibrated_predicted_return"]) < 1e-12
        cal.close()
