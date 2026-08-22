"""评估摘要导出 & API 辅助函数的轻量测试。

避免拉起 FastAPI 服务本身：直接验证 ``OnlinePredictionCalibrator.export_evaluation_summary``
落盘的 JSON 结构，并验证 ``api/api_server.py`` 里的只读辅助函数能在缺文件 / 有文件
两种情况下返回安全 payload。
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

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


def test_export_evaluation_summary_writes_safe_empty_when_no_data():
    with tempfile.TemporaryDirectory() as tmp:
        cal = OnlinePredictionCalibrator(_cfg(Path(tmp) / "ol.sqlite3"))
        out_dir = Path(tmp) / "eval"
        summary = cal.export_evaluation_summary(output_dir=out_dir)
        assert isinstance(summary, dict)
        assert summary.get("settled_count") == 0
        assert summary.get("pending_count") == 0
        summary_path = out_dir / "summary.json"
        assert summary_path.exists()
        on_disk = json.loads(summary_path.read_text())
        assert on_disk.get("groups") == {}
        assert "last_updated_at" in on_disk
        cal.close()


def test_export_evaluation_summary_groups_settled_rows():
    with tempfile.TemporaryDirectory() as tmp:
        cal = OnlinePredictionCalibrator(_cfg(Path(tmp) / "ol.sqlite3"))
        # 写两条已结算 + 一条未结算
        cal.record("BTCUSDT", "3m", "scalping", 0.005, 100.0, 60)
        cal.record("BTCUSDT", "3m", "scalping", -0.004, 101.0, 60)
        cal.record("BTCUSDT", "3m", "scalping", 0.002, 102.0, 60)
        cal._conn.execute(
            "UPDATE predictions SET settle_at = strftime('%s','now') - 1 WHERE settled=0"
        )
        cal._conn.commit()

        def actual(symbol, tf, last_price, settle_at):
            return 0.004

        n = cal.settle_due(actual)
        assert n == 3
        out_dir = Path(tmp) / "eval"
        summary = cal.export_evaluation_summary(output_dir=out_dir)
        assert summary["settled_count"] == 3
        groups = summary.get("groups") or {}
        # 期望出现 BTCUSDT_scalping 这个分组
        key = "BTCUSDT_scalping"
        assert key in groups
        entry = groups[key]
        assert entry["settled_count"] == 3
        # 也要在 out_dir 落盘细分文件
        detail = out_dir / "BTCUSDT_scalping.json"
        assert detail.exists()
        on_disk = json.loads(detail.read_text())
        assert on_disk["settled_count"] == 3
        cal.close()


def _load_api_server_module():
    """Import api/api_server.py without triggering uvicorn; tolerate missing optional deps.

    Because the module imports ccxt/fastapi/etc. at top-level, we skip if any are
    missing (these are present in the venv used by the real service).
    """
    spec = importlib.util.spec_from_file_location(
        "ai_bot3_api_server", str(ROOT / "api" / "api_server.py")
    )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        import pytest
        pytest.skip(f"api_server optional dep missing: {exc.name}")
    return module


def test_api_evaluation_helpers_safe_when_missing(tmp_path, monkeypatch):
    mod = _load_api_server_module()
    # Repoint EVALUATION_DIR to a tmp empty dir.
    monkeypatch.setattr(mod, "EVALUATION_DIR", tmp_path, raising=True)
    payload = mod._read_evaluation_summary()
    assert isinstance(payload, dict)
    # safe empty payload shape
    assert payload.get("settled_count") == 0
    assert payload.get("groups") == {}
    # available flag set to False when summary.json missing
    assert payload.get("available", False) is False
    files = mod._evaluation_files_for_symbol("BTCUSDT")
    assert files == []
    detail = mod._read_evaluation_for_symbol_mode("BTCUSDT", "scalping")
    assert detail == {}


def test_api_evaluation_helpers_read_existing(tmp_path, monkeypatch):
    mod = _load_api_server_module()
    monkeypatch.setattr(mod, "EVALUATION_DIR", tmp_path, raising=True)
    summary = {
        "settled_count": 5,
        "pending_count": 1,
        "groups": {
            "BTCUSDT_scalping": {
                "symbol": "BTCUSDT",
                "mode": "scalping",
                "settled_count": 5,
                "pending_count": 1,
                "hit_rate": 0.6,
                "last_updated_at": "2026-05-13T00:00:00+00:00",
            }
        },
        "last_updated_at": "2026-05-13T00:00:00+00:00",
    }
    (tmp_path / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (tmp_path / "BTCUSDT_scalping.json").write_text(
        json.dumps(summary["groups"]["BTCUSDT_scalping"]), encoding="utf-8"
    )
    got = mod._read_evaluation_summary()
    assert got["settled_count"] == 5
    detail = mod._read_evaluation_for_symbol_mode("BTCUSDT", "scalping")
    assert detail["hit_rate"] == 0.6
    files = mod._evaluation_files_for_symbol("BTCUSDT")
    assert any(item["file"] == "BTCUSDT_scalping.json" for item in files)
