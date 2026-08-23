"""Brain 发布治理元数据的轻量测试。

只测纯函数 ``_decide_promotion`` 与 ``brain_stage_paths``，避免依赖 talib /
sklearn 的完整训练流程；这两块也是 docs/final_optimized_quant_brain_plan.md
§9 中“发布状态机”的最小可验证表面。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_brain_module():
    spec = importlib.util.spec_from_file_location(
        "ai_bot3_brain_model", str(ROOT / "core" / "brain_model.py")
    )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        import pytest
        pytest.skip(f"brain_model optional dep missing: {exc.name}")
    return module


def test_brain_stage_dirs_are_created(tmp_path):
    bm = _load_brain_module()
    cfg = {"brain_model": {"model_dir": str(tmp_path / "brain")}}
    stages = bm.brain_stage_paths("BTCUSDT", "scalping", cfg)
    assert set(stages.keys()) == set(bm.BRAIN_STAGE_DIRS)
    for stage, p in stages.items():
        assert p.exists() and p.is_dir(), f"{stage} dir not created"
    # brain_paths must still point to the legacy root-level joblib (compat).
    model_path, meta_path = bm.brain_paths("BTCUSDT", "scalping", cfg)
    assert model_path.suffix == ".joblib"
    assert model_path.parent == tmp_path / "brain"


def test_decide_promotion_insufficient_samples_is_rejected_baseline():
    bm = _load_brain_module()
    metrics = {
        "validation_samples": 20,
        "direction_acc_nonflat": 0.99,
        "precision_long": 0.99,
        "precision_short": 0.99,
        "actionable_rate": 0.5,
    }
    decision, reason, baseline = bm._decide_promotion(metrics, samples=1000, min_samples=600)
    assert decision == "rejected"
    assert reason == "brain_baseline_rejected_profitability_rebuild"
    assert baseline["baseline_only"] is True
    assert baseline["profitability_evidence"] is False


def test_decide_promotion_low_direction_acc_rejected():
    bm = _load_brain_module()
    metrics = {
        "validation_samples": 200,
        "direction_acc_nonflat": 0.30,
        "precision_long": 0.30,
        "precision_short": 0.30,
        "actionable_rate": 0.10,
    }
    decision, reason, _ = bm._decide_promotion(metrics, samples=1000, min_samples=600)
    assert decision == "rejected"
    assert reason == "brain_baseline_rejected_profitability_rebuild"


def test_decide_promotion_strong_metrics_still_rejected():
    bm = _load_brain_module()
    metrics = {
        "validation_samples": 500,
        "direction_acc_nonflat": 0.60,
        "precision_long": 0.60,
        "precision_short": 0.40,
        "actionable_rate": 0.30,
    }
    decision, reason, baseline = bm._decide_promotion(metrics, samples=2000, min_samples=600)
    assert decision == "rejected"
    assert reason == "brain_baseline_rejected_profitability_rebuild"
    assert baseline["baseline_only"] is True


def test_decide_promotion_floor_above_minimum_still_rejected():
    bm = _load_brain_module()
    metrics = {
        "validation_samples": 300,
        "direction_acc_nonflat": 0.51,
        "precision_long": 0.51,
        "precision_short": 0.51,
        "actionable_rate": 0.10,
    }
    decision, reason, _ = bm._decide_promotion(metrics, samples=1500, min_samples=600)
    assert decision == "rejected"
    assert reason == "brain_baseline_rejected_profitability_rebuild"
