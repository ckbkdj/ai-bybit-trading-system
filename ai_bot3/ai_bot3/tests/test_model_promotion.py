from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import joblib


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.brain_model import brain_stage_artifact_paths, load_brain_bundle
from scripts.promote_brain_model import main


def test_candidate_promotion_requires_evidence_and_live_loader_uses_promoted_artifact():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        model_dir = root / "brain"
        cfg = {"brain_model": {"model_dir": str(model_dir), "inference_stage": "live"}}
        candidate_model, candidate_meta = brain_stage_artifact_paths(
            "BTCUSDT", "scalping", "candidate", cfg
        )
        joblib.dump({"model": "synthetic", "feature_columns": [], "meta": {}}, candidate_model)
        candidate_meta.write_text(
            json.dumps({"promote_decision": "candidate", "release_stage": "candidate"}),
            encoding="utf-8",
        )
        evidence = root / "evidence.json"
        evidence.write_text(
            json.dumps(
                {
                    "testnet_completed": True,
                    "shadow_observation_days": 30,
                    "cost_adjusted_out_of_sample_positive": True,
                    "max_drawdown_within_approved_limit": True,
                    "operator_kill_switch_drill": True,
                }
            ),
            encoding="utf-8",
        )
        with patch.object(
            sys,
            "argv",
            [
                "promote_brain_model.py",
                "BTCUSDT",
                "scalping",
                "--evidence",
                str(evidence),
                "--approval-id",
                "approval-test-001",
                "--model-dir",
                str(model_dir),
            ],
        ):
            assert main() == 0
        loaded = load_brain_bundle("BTCUSDT", "scalping", cfg)
        assert loaded is not None
        assert loaded["meta"]["release_stage"] == "live"
        assert loaded["meta"]["approval_id"] == "approval-test-001"
