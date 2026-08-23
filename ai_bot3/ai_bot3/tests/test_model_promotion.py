from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.promote_brain_model import main


def test_brain_promotion_is_disabled_and_preserves_candidate_artifact():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        model_dir = root / "brain"
        candidate_dir = model_dir / "candidate"
        candidate_dir.mkdir(parents=True)
        candidate_model = candidate_dir / "BTCUSDT_scalping_brain.joblib"
        candidate_meta = candidate_dir / "BTCUSDT_scalping_brain.meta.json"
        candidate_model.write_bytes(b"preserved-old-candidate")
        candidate_meta.write_text('{"release_stage":"candidate"}', encoding="utf-8")
        original_model = candidate_model.read_bytes()
        original_meta = candidate_meta.read_bytes()
        evidence = root / "evidence.json"
        evidence.write_text("{}", encoding="utf-8")
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
            with pytest.raises(SystemExit, match="Brain promotion is disabled"):
                main()
        assert candidate_model.read_bytes() == original_model
        assert candidate_meta.read_bytes() == original_meta
        live_model = model_dir / "live" / "BTCUSDT_scalping_brain.joblib"
        live_meta = model_dir / "live" / "BTCUSDT_scalping_brain.meta.json"
        assert not live_model.exists()
        assert not live_meta.exists()
