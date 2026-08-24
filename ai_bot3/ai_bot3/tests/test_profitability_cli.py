from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_profitability_rebuild


def test_cli_passes_separate_lockbox_store_without_opening_it(tmp_path, monkeypatch):
    captured = {}
    sealed_store = tmp_path / "sealed.sqlite3"

    class Result:
        passed = False

        @staticmethod
        def to_dict():
            return {"profitability_gate": "FAILED"}

    class Runner:
        def __init__(self, config):
            captured["config"] = config

        @staticmethod
        def run():
            return Result()

    monkeypatch.setenv("BYBIT_LOCKBOX_PUBLIC_PIT_STORE", str(sealed_store))
    monkeypatch.setattr(run_profitability_rebuild, "_local_head_commit", lambda: "1" * 40)
    monkeypatch.setattr(run_profitability_rebuild, "ProfitabilityRebuild", Runner)
    monkeypatch.setattr(sys, "argv", ["run_profitability_rebuild.py"])

    assert run_profitability_rebuild.main() == 2
    assert captured["config"].lockbox_bybit_pit_store_path == sealed_store
    assert captured["config"].walk_forward_folds == 6
    assert not sealed_store.exists()
