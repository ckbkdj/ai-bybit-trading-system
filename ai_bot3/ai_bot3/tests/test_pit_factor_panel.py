from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.training.pit_factor_panel import TradPanelHistorySource


def _service(tmp_path: Path) -> Path:
    root = tmp_path / "data_service"
    panel = root / "data" / "canonical" / "panel.parquet"
    panel.parent.mkdir(parents=True)
    (root / "config").mkdir()
    runs = root / "operations" / "runs"
    runs.mkdir(parents=True)
    (root / "config" / "service.json").write_text(
        json.dumps({"TRAD_SERVICE_CANONICAL_PANEL": "data/canonical/panel.parquet"}),
        encoding="utf-8",
    )
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for day in range(6):
        for symbol, offset in (("GLD.US", 0.0), ("USO.US", 100.0)):
            rows.append(
                {
                    "symbol": symbol,
                    "ts": start + timedelta(days=day),
                    "close": 100.0 + offset + day,
                    "asset_family": "ignored",
                }
            )
    pq.write_table(pa.Table.from_pylist(rows), panel)
    digest = hashlib.sha256(panel.read_bytes()).hexdigest()
    (runs / "pass.json").write_text(
        json.dumps(
            {
                "run_id": "pass",
                "status": "PASS",
                "finished_at": "2025-02-01T00:00:00Z",
                "canonical_sha_after": digest,
            }
        ),
        encoding="utf-8",
    )
    return root


def test_trad_panel_history_joins_only_values_available_before_crypto_decision(tmp_path):
    source = TradPanelHistorySource(
        _service(tmp_path),
        instruments={"gld_return": "GLD.US", "uso_return": "USO.US"},
        availability_lag=timedelta(hours=30),
        verify_sha256=True,
    )
    history, evidence = source.load()
    assert evidence["hash_verified"] is True
    assert evidence["selection_policy"] == "explicit_symbol_allowlist_base_prices_only"
    assert evidence["maximum_age_seconds"] == 7 * 24 * 60 * 60
    assert history["available_at"].is_monotonic_increasing

    decisions = pd.DataFrame(
        {
            "decision_at": [
                datetime(2025, 1, 3, 5, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 7, tzinfo=timezone.utc),
            ]
        }
    )
    joined = source.join(decisions, history=history)
    assert pd.isna(joined.loc[0, "gld_return"])
    assert joined.loc[1, "gld_return"] == (101.0 / 100.0 - 1.0)
    assert joined.loc[1, "factor_available_at"] <= joined.loc[1, "decision_at"]


def test_trad_panel_history_does_not_carry_stale_returns_forward(tmp_path):
    source = TradPanelHistorySource(
        _service(tmp_path),
        instruments={"gld_return": "GLD.US"},
        availability_lag=timedelta(hours=30),
        maximum_age=timedelta(days=7),
        verify_sha256=True,
    )
    history, _ = source.load()
    latest_available = history["available_at"].max().to_pydatetime()
    decisions = pd.DataFrame(
        {"decision_at": [latest_available + timedelta(days=7, seconds=1)]}
    )

    joined = source.join(decisions, history=history)

    assert pd.isna(joined.loc[0, "factor_available_at"])
    assert pd.isna(joined.loc[0, "gld_return"])
