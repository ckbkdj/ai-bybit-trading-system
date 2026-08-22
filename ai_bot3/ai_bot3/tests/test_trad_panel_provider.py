from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core.providers.base import ProviderStatus
from core.providers.trad_panel_provider import TradPanelProvider


def _build_service(tmp_path: Path, *, latest_status: str = "PASS") -> Path:
    root = tmp_path / "data_service"
    panel = root / "data" / "canonical" / "panel.parquet"
    panel.parent.mkdir(parents=True)
    (root / "config").mkdir()
    (root / "operations" / "runs").mkdir(parents=True)
    (root / "config" / "service.json").write_text(
        json.dumps({"TRAD_SERVICE_CANONICAL_PANEL": "data/canonical/panel.parquet"}),
        encoding="utf-8",
    )
    rows = []
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    for offset in range(25):
        # A future row must be excluded by the as-of cutoff.
        rows.append({
            "symbol": "SPY.US",
            "ts": start + timedelta(days=offset),
            "close": 100.0 + offset,
            "asset_family": "wrong_labels_are_not_selection_keys" if offset > 10 else "US_equity_sp500",
        })
    pq.write_table(pa.Table.from_pylist(rows), panel)
    panel_sha = hashlib.sha256(panel.read_bytes()).hexdigest()
    passed = {
        "run_id": "pass-run",
        "status": "PASS",
        "finished_at": "2026-08-01T00:00:00+00:00",
        "canonical_sha_after": panel_sha,
    }
    (root / "operations" / "runs" / "pass-run.json").write_text(
        json.dumps(passed), encoding="utf-8"
    )
    if latest_status != "PASS":
        blocked = {
            "run_id": "blocked-run",
            "status": latest_status,
            "finished_at": "2026-08-02T00:00:00+00:00",
            "canonical_sha_after": panel_sha,
            "message": "maintenance evidence missing",
        }
        (root / "operations" / "runs" / "blocked-run.json").write_text(
            json.dumps(blocked), encoding="utf-8"
        )
    return root


def test_provider_uses_allowlist_and_enforces_availability_lag(tmp_path: Path):
    root = _build_service(tmp_path)
    provider = TradPanelProvider(
        root,
        instruments={"spy": "SPY.US"},
        availability_lag=timedelta(hours=30),
        verify_sha256=True,
        stale_after=timedelta(days=30),
    )
    # Cutoff is 2026-07-23 18:00 UTC, so the 2026-07-24 row is invisible.
    result = provider.fetch(as_of=datetime(2026, 7, 25, tzinfo=timezone.utc))
    assert result.status == ProviderStatus.OK
    assert result.data is not None
    assert result.data["latest_observation"].startswith("2026-07-23")
    assert result.data["reported_asset_families"]["spy"] == "wrong_labels_are_not_selection_keys"
    assert result.data["selection_policy"] == "explicit_symbol_allowlist"
    assert result.data["fusion_eligible"] is False
    assert result.data["hash_verified"] is True
    assert result.data["features"]["cross_asset_spy_ret_20d"] == pytest.approx(20.0 / 102.0)


def test_latest_failed_update_degrades_but_does_not_replace_last_pass(tmp_path: Path):
    root = _build_service(tmp_path, latest_status="BLOCKED")
    provider = TradPanelProvider(
        root,
        instruments={"spy": "SPY.US"},
        stale_after=timedelta(days=30),
    )
    result = provider.fetch(as_of=datetime(2026, 7, 25, tzinfo=timezone.utc))
    assert result.status == ProviderStatus.DEGRADED
    assert result.data is not None
    assert result.data["latest_run_id"] == "blocked-run"
    assert result.data["latest_pass_run_id"] == "pass-run"
    assert any("latest update is BLOCKED" in warning for warning in result.warnings)


def test_hash_mismatch_fails_closed(tmp_path: Path):
    root = _build_service(tmp_path)
    pass_path = root / "operations" / "runs" / "pass-run.json"
    receipt = json.loads(pass_path.read_text(encoding="utf-8"))
    receipt["canonical_sha_after"] = "0" * 64
    pass_path.write_text(json.dumps(receipt), encoding="utf-8")
    result = TradPanelProvider(
        root,
        instruments={"spy": "SPY.US"},
        verify_sha256=True,
    ).fetch(as_of=datetime(2026, 7, 25, tzinfo=timezone.utc))
    assert result.status == ProviderStatus.OUTAGE
    assert result.data is None
    assert "hash does not match" in (result.error or "")
