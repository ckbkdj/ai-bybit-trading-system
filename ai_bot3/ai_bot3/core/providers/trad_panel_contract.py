"""Scope-bound evidence contract for external daily market prices.

The external data service contains thousands of columns and may legitimately
fail a full-panel audit because an unrelated derived factor is stale.  The
profitability alpha consumes only allow-listed market closes, so its contract
is deliberately narrower and stricter:

* the promoted canonical file and its predecessor baseline must match the
  before/after hashes in the successful promotion receipt;
* every allow-listed baseline price must remain numerically identical in the
  canonical panel;
* new rows may only extend each symbol after its previous last timestamp -- a
  historical backfill or rewrite is rejected;
* a SHA-bound external audit must exist, and any finding that affects panel
  keys or base prices fails the scoped contract;
* no derived column from the external panel is read or trusted.

This does not turn mutable macro/fundamental snapshots into PIT data.  It is a
market-price-only contract for simple returns whose price levels are excluded
from the model.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


BASE_PRICE_COLUMNS = frozenset({"symbol", "ts", "close"})


def configured_panel_path(
    root: Path,
    *,
    config_key: str,
    default_relative: str,
) -> Path:
    """Resolve a panel path without accepting an implicit working directory."""

    service_root = Path(root).expanduser().resolve()
    relative = Path(default_relative)
    config_path = service_root / "config" / "service.json"
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        configured = payload.get(config_key)
        if configured:
            relative = Path(str(configured))
    except (OSError, ValueError, TypeError):
        pass
    return (
        relative.resolve()
        if relative.is_absolute()
        else (service_root / relative).resolve()
    )


def _read_prices(
    path: Path,
    symbols: Sequence[str],
    *,
    extra_columns: Sequence[str] = (),
) -> pd.DataFrame:
    try:
        import pyarrow.dataset as ds
    except ImportError as exc:  # pragma: no cover - minimal installs
        raise RuntimeError("pyarrow is required for external market prices") from exc

    if not path.is_file():
        raise FileNotFoundError(path)
    dataset = ds.dataset(str(path), format="parquet")
    columns = list(dict.fromkeys((*BASE_PRICE_COLUMNS, *extra_columns)))
    if missing := sorted(set(columns).difference(dataset.schema.names)):
        raise RuntimeError(f"external panel missing columns: {missing}")
    table = dataset.to_table(
        columns=columns,
        filter=ds.field("symbol").isin(sorted(set(symbols))),
    )
    frame = table.to_pandas()
    frame["symbol"] = frame["symbol"].astype(str)
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    invalid = frame["ts"].isna() | ~np.isfinite(frame["close"]) | (frame["close"] <= 0)
    if invalid.any():
        raise RuntimeError("external base-price subset contains invalid timestamps or closes")
    if frame.duplicated(["symbol", "ts"]).any():
        raise RuntimeError("external base-price subset contains duplicate symbol timestamps")
    present = set(frame["symbol"])
    if missing_symbols := sorted(set(symbols).difference(present)):
        raise RuntimeError(f"external base-price subset is missing symbols: {missing_symbols}")
    return frame.sort_values(["symbol", "ts"]).reset_index(drop=True)


def _matching_audit(root: Path, canonical_sha256: str) -> tuple[Path, dict[str, Any]]:
    audit_root = root / "operations" / "audit_work" / "history" / "panels"
    candidates = list(audit_root.glob(f"*/sha_{canonical_sha256}/manifest.json"))
    if not candidates:
        raise RuntimeError("external panel has no SHA-bound quality audit manifest")
    records: list[tuple[pd.Timestamp, Path, dict[str, Any]]] = []
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if str(payload.get("panel_sha256") or "") != canonical_sha256:
                continue
            created = pd.to_datetime(
                payload.get("created_at_local"), utc=True, errors="coerce"
            )
            if pd.isna(created):
                created = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
            records.append((created, path, payload))
        except (OSError, ValueError, TypeError):
            continue
    if not records:
        raise RuntimeError("external panel quality audit is not bound to the canonical SHA")
    _, path, payload = max(records, key=lambda item: item[0])
    return path, payload


def _base_relevant_issue(issue: Mapping[str, Any]) -> bool:
    affected = {
        str(value).strip().lower()
        for value in (issue.get("affected_columns") or ())
        if str(value).strip()
    }
    if affected.intersection(BASE_PRICE_COLUMNS):
        return True
    if affected:
        return False
    category = str(issue.get("category") or "").strip().lower()
    text = " ".join(
        str(issue.get(key) or "")
        for key in ("issue_id", "title", "description", "likely_root_cause")
    ).lower()
    category_tokens = ("fresh", "key", "ohlcv", "base_price", "market_session")
    text_tokens = (
        "duplicate symbol",
        "duplicate key",
        "base price",
        "base-price",
        "missing market row",
        "market freshness",
        "timestamp null",
    )
    return any(token in category for token in category_tokens) or any(
        token in text for token in text_tokens
    )


def verify_scoped_base_price_audit(
    root: Path,
    canonical_sha256: str,
) -> dict[str, object]:
    """Re-read the SHA-bound audit so a later finding invalidates cached data."""

    service_root = Path(root).expanduser().resolve()
    audit_path, audit = _matching_audit(service_root, canonical_sha256)
    issues = [item for item in (audit.get("issues") or ()) if isinstance(item, Mapping)]
    relevant_issues = [item for item in issues if _base_relevant_issue(item)]
    if relevant_issues:
        issue_ids = [str(item.get("issue_id") or "UNKNOWN") for item in relevant_issues]
        raise RuntimeError(
            f"SHA-bound audit reports base-price contract issues: {issue_ids}"
        )
    return {
        "audit_manifest": str(audit_path),
        "full_panel_audit_status": audit.get("audit_status"),
        "scoped_base_price_audit_status": "PASS",
        "full_panel_issue_count": len(issues),
        "base_price_issue_count": 0,
    }


def load_revision_controlled_prices(
    root: Path,
    *,
    canonical_path: Path,
    receipt: Mapping[str, Any],
    canonical_sha256: str,
    baseline_sha256: str,
    symbols: Sequence[str],
    extra_columns: Sequence[str] = (),
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Load allow-listed closes only after their append-only evidence passes."""

    service_root = Path(root).expanduser().resolve()
    baseline_path = configured_panel_path(
        service_root,
        config_key="TRAD_SERVICE_BASELINE_PANEL",
        default_relative="data/baseline/panel.parquet",
    )
    expected_after = str(receipt.get("canonical_sha_after") or "")
    expected_before = str(receipt.get("canonical_sha_before") or "")
    if not expected_after or canonical_sha256 != expected_after:
        raise RuntimeError("canonical panel hash does not match its PASS receipt")
    if not expected_before or baseline_sha256 != expected_before:
        raise RuntimeError("baseline panel hash does not match the PASS receipt predecessor")

    canonical = _read_prices(
        canonical_path,
        symbols,
        extra_columns=extra_columns,
    )
    baseline = _read_prices(baseline_path, symbols)
    overlap = baseline.merge(
        canonical[["symbol", "ts", "close"]],
        on=["symbol", "ts"],
        how="left",
        suffixes=("_baseline", "_canonical"),
        indicator=True,
    )
    if (overlap["_merge"] != "both").any():
        raise RuntimeError("canonical panel removed an allow-listed baseline price")
    changed = ~np.isclose(
        overlap["close_baseline"].to_numpy(dtype=float),
        overlap["close_canonical"].to_numpy(dtype=float),
        rtol=0.0,
        atol=1e-12,
    )
    if changed.any():
        raise RuntimeError("canonical panel rewrote an allow-listed baseline price")

    baseline_keys = baseline[["symbol", "ts"]]
    additions = canonical.merge(
        baseline_keys,
        on=["symbol", "ts"],
        how="left",
        indicator=True,
    )
    additions = additions[additions["_merge"] == "left_only"].copy()
    baseline_max = baseline.groupby("symbol")["ts"].max()
    if not additions.empty:
        previous_end = additions["symbol"].map(baseline_max)
        if (additions["ts"] <= previous_end).any():
            raise RuntimeError("canonical panel backfilled an allow-listed historical price")

    audit_evidence = verify_scoped_base_price_audit(service_root, canonical_sha256)

    evidence: dict[str, object] = {
        "baseline_path": str(baseline_path),
        "baseline_sha256": baseline_sha256,
        "canonical_sha256": canonical_sha256,
        "receipt_predecessor_hash_verified": True,
        "append_only_revision_verified": True,
        "overlapping_price_rows_verified": int(len(overlap)),
        "appended_price_rows": int(len(additions)),
        "historical_backfill_rows": 0,
        "price_level_used_as_feature": False,
        "return_transform": "simple_return_from_adjacent_market_closes",
        "external_derived_columns_trusted": False,
        **audit_evidence,
    }
    return canonical, evidence


__all__ = [
    "BASE_PRICE_COLUMNS",
    "configured_panel_path",
    "load_revision_controlled_prices",
    "verify_scoped_base_price_audit",
]
