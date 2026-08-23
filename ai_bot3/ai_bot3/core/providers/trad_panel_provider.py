"""Read-only, point-in-time adapter for the external ``trad_data_service`` panel.

The external panel is useful as slow cross-asset context, but it is not a crypto
execution feed.  This adapter therefore has deliberately narrow behaviour:

* only four base columns are read (``symbol``, ``ts``, ``close``,
  ``asset_family``); none of the thousands of precomputed factors are trusted;
* instruments are selected by an explicit symbol allow-list because historical
  ``asset_family`` labels are not stable enough to be a trading contract;
* observations are delayed before they become available to prevent look-ahead;
* the result is always marked shadow-only until a separate PIT/OOS ablation has
  approved these columns for model training and live fusion.

No file below the external service root is ever written by this module.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .base import Provider, ProviderResult, ProviderStatus


DEFAULT_INSTRUMENTS: Dict[str, str] = {
    "spy": "SPY.US",
    "qqq": "QQQ.US",
    "tlt": "TLT.US",
    "gld": "GLD.US",
    "uso": "USO.US",
    "uup": "UUP.US",
    "gbtc": "GBTC.US",
    "coin": "COIN.US",
    "mstr": "MSTR.US",
    "xlv": "XLV.US",
    "ibb": "IBB.US",
    "fxi": "FXI.US",
    "kweb": "KWEB.US",
}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_time(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        return _utc(datetime.fromisoformat(text))
    except (TypeError, ValueError):
        return None


def _sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class PanelRunState:
    latest: Optional[Dict[str, Any]]
    latest_pass: Optional[Dict[str, Any]]


class TradPanelProvider(Provider[Dict[str, Any]]):
    """Expose lagged daily cross-asset returns from a promoted canonical panel."""

    name = "trad_data_service.canonical_panel"

    def __init__(
        self,
        root: Optional[Path] = None,
        *,
        availability_lag: timedelta = timedelta(hours=30),
        instruments: Optional[Dict[str, str]] = None,
        verify_sha256: bool | None = None,
        stale_after: timedelta = timedelta(days=7),
    ) -> None:
        configured = root or os.getenv("TRAD_DATA_SERVICE_ROOT", "").strip()
        if not configured:
            raise ValueError("TRAD_DATA_SERVICE_ROOT is not configured")
        self.root = Path(configured).expanduser().resolve()
        self.availability_lag = availability_lag
        self.instruments = dict(instruments or DEFAULT_INSTRUMENTS)
        self.verify_sha256 = (
            str(os.getenv("TRAD_PANEL_VERIFY_SHA256", "false")).strip().lower()
            in {"1", "true", "yes", "on"}
            if verify_sha256 is None
            else bool(verify_sha256)
        )
        self.stale_after = stale_after
        self._rows_cache: tuple[tuple[int, int], list[Dict[str, Any]]] | None = None
        self._verified_hash_cache: tuple[tuple[int, int], str] | None = None

    @property
    def panel_path(self) -> Path:
        config_path = self.root / "config" / "service.json"
        relative = Path("data/canonical/panel.parquet")
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            configured = payload.get("TRAD_SERVICE_CANONICAL_PANEL")
            if configured:
                candidate = Path(str(configured))
                relative = candidate if not candidate.is_absolute() else candidate
        except (OSError, ValueError, TypeError):
            pass
        return relative.resolve() if relative.is_absolute() else (self.root / relative).resolve()

    @property
    def runs_dir(self) -> Path:
        return self.root / "operations" / "runs"

    def inspect_runs(self) -> PanelRunState:
        records = []
        if self.runs_dir.exists():
            for path in self.runs_dir.glob("*.json"):
                if path.name == "latest_audit.json":
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError, TypeError):
                    continue
                finished_at = _parse_time(payload.get("finished_at"))
                if finished_at is not None:
                    records.append((finished_at, payload))
        records.sort(key=lambda item: item[0])
        latest = records[-1][1] if records else None
        passed = [payload for _, payload in records if payload.get("status") == "PASS"]
        return PanelRunState(latest=latest, latest_pass=passed[-1] if passed else None)

    def _read_rows(self, cutoff: datetime) -> list[Dict[str, Any]]:
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:  # pragma: no cover - exercised on minimal installs
            raise RuntimeError("pyarrow is required for TRAD_DATA_SERVICE_ROOT") from exc

        panel_stat = self.panel_path.stat()
        fingerprint = (int(panel_stat.st_mtime_ns), int(panel_stat.st_size))
        if self._rows_cache is not None and self._rows_cache[0] == fingerprint:
            return [row for row in self._rows_cache[1] if row["observed_at"] <= cutoff]

        dataset = ds.dataset(str(self.panel_path), format="parquet")
        required = {"symbol", "ts", "close", "asset_family"}
        missing = sorted(required.difference(dataset.schema.names))
        if missing:
            raise RuntimeError(f"canonical panel missing columns: {missing}")
        symbols = sorted(set(self.instruments.values()))
        table = dataset.to_table(
            columns=["symbol", "ts", "close", "asset_family"],
            filter=ds.field("symbol").isin(symbols),
        )
        rows = []
        for row in table.to_pylist():
            observed_at = _parse_time(row.get("ts"))
            if observed_at is None:
                continue
            try:
                close = float(row.get("close"))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(close) or close <= 0:
                continue
            rows.append({
                "symbol": str(row.get("symbol")),
                "observed_at": observed_at,
                "close": close,
                "asset_family": str(row.get("asset_family") or ""),
            })
        self._rows_cache = (fingerprint, rows)
        return [row for row in rows if row["observed_at"] <= cutoff]

    @staticmethod
    def _series_by_symbol(rows: Iterable[Dict[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
        grouped: Dict[str, Dict[datetime, Dict[str, Any]]] = {}
        for row in rows:
            symbol_rows = grouped.setdefault(row["symbol"], {})
            timestamp = row["observed_at"]
            previous = symbol_rows.get(timestamp)
            if previous is not None and not math.isclose(
                previous["close"], row["close"], rel_tol=0.0, abs_tol=1e-12
            ):
                raise RuntimeError(f"conflicting duplicate close for {row['symbol']} at {timestamp.isoformat()}")
            symbol_rows[timestamp] = row
        return {
            symbol: [by_time[key] for key in sorted(by_time)]
            for symbol, by_time in grouped.items()
        }

    def fetch(self, *, as_of: datetime) -> ProviderResult[Dict[str, Any]]:
        generated_at = datetime.now(timezone.utc)
        as_of_utc = _utc(as_of)
        cutoff = as_of_utc - self.availability_lag
        warnings = []
        try:
            panel = self.panel_path
            if not panel.is_file():
                raise RuntimeError(f"canonical panel not found: {panel}")
            run_state = self.inspect_runs()
            if run_state.latest_pass is None:
                raise RuntimeError("no successful promotion receipt found")

            expected_sha = str(run_state.latest_pass.get("canonical_sha_after") or "")
            if self.verify_sha256:
                if not expected_sha:
                    raise RuntimeError("latest PASS receipt has no canonical_sha_after")
                stat = panel.stat()
                fingerprint = (int(stat.st_mtime_ns), int(stat.st_size))
                if (
                    self._verified_hash_cache is not None
                    and self._verified_hash_cache[0] == fingerprint
                ):
                    actual_sha = self._verified_hash_cache[1]
                else:
                    actual_sha = _sha256(panel)
                    self._verified_hash_cache = (fingerprint, actual_sha)
                if actual_sha != expected_sha:
                    raise RuntimeError("canonical panel hash does not match latest PASS receipt")

            status = ProviderStatus.OK
            latest = run_state.latest
            if latest and latest.get("status") != "PASS":
                status = ProviderStatus.DEGRADED
                warnings.append(
                    f"latest update is {latest.get('status')}: {latest.get('message') or 'no message'}"
                )
                if latest.get("canonical_sha_after") not in (None, "", expected_sha):
                    raise RuntimeError("failed run reports an unexpected canonical hash")

            rows = self._read_rows(cutoff)
            grouped = self._series_by_symbol(rows)
            features: Dict[str, float] = {}
            observations: Dict[str, str] = {}
            families: Dict[str, str] = {}
            latest_observation: Optional[datetime] = None
            for alias, symbol in self.instruments.items():
                series = grouped.get(symbol, [])
                if not series:
                    warnings.append(f"missing allow-listed instrument: {symbol}")
                    status = ProviderStatus.DEGRADED
                    continue
                last = series[-1]
                latest_observation = max(latest_observation or last["observed_at"], last["observed_at"])
                observations[alias] = last["observed_at"].isoformat()
                families[alias] = last["asset_family"]
                for horizon in (1, 5, 20):
                    if len(series) <= horizon:
                        warnings.append(f"{symbol} lacks {horizon}-session history")
                        status = ProviderStatus.DEGRADED
                        continue
                    previous = series[-1 - horizon]["close"]
                    features[f"cross_asset_{alias}_ret_{horizon}d"] = last["close"] / previous - 1.0

            if latest_observation is None:
                raise RuntimeError("no allow-listed observations exist before the PIT cutoff")
            available_at = latest_observation + self.availability_lag
            if available_at > as_of_utc:
                raise RuntimeError("point-in-time availability invariant violated")
            if as_of_utc - available_at > self.stale_after:
                status = ProviderStatus.DEGRADED
                warnings.append("external panel context is stale")

            data = {
                "schema_version": "external-panel-context.v1",
                "as_of": as_of_utc.isoformat(),
                "cutoff": cutoff.isoformat(),
                "latest_observation": latest_observation.isoformat(),
                "available_at": available_at.isoformat(),
                "features": features,
                "observations": observations,
                "reported_asset_families": families,
                "latest_run_status": latest.get("status") if latest else "UNKNOWN",
                "latest_run_id": latest.get("run_id") if latest else None,
                "latest_pass_run_id": run_state.latest_pass.get("run_id"),
                "canonical_sha_from_receipt": expected_sha,
                "hash_verified": self.verify_sha256,
                "selection_policy": "explicit_symbol_allowlist",
                "fusion_eligible": False,
                "fusion_blocker": "shadow_only_pending_pit_oos_ablation",
            }
            return ProviderResult(
                status=status,
                data=data,
                generated_at=generated_at,
                source=self.name,
                warnings=tuple(warnings),
            )
        except Exception as exc:
            return ProviderResult(
                status=ProviderStatus.OUTAGE,
                data=None,
                generated_at=generated_at,
                source=self.name,
                warnings=tuple(warnings),
                error=str(exc),
            )


__all__ = ["DEFAULT_INSTRUMENTS", "PanelRunState", "TradPanelProvider"]
