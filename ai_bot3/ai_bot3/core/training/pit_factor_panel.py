from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd


TRAD_PANEL_INSTRUMENTS: Mapping[str, str] = {
    "spy_return": "SPY.US",
    "qqq_return": "QQQ.US",
    "tlt_return": "TLT.US",
    "uup_return": "UUP.US",
    "gld_return": "GLD.US",
    "uso_return": "USO.US",
    "xlv_return": "XLV.US",
    "ibb_return": "IBB.US",
    "fxi_return": "FXI.US",
    "kweb_return": "KWEB.US",
    "coin_return": "COIN.US",
    "mstr_return": "MSTR.US",
}

TRAD_PANEL_FACTOR_GROUPS: Mapping[str, tuple[str, ...]] = {
    "us_risk": ("spy_return", "qqq_return"),
    "rates_usd": ("tlt_return", "uup_return"),
    "commodities": ("gld_return", "uso_return"),
    "healthcare": ("xlv_return", "ibb_return"),
    "china": ("fxi_return", "kweb_return"),
    "crypto_equities": ("coin_return", "mstr_return"),
}

TRAD_PANEL_MISSING_REQUIRED_FACTORS: Mapping[str, tuple[str, ...]] = {
    "us_risk": ("vix_level",),
    "rates_usd": ("real_yield_10y",),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class TradPanelHistorySource:
    """Read explicit base prices and expose conservatively lagged PIT returns."""

    def __init__(
        self,
        root: Path,
        *,
        instruments: Mapping[str, str] | None = None,
        availability_lag: timedelta = timedelta(hours=30),
        maximum_age: timedelta = timedelta(days=7),
        verify_sha256: bool = True,
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self.instruments = dict(instruments or TRAD_PANEL_INSTRUMENTS)
        self.availability_lag = availability_lag
        self.maximum_age = maximum_age
        self.verify_sha256 = verify_sha256
        if availability_lag < timedelta(hours=24):
            raise ValueError("daily external prices require at least a 24-hour PIT lag")
        if maximum_age <= timedelta(0):
            raise ValueError("external price maximum age must be positive")
        if not self.instruments:
            raise ValueError("an explicit instrument allowlist is required")

    @property
    def panel_path(self) -> Path:
        relative = Path("data/canonical/panel.parquet")
        config = self.root / "config" / "service.json"
        try:
            payload = json.loads(config.read_text(encoding="utf-8"))
            configured = payload.get("TRAD_SERVICE_CANONICAL_PANEL")
            if configured:
                relative = Path(str(configured))
        except (OSError, ValueError, TypeError):
            pass
        return relative.resolve() if relative.is_absolute() else (self.root / relative).resolve()

    def _latest_pass(self) -> dict[str, object]:
        records: list[tuple[pd.Timestamp, dict[str, object]]] = []
        for path in (self.root / "operations" / "runs").glob("*.json"):
            if path.name == "latest_audit.json":
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("status") != "PASS":
                    continue
                finished = pd.to_datetime(payload.get("finished_at"), utc=True, errors="coerce")
                if pd.isna(finished):
                    continue
                records.append((finished, payload))
            except (OSError, ValueError, TypeError):
                continue
        if not records:
            raise RuntimeError("external panel has no successful promotion receipt")
        return max(records, key=lambda item: item[0])[1]

    def load(self) -> tuple[pd.DataFrame, dict[str, object]]:
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pyarrow is required for external PIT factor history") from exc
        panel = self.panel_path
        if not panel.is_file():
            raise FileNotFoundError(panel)
        receipt = self._latest_pass()
        expected_sha = str(receipt.get("canonical_sha_after") or "")
        actual_sha = _sha256(panel) if self.verify_sha256 else None
        if self.verify_sha256 and (not expected_sha or actual_sha != expected_sha):
            raise RuntimeError("canonical panel hash does not match its latest PASS receipt")
        dataset = ds.dataset(str(panel), format="parquet")
        required = {"symbol", "ts", "close"}
        if missing := sorted(required.difference(dataset.schema.names)):
            raise RuntimeError(f"canonical panel missing columns: {missing}")
        symbols = sorted(set(self.instruments.values()))
        table = dataset.to_table(
            columns=["symbol", "ts", "close"],
            filter=ds.field("symbol").isin(symbols),
        )
        raw = table.to_pandas()
        raw["ts"] = pd.to_datetime(raw["ts"], utc=True, errors="coerce")
        raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
        raw = raw.dropna(subset=["symbol", "ts", "close"])
        raw = raw[raw["close"] > 0].copy()
        conflicts = raw.groupby(["symbol", "ts"])["close"].nunique()
        if (conflicts > 1).any():
            raise RuntimeError("external panel contains conflicting base prices")
        raw = raw.drop_duplicates(["symbol", "ts"], keep="last")
        wide = raw.pivot(index="ts", columns="symbol", values="close").sort_index()
        history = pd.DataFrame(index=wide.index)
        missing_symbols: list[str] = []
        for feature, symbol in self.instruments.items():
            if symbol not in wide:
                history[feature] = float("nan")
                missing_symbols.append(symbol)
            else:
                history[feature] = wide[symbol].pct_change(fill_method=None)
        history = history.reset_index().rename(columns={"ts": "observed_at"})
        history["available_at"] = history["observed_at"] + self.availability_lag
        history = history.sort_values("available_at").reset_index(drop=True)
        evidence = {
            "source": "trad_data_service.canonical_panel",
            "panel_path": str(panel),
            "latest_pass_run_id": receipt.get("run_id"),
            "canonical_sha256": expected_sha,
            "hash_verified": self.verify_sha256,
            "selection_policy": "explicit_symbol_allowlist_base_prices_only",
            "availability_lag_seconds": int(self.availability_lag.total_seconds()),
            "maximum_age_seconds": int(self.maximum_age.total_seconds()),
            "row_count": len(history),
            "missing_symbols": sorted(set(missing_symbols)),
            "factor_columns": list(self.instruments),
            "pit_policy": (
                "available_at=panel_ts+conservative_daily_release_lag;"
                "reject_if_decision_at-available_at>maximum_age"
            ),
        }
        return history, evidence

    def join(
        self,
        decisions: pd.DataFrame,
        *,
        history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        if "decision_at" not in decisions:
            raise ValueError("decision_at is required for PIT factor join")
        factor_history = history if history is not None else self.load()[0]
        left = decisions.copy().reset_index(drop=False).rename(columns={"index": "_row_id"})
        left["decision_at"] = pd.to_datetime(left["decision_at"], utc=True, errors="coerce")
        if left["decision_at"].isna().any():
            raise ValueError("decision_at contains invalid timestamps")
        right = factor_history.copy()
        right["available_at"] = pd.to_datetime(right["available_at"], utc=True, errors="coerce")
        right = right.rename(columns={"available_at": "factor_available_at"})
        joined = pd.merge_asof(
            left.sort_values("decision_at"),
            right.sort_values("factor_available_at"),
            left_on="decision_at",
            right_on="factor_available_at",
            direction="backward",
            allow_exact_matches=True,
            tolerance=pd.Timedelta(self.maximum_age),
        )
        violation = joined["factor_available_at"].notna() & (
            joined["factor_available_at"] > joined["decision_at"]
        )
        if violation.any():
            raise RuntimeError("PIT factor join selected data unavailable at decision time")
        return joined.sort_values("_row_id").drop(columns=["_row_id"]).reset_index(drop=True)


__all__: Sequence[str] = (
    "TRAD_PANEL_FACTOR_GROUPS",
    "TRAD_PANEL_INSTRUMENTS",
    "TRAD_PANEL_MISSING_REQUIRED_FACTORS",
    "TradPanelHistorySource",
)
