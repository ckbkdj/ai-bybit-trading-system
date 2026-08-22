from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCHEMA_VERSION = "kfs_v1"
DEFAULT_FEATURE_VERSION = "kline_ta_v1"
LABEL_VERSION = "future_return_v1"
SCALER_VERSION = "standard_train_split_v1"
FORBIDDEN_HISTORICAL_FEATURE_KEYS = (
    "news", "funding", "long_short", "longshort", "liquidation", "llm", "snapshot", "market_snapshot"
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def timeframe_ms(tf: str) -> int:
    unit = tf[-1].lower()
    n = int(tf[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    if unit not in mult:
        raise ValueError(f"unsupported timeframe: {tf}")
    return n * mult[unit]


def _stable_hash(obj: Any, n: int = 24) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()[:n]


def _config_hash(cfg: Dict[str, Any]) -> str:
    relevant = {
        "modes": cfg.get("modes"),
        "general": {"cache_days": (cfg.get("general") or {}).get("cache_days")},
        "training": cfg.get("training"),
        "brain_model": cfg.get("brain_model"),
    }
    return _stable_hash(relevant)


@dataclass(frozen=True)
class ModeSpec:
    mode_name: str
    symbol: str
    base_timeframe: str
    requested_limit: int
    lookback_window: int
    cache_days: int
    label_horizon: int
    feature_version: str = DEFAULT_FEATURE_VERSION
    feature_set: str = "kline_ta_core"
    multi_timeframe: bool = False
    timeframe_set: Tuple[str, ...] = ()
    config_hash: str = ""


@dataclass(frozen=True)
class ModelDataSignature:
    mode_name: str
    symbol: str
    timeframe_set: Tuple[str, ...]
    base_timeframe: str
    train_start_ts: int
    train_end_ts: int
    row_count: int
    feature_version: str
    feature_set_hash: str
    config_hash: str
    schema_version: str = SCHEMA_VERSION
    label_version: str = LABEL_VERSION
    scaler_version: str = SCALER_VERSION

    def digest(self) -> str:
        return _stable_hash(asdict(self), 32)


@dataclass
class StoreUpdateResult:
    symbol: str
    updated_timeframes: Dict[str, int]
    enhanced_rows_upserted: Dict[str, int]
    base_new_rows: int


@dataclass
class BuiltDataset:
    df: pd.DataFrame
    feature_columns: list[str]
    target_column: str
    signature: ModelDataSignature


class KlineFeatureStore:
    def __init__(self, db_path: str | Path, cfg: Dict[str, Any], fetcher: Any = None, source: str = "binance"):
        self.db_path = Path(db_path)
        if not self.db_path.is_absolute():
            self.db_path = Path.cwd() / self.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.cfg = cfg or {}
        self.fetcher = fetcher
        self.source = source
        self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        return con

    def ensure_schema(self) -> None:
        with self._connect() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw_kline(
                    symbol TEXT NOT NULL, timeframe TEXT NOT NULL, source TEXT NOT NULL,
                    open_time INTEGER NOT NULL, close_time INTEGER NOT NULL,
                    open REAL, high REAL, low REAL, close REAL, volume REAL,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY(symbol,timeframe,source,open_time)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS enhanced_kline(
                    symbol TEXT NOT NULL, timeframe TEXT NOT NULL, source TEXT NOT NULL,
                    feature_version TEXT NOT NULL, config_hash TEXT NOT NULL, schema_version TEXT NOT NULL,
                    open_time INTEGER NOT NULL, close_time INTEGER NOT NULL,
                    open REAL, high REAL, low REAL, close REAL, volume REAL,
                    features_json TEXT NOT NULL, feature_set_hash TEXT NOT NULL, computed_at TEXT NOT NULL,
                    PRIMARY KEY(symbol,timeframe,source,feature_version,config_hash,schema_version,open_time)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS model_registry(
                    model_kind TEXT NOT NULL, mode_name TEXT NOT NULL, symbol TEXT NOT NULL,
                    signature_digest TEXT NOT NULL, model_path TEXT,
                    base_timeframe TEXT, timeframe_set_json TEXT,
                    feature_version TEXT, schema_version TEXT, config_hash TEXT, feature_set_hash TEXT,
                    train_start_ts INTEGER, train_end_ts INTEGER, row_count INTEGER,
                    trained_at TEXT, status TEXT, reason TEXT, metadata_json TEXT,
                    PRIMARY KEY(model_kind,mode_name,symbol,signature_digest)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS enhanced_update_meta(
                    symbol TEXT NOT NULL, timeframe TEXT NOT NULL, source TEXT NOT NULL,
                    feature_version TEXT NOT NULL, config_hash TEXT NOT NULL, schema_version TEXT NOT NULL,
                    last_enhanced_open_time INTEGER,
                    last_enhanced_close_time INTEGER,
                    last_raw_open_time INTEGER,
                    overlap_rows INTEGER,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(symbol,timeframe,source,feature_version,config_hash,schema_version)
                )
            """)
            con.commit()

    def load_mode_specs(self, symbols: Sequence[str] | None = None) -> list[ModeSpec]:
        syms = list(symbols or (self.cfg.get("general", {}) or {}).get("symbols") or [])
        modes = self.cfg.get("modes") or {}
        cache_days = int((self.cfg.get("general") or {}).get("cache_days", 1098))
        horizons = (self.cfg.get("brain_model") or {}).get("horizons") or {}
        mtf_cfg = (self.cfg.get("training") or {}).get("multi_timeframe") or {}
        mtf_enabled = bool(mtf_cfg.get("enabled", False))
        cfg_hash = _config_hash(self.cfg)
        all_tfs = [str(v[0]) for v in modes.values()]
        out: list[ModeSpec] = []
        for sym in syms:
            for mode, vals in modes.items():
                base_tf = str(vals[0]); base_ms = timeframe_ms(base_tf)
                if mtf_enabled:
                    tfset = tuple(dict.fromkeys([base_tf] + [tf for tf in all_tfs if timeframe_ms(tf) > base_ms]))
                else:
                    tfset = (base_tf,)
                out.append(ModeSpec(
                    mode_name=str(mode), symbol=str(sym), base_timeframe=base_tf,
                    requested_limit=int(vals[1]), lookback_window=int(vals[2]), cache_days=cache_days,
                    label_horizon=int(horizons.get(mode, 1)), multi_timeframe=mtf_enabled,
                    timeframe_set=tfset, config_hash=cfg_hash,
                ))
        return out

    def spec_for(self, symbol: str, mode_name: str) -> ModeSpec:
        for spec in self.load_mode_specs([symbol]):
            if spec.mode_name == mode_name:
                return spec
        raise KeyError(f"mode not found: {symbol}-{mode_name}")

    def upsert_raw_frame(self, symbol: str, timeframe: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        x = df.copy()
        if "open_time" not in x.columns:
            if "ts" not in x.columns:
                raise ValueError("raw kline frame requires ts or open_time")
            ts = pd.to_datetime(x["ts"], utc=True)
            # pandas may store datetime64 in ns/us depending on version/input; timestamp() is always seconds.
            x["open_time"] = ts.map(lambda v: int(v.timestamp() * 1000))
        if "close_time" not in x.columns:
            x["close_time"] = x["open_time"].astype("int64") + timeframe_ms(timeframe)
        now = _now_iso(); rows = []
        for _, r in x.iterrows():
            rows.append((symbol, timeframe, self.source, int(r["open_time"]), int(r["close_time"]),
                         float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r["volume"]), now))
        with self._connect() as con:
            con.executemany("""
                INSERT OR REPLACE INTO raw_kline(symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            con.commit()
        return len(rows)

    def load_raw_frame(self, symbol: str, timeframe: str, limit: int | None = None, start_open_time: int | None = None) -> pd.DataFrame:
        sql = "SELECT open_time,close_time,open,high,low,close,volume FROM raw_kline WHERE symbol=? AND timeframe=? AND source=?"
        params: list[Any] = [symbol, timeframe, self.source]
        if start_open_time is not None:
            sql += " AND open_time>=?"
            params.append(int(start_open_time))
        sql += " ORDER BY open_time ASC"
        with self._connect() as con:
            df = pd.read_sql(sql, con, params=tuple(params))
        if limit and len(df) > limit:
            df = df.tail(int(limit)).reset_index(drop=True)
        return df

    def _last_raw_open_time(self, symbol: str, timeframe: str) -> Optional[int]:
        with self._connect() as con:
            row = con.execute("SELECT MAX(open_time) AS m FROM raw_kline WHERE symbol=? AND timeframe=? AND source=?", (symbol, timeframe, self.source)).fetchone()
        return int(row["m"]) if row and row["m"] is not None else None

    async def ensure_raw_kline(self, symbol: str, timeframe: str, requested_limit: int) -> int:
        if self.fetcher is None:
            return 0
        last = self._last_raw_open_time(symbol, timeframe)
        if last is None:
            df = await self.fetcher.get_ohlcv(symbol, timeframe, requested_limit)
        elif hasattr(self.fetcher, "get_ohlcv_incremental"):
            df = await self.fetcher.get_ohlcv_incremental(symbol, timeframe, last + timeframe_ms(timeframe), limit=1500)
        else:
            return 0
        return self.upsert_raw_frame(symbol, timeframe, df)

    async def update_for_mode(self, spec: ModeSpec) -> StoreUpdateResult:
        updated: Dict[str, int] = {}; enhanced: Dict[str, int] = {}
        for tf in spec.timeframe_set or (spec.base_timeframe,):
            limit = spec.requested_limit if tf == spec.base_timeframe else max(1500, min(spec.requested_limit, 10000))
            updated[tf] = await self.ensure_raw_kline(spec.symbol, tf, limit)
            enhanced[tf] = self.update_enhanced_kline(spec.symbol, tf, spec)
        return StoreUpdateResult(spec.symbol, updated, enhanced, updated.get(spec.base_timeframe, 0))

    def compute_kline_features(self, df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        if df.empty:
            return df.copy(), _stable_hash([])
        x = df.copy().sort_values("open_time").reset_index(drop=True)
        close = pd.to_numeric(x["close"], errors="coerce").astype(float).replace(0, np.nan)
        open_ = pd.to_numeric(x["open"], errors="coerce").astype(float).replace(0, np.nan)
        high = pd.to_numeric(x["high"], errors="coerce").astype(float)
        low = pd.to_numeric(x["low"], errors="coerce").astype(float)
        volume = pd.to_numeric(x["volume"], errors="coerce").astype(float).fillna(0.0)
        feats = pd.DataFrame(index=x.index)
        feats["ret_1"] = close.pct_change(1)
        feats["ret_3"] = close.pct_change(3)
        feats["ret_6"] = close.pct_change(6)
        feats["logret_1"] = np.log(close / close.shift(1))
        feats["range_pct"] = (high - low) / close
        feats["body_pct"] = (close - open_) / open_
        feats["upper_wick_pct"] = (high - np.maximum(open_, close)) / close
        feats["lower_wick_pct"] = (np.minimum(open_, close) - low) / close
        feats["volume_zscore"] = (volume - volume.rolling(48, min_periods=8).mean()) / (volume.rolling(48, min_periods=8).std() + 1e-9)
        tr = pd.concat([(high-low), (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
        feats["atr_pct"] = tr.rolling(14, min_periods=4).mean() / close
        feats["realized_vol_12"] = close.pct_change().rolling(12, min_periods=6).std()
        feats["realized_vol_24"] = close.pct_change().rolling(24, min_periods=8).std()
        ema8 = close.ewm(span=8, adjust=False, min_periods=4).mean(); ema21 = close.ewm(span=21, adjust=False, min_periods=8).mean()
        feats["ema_gap_8_21"] = ema8 / (ema21 + 1e-9) - 1
        feats["ma_gap_21_55"] = close.rolling(21, min_periods=8).mean() / (close.rolling(55, min_periods=16).mean() + 1e-9) - 1
        mid = close.rolling(20, min_periods=8).mean(); std = close.rolling(20, min_periods=8).std(); upper = mid + 2*std; lower = mid - 2*std
        feats["boll_pos"] = (close - lower) / (upper - lower + 1e-9)
        feats["trend_strength"] = close.pct_change(12) / (feats["realized_vol_24"] + 1e-9)
        self._validate_feature_columns(feats.columns)
        out = pd.concat([x[["open_time","close_time","open","high","low","close","volume"]], feats], axis=1).replace([np.inf,-np.inf], np.nan)
        return out, _stable_hash(list(feats.columns), 16)

    def _validate_feature_columns(self, cols: Iterable[str]) -> None:
        bad = [c for c in cols if any(tok in c.lower() for tok in FORBIDDEN_HISTORICAL_FEATURE_KEYS)]
        if bad:
            raise ValueError(f"forbidden historical feature columns: {bad}")

    def _last_enhanced_open_time(self, symbol: str, timeframe: str, spec: ModeSpec) -> Optional[int]:
        with self._connect() as con:
            row = con.execute(
                """SELECT MAX(open_time) AS m FROM enhanced_kline
                   WHERE symbol=? AND timeframe=? AND source=? AND feature_version=? AND config_hash=? AND schema_version=?""",
                (symbol, timeframe, self.source, spec.feature_version, spec.config_hash, SCHEMA_VERSION),
            ).fetchone()
        return int(row["m"]) if row and row["m"] is not None else None

    def _raw_open_time_at_tail_offset(self, symbol: str, timeframe: str, last_open_time: int, overlap_rows: int) -> int:
        with self._connect() as con:
            row = con.execute(
                """SELECT open_time FROM raw_kline
                   WHERE symbol=? AND timeframe=? AND source=? AND open_time<=?
                   ORDER BY open_time DESC LIMIT 1 OFFSET ?""",
                (symbol, timeframe, self.source, int(last_open_time), max(0, int(overlap_rows))),
            ).fetchone()
        if row and row["open_time"] is not None:
            return int(row["open_time"])
        return max(0, int(last_open_time) - timeframe_ms(timeframe) * max(1, int(overlap_rows)))

    def _overlap_rows(self, spec: ModeSpec) -> int:
        configured = int((self.cfg.get("training") or {}).get("feature_overlap_rows", 0) or 0)
        # Longest current rolling window is 55; use a conservative default so tail recomputation is stable.
        return max(configured, int(spec.lookback_window or 0), 256) + 1

    def update_enhanced_kline(self, symbol: str, timeframe: str, spec: ModeSpec) -> int:
        overlap = self._overlap_rows(spec)
        last_enhanced = self._last_enhanced_open_time(symbol, timeframe, spec)
        start_open_time = None
        if last_enhanced is not None:
            start_open_time = self._raw_open_time_at_tail_offset(symbol, timeframe, last_enhanced, overlap)
        raw = self.load_raw_frame(symbol, timeframe, start_open_time=start_open_time)
        feats, fhash = self.compute_kline_features(raw)
        if feats.empty:
            return 0
        # If this is an incremental tail recompute, only write rows from the overlap/tail region.
        if start_open_time is not None:
            feats = feats[feats["open_time"] >= int(start_open_time)].copy()
        feature_cols = [c for c in feats.columns if c not in {"open_time","close_time","open","high","low","close","volume"}]
        rows = []
        now = _now_iso()
        for _, r in feats.iterrows():
            fj = {c: (None if pd.isna(r[c]) else float(r[c])) for c in feature_cols}
            rows.append((symbol, timeframe, self.source, spec.feature_version, spec.config_hash, SCHEMA_VERSION,
                         int(r.open_time), int(r.close_time), float(r.open), float(r.high), float(r.low), float(r.close), float(r.volume),
                         json.dumps(fj, ensure_ascii=False, sort_keys=True), fhash, now))
        with self._connect() as con:
            con.executemany("""
                INSERT OR REPLACE INTO enhanced_kline(symbol,timeframe,source,feature_version,config_hash,schema_version,open_time,close_time,open,high,low,close,volume,features_json,feature_set_hash,computed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            if rows:
                con.execute("""
                    INSERT OR REPLACE INTO enhanced_update_meta(symbol,timeframe,source,feature_version,config_hash,schema_version,last_enhanced_open_time,last_enhanced_close_time,last_raw_open_time,overlap_rows,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    symbol, timeframe, self.source, spec.feature_version, spec.config_hash, SCHEMA_VERSION,
                    int(feats["open_time"].max()), int(feats["close_time"].max()),
                    int(raw["open_time"].max()) if not raw.empty else None,
                    int(overlap), now,
                ))
            con.commit()
        return len(rows)

    def load_enhanced_frame(self, symbol: str, timeframe: str, spec: ModeSpec, limit: int | None = None) -> pd.DataFrame:
        sql = """SELECT open_time,close_time,open,high,low,close,volume,features_json,feature_set_hash
                 FROM enhanced_kline WHERE symbol=? AND timeframe=? AND source=? AND feature_version=? AND config_hash=? AND schema_version=?
                 ORDER BY open_time ASC"""
        with self._connect() as con:
            rows = con.execute(sql, (symbol, timeframe, self.source, spec.feature_version, spec.config_hash, SCHEMA_VERSION)).fetchall()
        records = []
        for r in rows:
            d = dict(r); fj = json.loads(d.pop("features_json") or "{}"); d.update(fj); records.append(d)
        df = pd.DataFrame(records)
        if limit and len(df) > limit:
            df = df.tail(int(limit)).reset_index(drop=True)
        return df

    def build_feature_frame(self, spec: ModeSpec, include_mtf: bool = True) -> pd.DataFrame:
        base = self.load_enhanced_frame(spec.symbol, spec.base_timeframe, spec, spec.requested_limit).sort_values("close_time")
        if base.empty or not include_mtf or not spec.multi_timeframe:
            return base.reset_index(drop=True)
        out = base.copy()
        for tf in spec.timeframe_set:
            if tf == spec.base_timeframe:
                continue
            high = self.load_enhanced_frame(spec.symbol, tf, spec).sort_values("close_time")
            if high.empty:
                continue
            keep = [c for c in high.columns if c not in {"open_time"}]
            high = high[keep].rename(columns={c: f"mtf_{tf}_{c}" for c in keep if c not in {"close_time"}})
            high[f"mtf_{tf}_source_close_time"] = high["close_time"]
            out = pd.merge_asof(out.sort_values("close_time"), high.sort_values("close_time"), on="close_time", direction="backward")
            src = f"mtf_{tf}_source_close_time"
            if src in out.columns and out[src].notna().any():
                if not (out.loc[out[src].notna(), src] <= out.loc[out[src].notna(), "close_time"]).all():
                    raise ValueError(f"future leakage in multi-timeframe merge: {tf}")
        return out.reset_index(drop=True)

    def build_mode_dataset(self, spec: ModeSpec, shift_features: bool = True) -> BuiltDataset:
        frame = self.build_feature_frame(spec, include_mtf=True)
        if frame.empty:
            sig = ModelDataSignature(spec.mode_name, spec.symbol, tuple(spec.timeframe_set), spec.base_timeframe, 0, 0, 0, spec.feature_version, _stable_hash([]), spec.config_hash)
            return BuiltDataset(frame, [], "future_return", sig)
        excluded = {"open_time","close_time","open","high","low","close","volume","feature_set_hash","future_return"}
        # Only model derived feature columns, never raw OHLCV/timestamps/labels/source diagnostic columns.
        feature_cols = [c for c in frame.columns if c not in excluded and not c.endswith("source_close_time")]
        self._validate_feature_columns(feature_cols)
        raw_leaks = {"open", "high", "low", "close", "volume", "open_time", "close_time", "future_return"}.intersection(feature_cols)
        if raw_leaks:
            raise ValueError(f"raw/label columns cannot be model features: {sorted(raw_leaks)}")
        ds = frame.copy().sort_values("close_time").reset_index(drop=True)
        if shift_features:
            ds[feature_cols] = ds[feature_cols].shift(1)
        horizon = max(1, int(spec.label_horizon))
        ds["future_return"] = ds["close"].shift(-horizon) / ds["close"] - 1.0
        ds = ds.dropna(subset=feature_cols + ["future_return"]).reset_index(drop=True)
        fhash = _stable_hash(feature_cols, 16)
        sig = ModelDataSignature(
            mode_name=spec.mode_name, symbol=spec.symbol, timeframe_set=tuple(spec.timeframe_set), base_timeframe=spec.base_timeframe,
            train_start_ts=int(ds["close_time"].iloc[0]) if len(ds) else 0,
            train_end_ts=int(ds["close_time"].iloc[-1]) if len(ds) else 0,
            row_count=int(len(ds)), feature_version=spec.feature_version, feature_set_hash=fhash, config_hash=spec.config_hash,
        )
        return BuiltDataset(ds, feature_cols, "future_return", sig)

    def should_train(self, model_kind: str, spec: ModeSpec, signature: ModelDataSignature, model_path: str | None, new_rows: int) -> tuple[bool, str]:
        digest = signature.digest()
        exists = bool(model_path and Path(model_path).exists())
        with self._connect() as con:
            row = con.execute("SELECT status FROM model_registry WHERE model_kind=? AND mode_name=? AND symbol=? AND signature_digest=? AND status='trained'", (model_kind, spec.mode_name, spec.symbol, digest)).fetchone()
        if exists and row:
            return False, "skipped_same_signature"
        if int(signature.row_count) <= 0:
            return False, "skipped_empty_dataset"
        min_new = int((self.cfg.get("training") or {}).get("min_new_rows_for_retrain", 3))
        if 0 < int(new_rows) < min_new:
            return False, f"skipped_new_rows_below_threshold:{new_rows}<{min_new}"
        return True, "train_required"

    def record_model(self, model_kind: str, spec: ModeSpec, signature: ModelDataSignature, model_path: str | None, status: str, reason: str, metadata: Dict[str, Any] | None = None) -> None:
        with self._connect() as con:
            con.execute("""
                INSERT OR REPLACE INTO model_registry(model_kind,mode_name,symbol,signature_digest,model_path,base_timeframe,timeframe_set_json,feature_version,schema_version,config_hash,feature_set_hash,train_start_ts,train_end_ts,row_count,trained_at,status,reason,metadata_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                model_kind, spec.mode_name, spec.symbol, signature.digest(), model_path, spec.base_timeframe,
                json.dumps(list(signature.timeframe_set)), signature.feature_version, signature.schema_version,
                signature.config_hash, signature.feature_set_hash, int(signature.train_start_ts), int(signature.train_end_ts),
                int(signature.row_count), _now_iso(), status, reason, json.dumps(metadata or {}, ensure_ascii=False, default=str),
            ))
            con.commit()
