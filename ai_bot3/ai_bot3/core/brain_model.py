from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import talib
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier, ExtraTreesClassifier
from sklearn.metrics import precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from .evaluation.time_series_split import purged_holdout_boundary
    from .evaluation.statistical_governance import TrialLedger, TrialRecord
except ImportError:  # Direct file loading in governance tests and maintenance tools.
    from core.evaluation.time_series_split import purged_holdout_boundary
    from core.evaluation.statistical_governance import TrialLedger, TrialRecord

log = logging.getLogger("BrainModel")

BRAIN_VERSION = "brain_sklearn_v1"
DEFAULT_TARGET_LEVERAGED_PROFIT = 0.31
DEFAULT_LEVERAGE = {"BTCUSDT": 100, "ETHUSDT": 100, "XRPUSDT": 75, "SOLUSDT": 75, "1000PEPEUSDT": 75}
DEFAULT_HORIZONS = {"scalping": 3, "mid_short": 2, "trend": 2, "trend_swing": 2, "swing": 1}
LABEL_SHORT = 0
LABEL_FLAT = 1
LABEL_LONG = 2

# 发布状态机：live / candidate / shadow / rejected 四个目录。当前训练仍把
# 主 joblib 写入 model_dir 根目录以保留旧加载路径（向后兼容），同时把训练
# 决策记录到 meta，便于后续晋升流程消费。
BRAIN_STAGE_DIRS: Tuple[str, ...] = ("live", "candidate", "shadow", "rejected")
# 晋升最低门槛（与 docs/final_optimized_quant_brain_plan.md §9.3 保持一致）。
PROMOTE_MIN_VALIDATION_SAMPLES = 100
PROMOTE_MIN_DIRECTION_ACC = 0.50
PROMOTE_STRONG_HIT_RATE = 0.52
PROMOTE_STRONG_PRECISION = 0.55


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _brain_cfg(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    cfg = cfg or {}
    out = dict(cfg.get("brain_model") or {})
    out.setdefault("enabled", True)
    out.setdefault("target_leveraged_profit", DEFAULT_TARGET_LEVERAGED_PROFIT)
    out.setdefault("leverage", DEFAULT_LEVERAGE)
    out.setdefault("default_leverage", 75)
    out.setdefault("model_dir", "./models/brain")
    out.setdefault("history_db", "./data/brain_training_history.sqlite3")
    out.setdefault("trial_ledger_db", "./data/research_trials.sqlite3")
    out.setdefault("min_samples", 600)
    out.setdefault("validation_fraction", 0.2)
    out.setdefault("min_confidence", 0.58)
    out.setdefault("volatility_multiplier", 1.2)
    out.setdefault("min_train_threshold", 0.0012)
    out.setdefault("horizons", DEFAULT_HORIZONS)
    out.setdefault("inference_stage", os.environ.get("AI_BOT_BRAIN_INFERENCE_STAGE", "shadow"))
    return out


def _resolve_path(path_value: str | Path) -> Path:
    p = Path(path_value)
    if not p.is_absolute():
        p = _project_root() / p
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def symbol_leverage(symbol: str, cfg: Dict[str, Any] | None = None) -> int:
    bc = _brain_cfg(cfg)
    full = symbol.upper() if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    lev_map = {str(k).upper(): int(v) for k, v in (bc.get("leverage") or {}).items()}
    return int(lev_map.get(full, bc.get("default_leverage", 75)))


def target_raw_return(symbol: str, cfg: Dict[str, Any] | None = None) -> float:
    bc = _brain_cfg(cfg)
    return float(bc.get("target_leveraged_profit", DEFAULT_TARGET_LEVERAGED_PROFIT)) / max(1, symbol_leverage(symbol, cfg))


def horizon_for_mode(mode: str, cfg: Dict[str, Any] | None = None) -> int:
    return int((_brain_cfg(cfg).get("horizons") or DEFAULT_HORIZONS).get(mode, DEFAULT_HORIZONS.get(mode, 1)))


def _ensure_brain_stage_dirs(model_dir: Path) -> Dict[str, Path]:
    """Create models/brain/{live,candidate,shadow,rejected} (idempotent).

    Returns a mapping ``stage -> path`` for callers that want to materialise
    a candidate/shadow artifact. Kept side-effect only; old root-level
    ``models/brain/*.joblib`` files remain the authoritative inference path
    to preserve compatibility.
    """
    stages: Dict[str, Path] = {}
    for stage in BRAIN_STAGE_DIRS:
        sub = model_dir / stage
        try:
            sub.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("brain stage dir create failed %s: %s", sub, exc)
        stages[stage] = sub
    return stages


def brain_paths(symbol: str, mode: str, cfg: Dict[str, Any] | None = None) -> Tuple[Path, Path]:
    bc = _brain_cfg(cfg)
    model_dir = _resolve_path(bc.get("model_dir", "./models/brain") + "/.keep").parent
    model_dir.mkdir(parents=True, exist_ok=True)
    # 治理目录始终就位，便于 trainer / api 后续放置 candidate / shadow 工件。
    _ensure_brain_stage_dirs(model_dir)
    tag = f"{symbol}_{mode}_brain"
    return model_dir / f"{tag}.joblib", model_dir / f"{tag}_meta.json"


def brain_stage_paths(symbol: str, mode: str, cfg: Dict[str, Any] | None = None) -> Dict[str, Path]:
    """Return ``stage -> directory`` for the four governance stages.

    Side-effect: ensures the directories exist. Kept lightweight so callers
    can compose stage-specific paths without rediscovering the brain root.
    """
    bc = _brain_cfg(cfg)
    model_dir = _resolve_path(bc.get("model_dir", "./models/brain") + "/.keep").parent
    model_dir.mkdir(parents=True, exist_ok=True)
    return _ensure_brain_stage_dirs(model_dir)


def brain_stage_artifact_paths(
    symbol: str, mode: str, stage: str, cfg: Dict[str, Any] | None = None
) -> Tuple[Path, Path]:
    normalized = str(stage).lower()
    if normalized not in BRAIN_STAGE_DIRS:
        raise ValueError(f"unsupported brain stage: {stage}")
    directory = brain_stage_paths(symbol, mode, cfg)[normalized]
    tag = f"{symbol}_{mode}_brain"
    return directory / f"{tag}.joblib", directory / f"{tag}_meta.json"


def history_db_path(cfg: Dict[str, Any] | None = None) -> Path:
    return _resolve_path(_brain_cfg(cfg).get("history_db", "./data/brain_training_history.sqlite3"))


def _ensure_history_db(cfg: Dict[str, Any] | None = None) -> sqlite3.Connection:
    p = history_db_path(cfg)
    con = sqlite3.connect(str(p))
    con.execute("""
        CREATE TABLE IF NOT EXISTS brain_training_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            symbol TEXT,
            mode TEXT,
            timeframe TEXT,
            data_start_ts TEXT,
            data_end_ts TEXT,
            rows INTEGER,
            feature_count INTEGER,
            data_signature TEXT,
            model_path TEXT,
            meta_path TEXT,
            status TEXT,
            metrics_json TEXT
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_brain_runs_key ON brain_training_runs(symbol, mode, data_signature, status)")
    con.commit()
    return con


def _record_history(cfg: Dict[str, Any], symbol: str, mode: str, tf_code: str, df: pd.DataFrame, signature: str, model_path: Path, meta_path: Path, status: str, metrics: Dict[str, Any]) -> None:
    try:
        con = _ensure_history_db(cfg)
        con.execute(
            """INSERT INTO brain_training_runs(created_at,symbol,mode,timeframe,data_start_ts,data_end_ts,rows,feature_count,data_signature,model_path,meta_path,status,metrics_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                _now_iso(), symbol, mode, tf_code,
                str(df.index[0]) if len(df) else None,
                str(df.index[-1]) if len(df) else None,
                int(len(df)), int(metrics.get("feature_count") or 0), signature,
                str(model_path), str(meta_path), status, json.dumps(metrics, ensure_ascii=False, default=str),
            ),
        )
        con.commit(); con.close()
        if status == "trained":
            parameters = {
                "classifier": "HistGradientBoostingClassifier",
                "max_iter": 240,
                "learning_rate": 0.045,
                "max_leaf_nodes": 31,
                "l2_regularization": 0.05,
                "mode": mode,
                "timeframe": tf_code,
                "validation": metrics.get("validation"),
            }
            generated_at = str(metrics.get("generated_at") or _now_iso())
            trial_id = hashlib.sha256(
                f"{symbol}|{mode}|{signature}|{generated_at}".encode()
            ).hexdigest()[:32]
            ledger = TrialLedger(
                _resolve_path(cfg.get("trial_ledger_db", "./data/research_trials.sqlite3"))
            )
            ledger.append(
                TrialRecord(
                    trial_id=trial_id,
                    model_family="brain_hist_gradient_boosting",
                    data_signature=signature,
                    parameter_hash=TrialLedger.parameter_hash(parameters),
                    code_commit=str(metrics.get("code_commit") or "unknown"),
                    status=(
                        "rejected"
                        if str(metrics.get("promote_decision")) == "rejected"
                        else "completed"
                    ),
                    metrics=metrics,
                )
            )
    except Exception as exc:
        log.warning("brain training history write failed: %s", exc)


def _has_success(cfg: Dict[str, Any], symbol: str, mode: str, signature: str, model_path: Path) -> bool:
    if not model_path.exists():
        return False
    try:
        con = _ensure_history_db(cfg)
        row = con.execute(
            "SELECT id FROM brain_training_runs WHERE symbol=? AND mode=? AND data_signature=? AND status='trained' ORDER BY id DESC LIMIT 1",
            (symbol, mode, signature),
        ).fetchone()
        con.close()
        return row is not None
    except Exception:
        return False


def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype(float)


def build_brain_features(df: pd.DataFrame, mode: str, symbol: str, market_snapshot: Optional[Dict[str, Any]] = None, cfg: Dict[str, Any] | None = None) -> pd.DataFrame:
    bc = _brain_cfg(cfg)
    x = df.copy()
    for col in ("open", "high", "low", "close", "volume"):
        x[col] = _safe_num(x[col])
    close = x["close"].replace(0, np.nan)
    open_ = x["open"].replace(0, np.nan)
    high = x["high"]
    low = x["low"]
    vol = x["volume"].fillna(0)

    feat = pd.DataFrame(index=x.index)
    for n in (1, 2, 3, 6, 12, 24):
        feat[f"ret_{n}"] = close.pct_change(n)
    feat["log_volume"] = np.log1p(vol)
    feat["volume_zscore"] = (vol - vol.rolling(48, min_periods=8).mean()) / (vol.rolling(48, min_periods=8).std() + 1e-9)
    feat["range_pct"] = (high - low) / close
    feat["body_pct"] = (close - open_) / open_
    feat["upper_wick_pct"] = (high - np.maximum(open_, close)) / close
    feat["lower_wick_pct"] = (np.minimum(open_, close) - low) / close
    feat["ma_gap_8_21"] = close.rolling(8, min_periods=4).mean() / (close.rolling(21, min_periods=8).mean() + 1e-9) - 1
    feat["ma_gap_21_55"] = close.rolling(21, min_periods=8).mean() / (close.rolling(55, min_periods=16).mean() + 1e-9) - 1
    feat["rsi"] = talib.RSI(close.values.astype(float), 14)
    macd, sig, hist = talib.MACD(close.values.astype(float), 12, 26, 9)
    feat["macd"] = macd / close
    feat["macdhist"] = hist / close
    up, mid, lowb = talib.BBANDS(close.values.astype(float), 20)
    feat["boll_pos"] = (close - lowb) / (up - lowb + 1e-9)
    atr = talib.ATR(high.values.astype(float), low.values.astype(float), close.values.astype(float), 14)
    feat["atr_pct"] = atr / close
    feat["realized_vol_12"] = close.pct_change().rolling(12, min_periods=6).std()
    feat["realized_vol_24"] = close.pct_change().rolling(24, min_periods=8).std()
    feat["trend_strength"] = close.pct_change(12) / (feat["realized_vol_24"] + 1e-9)
    feat["mode_id"] = {"scalping": 1, "mid_short": 2, "trend": 3, "trend_swing": 4, "swing": 5}.get(mode, 0)
    # 当前快照只允许作为推理门控；训练集特征默认保持中性，避免把“今天”的市场快照泄漏到历史样本。
    snap = {}
    if not bool(bc.get("historical_kline_only", True)):
        snap = market_snapshot or {}
    for key in ("liquidation_imbalance", "funding_rate", "long_short_ratio", "open_interest_change", "volume_24h_change"):
        try:
            feat[f"snap_{key}"] = float(snap.get(key, 0.0) or 0.0)
        except Exception:
            feat[f"snap_{key}"] = 0.0
    shift_n = int(bc.get("anti_leakage_shift_features", 1) or 0)
    if shift_n > 0:
        # 训练标签是 t -> t+horizon，特征整体 shift(1) 后只使用 t-1 已收盘以前的信息。
        # 推理传入 market_snapshot 时不 shift，保留当前已知快照作为实时门控。
        if market_snapshot is None:
            feat = feat.shift(shift_n)
    feat = feat.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return feat.astype(np.float32)


def compute_signature(df: pd.DataFrame, symbol: str, mode: str, cfg: Dict[str, Any] | None = None) -> str:
    bc = _brain_cfg(cfg)
    payload = {
        "symbol": symbol,
        "mode": mode,
        "rows": int(len(df)),
        "start": str(df.index[0]) if len(df) else None,
        "end": str(df.index[-1]) if len(df) else None,
        "last_close": float(pd.to_numeric(df["close"], errors="coerce").iloc[-1]) if len(df) else None,
        "version": BRAIN_VERSION,
        "target": bc.get("target_leveraged_profit"),
        "horizon": horizon_for_mode(mode, cfg),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:24]


def make_brain_dataset(df: pd.DataFrame, mode: str, symbol: str, cfg: Dict[str, Any] | None = None) -> Tuple[pd.DataFrame, np.ndarray, Dict[str, Any]]:
    bc = _brain_cfg(cfg)
    horizon = horizon_for_mode(mode, cfg)
    features = build_brain_features(df, mode, symbol, cfg=cfg)
    close = pd.to_numeric(df["close"], errors="coerce").astype(float)
    future_return = close.shift(-horizon) / close - 1.0
    ret1 = close.pct_change().rolling(24, min_periods=8).std().fillna(0.0)
    strict = target_raw_return(symbol, cfg)
    train_threshold = np.minimum(strict, np.maximum(ret1 * float(bc.get("volatility_multiplier", 1.2)), float(bc.get("min_train_threshold", 0.0012))))
    y = np.full(len(df), LABEL_FLAT, dtype=np.int64)
    y[future_return.values >= train_threshold.values] = LABEL_LONG
    y[future_return.values <= -train_threshold.values] = LABEL_SHORT
    valid = future_return.notna() & np.isfinite(future_return) & features.notna().all(axis=1)
    X = features.loc[valid]
    yy = y[valid.values]
    meta = {
        "horizon": horizon,
        "strict_target_return": strict,
        "target_leveraged_profit": float(bc.get("target_leveraged_profit", DEFAULT_TARGET_LEVERAGED_PROFIT)),
        "leverage": symbol_leverage(symbol, cfg),
        "feature_columns": list(features.columns),
        "class_counts": {str(k): int(v) for k, v in zip(*np.unique(yy, return_counts=True))} if len(yy) else {},
        "training_data_policy": {
            "source": "ohlcv_kline_only",
            "no_news": True,
            "no_current_snapshot_broadcast": bool(bc.get("historical_kline_only", True)),
        },
        "anti_leakage": {
            "feature_shift": int(bc.get("anti_leakage_shift_features", 1) or 0),
            "label_alignment": f"future_return=close.shift(-{horizon})/close-1",
            "chronological_split": True,
            "shuffle": False,
        },
        "actual_rows": int(len(df)),
        "data_start_ts": str(df.index[0]) if len(df) else None,
        "data_end_ts": str(df.index[-1]) if len(df) else None,
    }
    return X, yy, meta


def _model() -> Pipeline:
    # HistGradientBoosting is fast and robust for medium tabular data; scaler helps fallback linear-ish splits with stable numeric ranges.
    return Pipeline([
        ("scaler", StandardScaler()),
        ("clf", HistGradientBoostingClassifier(max_iter=240, learning_rate=0.045, max_leaf_nodes=31, l2_regularization=0.05, random_state=42)),
    ])


def _evaluate(model: Pipeline, X_val: pd.DataFrame, y_val: np.ndarray, strict_target: float, leverage: int) -> Dict[str, Any]:
    pred = model.predict(X_val)
    proba = model.predict_proba(X_val) if hasattr(model, "predict_proba") else np.zeros((len(X_val), 3))
    labels = list(getattr(model.named_steps.get("clf"), "classes_", [0,1,2]))
    def col(label):
        return labels.index(label) if label in labels else None
    long_col, short_col = col(LABEL_LONG), col(LABEL_SHORT)
    p_long = proba[:, long_col] if long_col is not None and len(proba) else np.zeros(len(pred))
    p_short = proba[:, short_col] if short_col is not None and len(proba) else np.zeros(len(pred))
    nonflat = y_val != LABEL_FLAT
    direction_acc = float((pred[nonflat] == y_val[nonflat]).mean()) if nonflat.any() else 0.0
    precision_long = float(precision_score(y_val, pred, labels=[LABEL_LONG], average='micro', zero_division=0))
    precision_short = float(precision_score(y_val, pred, labels=[LABEL_SHORT], average='micro', zero_division=0))
    conf = np.maximum(p_long, p_short)
    actionable = (pred != LABEL_FLAT) & (conf >= 0.58)
    return {
        "validation_samples": int(len(y_val)),
        "direction_acc_nonflat": direction_acc,
        "precision_long": precision_long,
        "precision_short": precision_short,
        "flat_rate_pred": float((pred == LABEL_FLAT).mean()) if len(pred) else 1.0,
        "actionable_rate": float(actionable.mean()) if len(actionable) else 0.0,
        "strict_target_return": strict_target,
        "leverage": leverage,
        "target_leveraged_profit": DEFAULT_TARGET_LEVERAGED_PROFIT,
    }


def _decide_promotion(metrics: Dict[str, Any], samples: int, min_samples: int) -> Tuple[str, str, Dict[str, Any]]:
    """Decide promote_decision / promote_reason / baseline_comparison.

    Conservative defaults: anything below the minimum validation sample
    floor stays in ``shadow``; below baseline direction accuracy gets
    ``rejected``; strong metrics get ``candidate`` (NOT auto-live; promotion
    to ``live`` is reserved for the release pipeline which must also factor
    in shadow / settled outcomes per docs §9.3).
    """
    val_samples = int(metrics.get("validation_samples") or 0)
    direction_acc = float(metrics.get("direction_acc_nonflat") or 0.0)
    precision_long = float(metrics.get("precision_long") or 0.0)
    precision_short = float(metrics.get("precision_short") or 0.0)
    actionable_rate = float(metrics.get("actionable_rate") or 0.0)
    baseline = {
        "baseline_direction_acc": PROMOTE_MIN_DIRECTION_ACC,
        "baseline_hit_rate": PROMOTE_STRONG_HIT_RATE,
        "baseline_precision": PROMOTE_STRONG_PRECISION,
        "direction_acc_nonflat": direction_acc,
        "precision_long": precision_long,
        "precision_short": precision_short,
        "actionable_rate": actionable_rate,
        "validation_samples": val_samples,
        "min_validation_samples": PROMOTE_MIN_VALIDATION_SAMPLES,
        "min_training_samples": int(min_samples),
        "training_samples": int(samples),
    }
    # Insufficient samples / metrics → shadow (don't auto-promote).
    if samples < int(min_samples):
        return "shadow", "training_samples_below_floor", baseline
    if val_samples < PROMOTE_MIN_VALIDATION_SAMPLES:
        return "shadow", "validation_samples_below_floor", baseline
    if direction_acc < PROMOTE_MIN_DIRECTION_ACC:
        return "rejected", "direction_acc_below_baseline", baseline
    strong = (
        direction_acc >= PROMOTE_STRONG_HIT_RATE
        and (precision_long >= PROMOTE_STRONG_PRECISION or precision_short >= PROMOTE_STRONG_PRECISION)
    )
    if strong:
        return "candidate", "metrics_meet_strong_baseline", baseline
    return "shadow", "metrics_above_floor_below_strong_baseline", baseline


def train_brain_from_df(df: pd.DataFrame, symbol: str, tf_code: str, mode: str, cfg: Dict[str, Any] | None = None, force: bool = False) -> Dict[str, Any]:
    bc = _brain_cfg(cfg)
    model_path, meta_path = brain_paths(symbol, mode, cfg)
    signature = compute_signature(df, symbol, mode, cfg)
    base_metrics = {"feature_count": 0, "data_signature": signature, "version": BRAIN_VERSION}
    if not bool(bc.get("enabled", True)):
        meta = {
            **base_metrics,
            "status": "disabled",
            "generated_at": _now_iso(),
            "promote_decision": "hold",
            "promote_reason": "brain_disabled",
            "baseline_comparison": {
                "min_validation_samples": PROMOTE_MIN_VALIDATION_SAMPLES,
                "baseline_direction_acc": PROMOTE_MIN_DIRECTION_ACC,
            },
        }
        _record_history(bc, symbol, mode, tf_code, df, signature, model_path, meta_path, "skipped_disabled", meta)
        return meta
    if (not force) and _has_success(bc, symbol, mode, signature, model_path):
        old = {}
        try:
            old = json.loads(meta_path.read_text())
        except Exception:
            pass
        meta = {
            **old,
            **base_metrics,
            "status": "skipped_same_signature",
            "generated_at": _now_iso(),
            "model_path": str(model_path),
            "meta_path": str(meta_path),
        }
        # Preserve any previously recorded governance fields without overwriting them.
        meta.setdefault("promote_decision", old.get("promote_decision", "hold"))
        meta.setdefault("promote_reason", old.get("promote_reason", "unchanged_signature"))
        meta.setdefault("baseline_comparison", old.get("baseline_comparison", {}))
        _record_history(bc, symbol, mode, tf_code, df, signature, model_path, meta_path, "skipped_same_signature", meta)
        return meta
    X, y, ds_meta = make_brain_dataset(df, mode, symbol, cfg)
    min_samples = int(bc.get("min_samples", 600))
    base_metrics.update(ds_meta); base_metrics["feature_count"] = len(ds_meta.get("feature_columns") or [])
    if len(X) < min_samples or len(set(y.tolist())) < 2:
        meta = {
            **base_metrics,
            "status": "skipped_insufficient_samples_or_classes",
            "samples": int(len(X)),
            "generated_at": _now_iso(),
            "promote_decision": "shadow",
            "promote_reason": "insufficient_samples_or_classes",
            "baseline_comparison": {
                "training_samples": int(len(X)),
                "min_training_samples": int(min_samples),
                "min_validation_samples": PROMOTE_MIN_VALIDATION_SAMPLES,
            },
        }
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str))
        _record_history(bc, symbol, mode, tf_code, df, signature, model_path, meta_path, meta["status"], meta)
        return meta
    horizon = horizon_for_mode(mode, cfg)
    validation_fraction = float(bc.get("validation_fraction", 0.2))
    purge_rows = max(horizon, int(bc.get("validation_purge_bars", horizon)))
    boundary = purged_holdout_boundary(
        len(X),
        validation_fraction=validation_fraction,
        minimum_train_size=max(200, int(min_samples * (1.0 - validation_fraction)) - purge_rows),
        minimum_validation_size=PROMOTE_MIN_VALIDATION_SAMPLES,
        purge_size=purge_rows,
    )
    X_train = X.iloc[boundary.train_start : boundary.train_end]
    X_val = X.iloc[boundary.validation_start : boundary.validation_end]
    y_train = y[boundary.train_start : boundary.train_end]
    y_val = y[boundary.validation_start : boundary.validation_end]
    clf = _model()
    sample_weight = np.where(y_train == LABEL_FLAT, 1.0, 1.6)
    clf.fit(X_train, y_train, clf__sample_weight=sample_weight)
    metrics = _evaluate(clf, X_val, y_val, float(ds_meta["strict_target_return"]), int(ds_meta["leverage"]))
    decision, reason, baseline = _decide_promotion(metrics, samples=int(len(X)), min_samples=int(min_samples))
    meta = {
        **base_metrics,
        **metrics,
        "status": "trained",
        "samples": int(len(X)),
        "train_samples": int(len(X_train)),
        "validation": {
            "kind": "purged_chronological_holdout",
            "train_rows": int(boundary.train_size),
            "validation_rows": int(boundary.validation_size),
            "purge_rows": int(boundary.purge_size),
            "holdout_seen_during_fit": False,
        },
        "model_path": str(model_path),
        "meta_path": str(meta_path),
        "generated_at": _now_iso(),
        "promote_decision": decision,
        "release_stage": decision,
        "promote_reason": reason,
        "baseline_comparison": baseline,
    }
    # Persist the exact governance decision inside the model bundle as well as
    # the sidecar. Inference must never infer a release stage from metrics.
    bundle = {"model": clf, "feature_columns": list(X.columns), "meta": dict(meta)}
    tmp = model_path.with_suffix('.tmp.joblib')
    joblib.dump(bundle, tmp)
    tmp.replace(model_path)
    stage_model_path, stage_meta_path = brain_stage_artifact_paths(symbol, mode, decision, cfg)
    stage_tmp = stage_model_path.with_suffix(".tmp.joblib")
    shutil.copyfile(model_path, stage_tmp)
    stage_tmp.replace(stage_model_path)
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str))
    stage_meta_tmp = stage_meta_path.with_suffix(".tmp.json")
    stage_meta_tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str))
    stage_meta_tmp.replace(stage_meta_path)
    _record_history(bc, symbol, mode, tf_code, df, signature, model_path, meta_path, "trained", meta)
    return meta


def load_brain_bundle(symbol: str, mode: str, cfg: Dict[str, Any] | None = None) -> Optional[Dict[str, Any]]:
    model_path, _ = brain_paths(symbol, mode, cfg)
    inference_stage = str(_brain_cfg(cfg).get("inference_stage") or "shadow").lower()
    if inference_stage in {"candidate", "live"}:
        model_path, _ = brain_stage_artifact_paths(symbol, mode, inference_stage, cfg)
    elif inference_stage != "shadow":
        log.error("unsupported brain inference stage %s; refusing model load", inference_stage)
        return None
    if not model_path.exists():
        return None
    try:
        return joblib.load(model_path)
    except Exception as exc:
        log.warning("load brain model failed %s-%s: %s", symbol, mode, exc)
        return None


def predict_brain_from_df(df: pd.DataFrame, symbol: str, mode: str, cfg: Dict[str, Any] | None = None, market_snapshot: Optional[Dict[str, Any]] = None, local_predicted_return: Optional[float] = None) -> Dict[str, Any]:
    bc = _brain_cfg(cfg)
    strict = target_raw_return(symbol, cfg)
    lev = symbol_leverage(symbol, cfg)
    bundle = load_brain_bundle(symbol, mode, cfg)
    if not bundle:
        return {"version": BRAIN_VERSION, "status": "missing_model", "direction": "flat", "actionable": False, "leverage": lev, "target_leveraged_profit": float(bc.get("target_leveraged_profit", DEFAULT_TARGET_LEVERAGED_PROFIT)), "target_raw_return": strict, "reason": ["brain模型不存在，保留旧LSTM/因子输出"]}
    features = build_brain_features(df, mode, symbol, market_snapshot=market_snapshot, cfg=cfg)
    cols = bundle.get("feature_columns") or list(features.columns)
    for c in cols:
        if c not in features.columns:
            features[c] = 0.0
    row = features[cols].iloc[[-1]]
    model = bundle["model"]
    proba = model.predict_proba(row)[0]
    classes = list(model.named_steps.get("clf").classes_)
    def p(label):
        return float(proba[classes.index(label)]) if label in classes else 0.0
    p_short, p_flat, p_long = p(LABEL_SHORT), p(LABEL_FLAT), p(LABEL_LONG)
    edge = p_long - p_short
    direction = "long" if edge > 0.08 and p_long >= p_flat else "short" if edge < -0.08 and p_short >= p_flat else "flat"
    confidence = max(p_long, p_short) if direction != "flat" else p_flat
    vol = float(features["realized_vol_24"].iloc[-1]) if "realized_vol_24" in features else 0.0
    expected_return = abs(edge) * max(strict, vol * 1.2, float(bc.get("min_train_threshold", 0.0012)))
    if local_predicted_return is not None and np.isfinite(local_predicted_return):
        # LSTM is only a weak prior; keep sign agreement as confidence booster, not a hard override.
        if direction == "long" and local_predicted_return > 0: confidence = min(1.0, confidence + 0.05)
        if direction == "short" and local_predicted_return < 0: confidence = min(1.0, confidence + 0.05)
    snap = market_snapshot or {}
    liq = float(snap.get("liquidation_imbalance") or 0.0) if isinstance(snap, dict) else 0.0
    reasons = [f"proba long/flat/short={p_long:.3f}/{p_flat:.3f}/{p_short:.3f}", f"目标未杠杆收益={strict:.5f}({lev}x→31%)"]
    if direction == "long" and liq > 0.2: confidence = min(1.0, confidence + 0.04); reasons.append("爆仓/空头压力偏多，增强long信心")
    if direction == "short" and liq < -0.2: confidence = min(1.0, confidence + 0.04); reasons.append("爆仓/多头压力偏空，增强short信心")
    min_conf = float(bc.get("min_confidence", 0.58))
    model_meta = dict(bundle.get("meta") or {})
    release_stage = str(
        model_meta.get("release_stage")
        or model_meta.get("promote_decision")
        or "unreviewed"
    ).lower()
    signal_qualified = bool(direction != "flat" and confidence >= min_conf and expected_return >= strict)
    actionable = bool(signal_qualified and release_stage in {"candidate", "live"})
    if not actionable:
        if direction == "flat": reasons.append("模型倾向观望")
        if confidence < min_conf: reasons.append(f"置信度{confidence:.3f}低于阈值{min_conf:.3f}")
        if expected_return < strict: reasons.append(f"预期收益{expected_return:.5f}未达到31%杠杆目标所需{strict:.5f}")
        if release_stage not in {"candidate", "live"}: reasons.append(f"模型发布阶段{release_stage}不允许形成可交易信号")
    return {
        "version": BRAIN_VERSION,
        "status": "ok",
        "direction": direction,
        "actionable": actionable,
        "signal_qualified": signal_qualified,
        "release_stage": release_stage,
        "leverage": lev,
        "target_leveraged_profit": float(bc.get("target_leveraged_profit", DEFAULT_TARGET_LEVERAGED_PROFIT)),
        "target_raw_return": strict,
        "expected_return": float(expected_return if direction != "flat" else 0.0),
        "expected_leveraged_return": float((expected_return if direction != "flat" else 0.0) * lev),
        "confidence": float(confidence),
        "proba_long": p_long,
        "proba_flat": p_flat,
        "proba_short": p_short,
        "reason": reasons,
        "model_meta": model_meta,
    }
