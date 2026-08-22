"""在线学习校准器：让预测“越用越聪明”。

设计要点
--------
* 把每次预测落入 SQLite ``data/online_learning.sqlite3``，到期后回填真实
  收益率，用累计样本估算每个 ``(symbol, timeframe, mode)`` 的预测偏置
  ``bias``、幅度缩放 ``scale``、近期方向命中率 ``hit_rate`` 与自适应阈值
  ``adaptive_threshold``。
* 不是每次都用同一套固定算法，而是根据“最近真实表现”动态修正本地模型预测：
  - 若近期预测幅度系统性偏大，``scale`` 会缩小；
  - 若近期方向偏一边，``bias`` 会反向修正；
  - 若噪声大，``adaptive_threshold`` 会放宽（避免过早判断方向）。
* 完整保留旧字段，只在结果上额外加 ``raw_*`` / ``calibrated_*`` /
  ``online_learning`` 元数据，不破坏 ``ResultManager`` / ``api_server`` 兼容。
"""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("OnlineCalibration")

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "online_learning.sqlite3"
_DEFAULT_EVAL_DIR = Path(__file__).resolve().parent.parent / "model_results" / "evaluation"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


# 在 P0 评估闭环里新增的列；旧库会通过 ALTER TABLE 增量补齐，绝不丢列。
_EXTRA_PREDICTION_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("predicted_direction", "TEXT"),
    ("confidence", "REAL"),
    ("model_version", "TEXT"),
    ("target_raw_return", "REAL"),
    ("leverage", "REAL"),
    ("current_price", "REAL"),
    ("kline_last_price", "REAL"),
    ("feature_snapshot_hash", "TEXT"),
    ("actual_price", "REAL"),
    ("actual_direction", "TEXT"),
    ("hit", "INTEGER"),
    ("cost_adjusted_return", "REAL"),
    ("settled_at", "INTEGER"),
)


def _classify_direction(value: Optional[float], threshold: float = 0.0) -> str:
    if value is None:
        return "flat"
    try:
        v = float(value)
    except Exception:
        return "flat"
    if v > threshold:
        return "long"
    if v < -threshold:
        return "short"
    return "flat"


def _safe_mean(values: List[float]) -> Optional[float]:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


class OnlinePredictionCalibrator:
    """预测结果在线校准器。

    使用方法（在推理流程内）::

        cal = OnlinePredictionCalibrator(cfg)
        cal.settle_due(...)              # 结算到期的历史预测
        adj = cal.calibrate(symbol, timeframe, mode, predicted_return, last_price)
        cal.record(symbol, timeframe, mode, predicted_return, last_price, horizon_sec)

    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = (cfg or {}).get("online_learning") if cfg and "online_learning" in cfg else (cfg or {})
        cfg = cfg or {}
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.db_path = Path(cfg.get("db_path") or _DEFAULT_DB)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lookback: int = int(cfg.get("lookback", 200))
        self.min_samples: int = int(cfg.get("min_samples", 12))
        self.base_threshold: float = float(cfg.get("base_threshold", 0.0008))
        self.min_horizon_seconds: int = int(cfg.get("min_horizon_seconds", 60))
        self.horizon_multiplier: float = float(cfg.get("horizon_multiplier", 1.0))
        self._connect_and_init()

    def _connect_and_init(self) -> None:
        try:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS predictions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER,
                    symbol TEXT,
                    timeframe TEXT,
                    mode TEXT,
                    predicted_return REAL,
                    raw_predicted_return REAL,
                    last_price REAL,
                    horizon_seconds INTEGER,
                    settle_at INTEGER,
                    actual_return REAL,
                    settled INTEGER DEFAULT 0
                )
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_predictions_key
                ON predictions(symbol, timeframe, mode, created_at)
            """)
            # 增量迁移：旧库可能缺新字段，逐列 ALTER TABLE ADD COLUMN，绝不删除/重命名旧字段。
            self._migrate_add_columns(_EXTRA_PREDICTION_COLUMNS)
            self._conn.commit()
        except Exception as exc:
            logger.warning(f"在线学习 SQLite 初始化失败: {exc}")
            self._conn = None

    def close(self) -> None:
        connection = getattr(self, "_conn", None)
        if connection is not None:
            try:
                connection.commit()
            finally:
                connection.close()
                self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _existing_columns(self) -> List[str]:
        if not self._conn:
            return []
        try:
            cur = self._conn.execute("PRAGMA table_info(predictions)")
            return [row[1] for row in cur.fetchall()]
        except Exception:
            return []

    def _migrate_add_columns(self, columns: Tuple[Tuple[str, str], ...]) -> None:
        if not self._conn:
            return
        existing = set(self._existing_columns())
        for name, col_type in columns:
            if name in existing:
                continue
            try:
                self._conn.execute(f"ALTER TABLE predictions ADD COLUMN {name} {col_type}")
                existing.add(name)
            except Exception as exc:
                # 同一列在并发场景下可能已被另一进程加上，忽略即可
                logger.debug(f"ALTER TABLE ADD COLUMN {name} 跳过: {exc}")

    # ------------------------------------------------------------ statistics
    def _recent(self, symbol: str, timeframe: str, mode: str) -> List[Tuple[float, float]]:
        if not self._conn:
            return []
        try:
            cur = self._conn.execute(
                """
                SELECT predicted_return, actual_return
                FROM predictions
                WHERE symbol=? AND timeframe=? AND mode=? AND settled=1
                ORDER BY id DESC LIMIT ?
                """,
                (symbol, timeframe, mode, self.lookback),
            )
            return [(float(p), float(a)) for p, a in cur.fetchall() if p is not None and a is not None]
        except Exception:
            return []

    def _stats(self, samples: List[Tuple[float, float]]) -> Dict[str, float]:
        if not samples:
            return {"samples": 0, "bias": 0.0, "scale": 1.0, "hit_rate": 0.5, "rmse": 0.0}
        n = len(samples)
        bias = sum(p - a for p, a in samples) / n
        # 缩放：(actual / predicted) 中位数（避免极值）
        ratios: List[float] = []
        hits = 0
        sq_err = 0.0
        for p, a in samples:
            if abs(p) > 1e-9:
                ratios.append(max(-5.0, min(5.0, a / p)))
            sq_err += (p - a) ** 2
            if (p > 0 and a > 0) or (p < 0 and a < 0) or (p == 0 and a == 0):
                hits += 1
        ratios.sort()
        scale = ratios[len(ratios) // 2] if ratios else 1.0
        if scale < 0:
            # 方向系统性反向，scale 不应反号；交给 bias 处理
            scale = 1.0
        scale = max(0.2, min(3.0, scale))
        rmse = math.sqrt(sq_err / n)
        return {
            "samples": float(n),
            "bias": bias,
            "scale": scale,
            "hit_rate": hits / float(n),
            "rmse": rmse,
        }

    def _adaptive_threshold(self, stats: Dict[str, float]) -> float:
        """RMSE 越大、命中率越低，方向阈值越大（避免噪声下过早判断）。"""
        rmse = stats.get("rmse") or 0.0
        hit = stats.get("hit_rate") or 0.5
        threshold = self.base_threshold + 0.5 * rmse
        if hit < 0.5:
            threshold *= 1.4
        return float(threshold)

    # ---------------------------------------------------------- 校准入口
    def calibrate(
        self,
        symbol: str,
        timeframe: str,
        mode: str,
        predicted_return: float,
        last_price: float,
    ) -> Dict[str, Any]:
        """对单条预测做校准，返回 ``{ raw_*, calibrated_*, online_learning }``。"""
        raw = float(predicted_return)
        info = {
            "samples": 0,
            "bias": 0.0,
            "scale": 1.0,
            "hit_rate": 0.5,
            "rmse": 0.0,
            "adaptive_threshold": self.base_threshold,
            "enabled": self.enabled,
        }
        adjusted = raw
        if self.enabled and self._conn:
            samples = self._recent(symbol, timeframe, mode)
            if len(samples) >= self.min_samples:
                stats = self._stats(samples)
                info.update(stats)
                info["adaptive_threshold"] = self._adaptive_threshold(stats)
                # 校准：先减偏置，再按缩放调整
                adjusted = (raw - info["bias"]) * info["scale"]
        threshold = info["adaptive_threshold"]
        direction = "flat"
        if adjusted > threshold:
            direction = "up"
        elif adjusted < -threshold:
            direction = "down"
        confidence = min(1.0, abs(adjusted) / (threshold + 1e-9))
        return {
            "raw_predicted_return": raw,
            "calibrated_predicted_return": adjusted,
            "calibrated_trend": direction,
            "direction_confidence": confidence,
            "online_learning": info,
        }

    # ---------------------------------------------------------- 记录预测
    def record(
        self,
        symbol: str,
        timeframe: str,
        mode: str,
        predicted_return: float,
        last_price: float,
        horizon_seconds: int,
        raw_predicted_return: Optional[float] = None,
        *,
        predicted_direction: Optional[str] = None,
        confidence: Optional[float] = None,
        model_version: Optional[str] = None,
        target_raw_return: Optional[float] = None,
        leverage: Optional[float] = None,
        current_price: Optional[float] = None,
        kline_last_price: Optional[float] = None,
        feature_snapshot_hash: Optional[str] = None,
    ) -> None:
        """记录一次预测。

        新增字段全部为可选关键字参数，保持对老调用方完全兼容。
        旧调用 ``record(sym, tf, mode, ret, price, horizon)`` 仍然有效。
        """
        if not self.enabled or not self._conn:
            return
        horizon = max(self.min_horizon_seconds, int(horizon_seconds * self.horizon_multiplier))
        now = int(time.time())
        # 当 predicted_direction 未显式给出时，根据 predicted_return 估算 long/short/flat。
        if predicted_direction is None:
            predicted_direction = _classify_direction(predicted_return, threshold=self.base_threshold)
        # 默认行为：current_price/kline_last_price 缺失时回填 last_price，保留历史口径。
        if current_price is None:
            current_price = last_price
        if kline_last_price is None:
            kline_last_price = last_price
        try:
            # 只把存在的列写进 INSERT，避免老库还没 ALTER 完成时整条插入失败。
            existing_cols = set(self._existing_columns())
            base_cols = [
                "created_at", "symbol", "timeframe", "mode",
                "predicted_return", "raw_predicted_return", "last_price",
                "horizon_seconds", "settle_at", "settled",
            ]
            base_vals: List[Any] = [
                now, symbol, timeframe, mode,
                float(predicted_return),
                float(raw_predicted_return if raw_predicted_return is not None else predicted_return),
                float(last_price),
                int(horizon),
                int(now + horizon),
                0,
            ]
            extras: List[Tuple[str, Any]] = [
                ("predicted_direction", predicted_direction),
                ("confidence", None if confidence is None else float(confidence)),
                ("model_version", model_version),
                ("target_raw_return", None if target_raw_return is None else float(target_raw_return)),
                ("leverage", None if leverage is None else float(leverage)),
                ("current_price", None if current_price is None else float(current_price)),
                ("kline_last_price", None if kline_last_price is None else float(kline_last_price)),
                ("feature_snapshot_hash", feature_snapshot_hash),
            ]
            cols: List[str] = list(base_cols)
            vals: List[Any] = list(base_vals)
            for name, value in extras:
                if value is None:
                    continue
                if name not in existing_cols:
                    continue
                cols.append(name)
                vals.append(value)
            placeholders = ",".join("?" for _ in cols)
            sql = f"INSERT INTO predictions({','.join(cols)}) VALUES ({placeholders})"
            self._conn.execute(sql, vals)
            self._conn.commit()
        except Exception as exc:
            logger.debug(f"在线学习记录失败: {exc}")

    # ---------------------------------------------------------- 结算到期
    def settle_due(self, fetch_actual_return) -> int:
        """结算到期预测。

        回调 ``fetch_actual_return(symbol, timeframe, last_price, settle_at)`` 可以：

        * 返回 ``float`` —— 仅 actual_return，老调用方继续兼容；
        * 返回 ``dict`` —— 至少含 ``actual_return``，可选 ``actual_price`` /
          ``cost_adjusted_return``，新调用方使用，能同步落地真实价格与成本后收益。

        回调失败 / 返回 ``None`` 时跳过该条。返回成功结算条数。
        """
        if not self.enabled or not self._conn:
            return 0
        now = int(time.time())
        try:
            cur = self._conn.execute(
                """
                SELECT id, symbol, timeframe, last_price, settle_at, predicted_return, predicted_direction
                FROM predictions
                WHERE settled=0 AND settle_at <= ?
                LIMIT 200
                """,
                (now,),
            )
            rows = cur.fetchall()
        except Exception:
            # 兼容旧库尚未增列的极端情况：退化到老 SELECT。
            try:
                cur = self._conn.execute(
                    """
                    SELECT id, symbol, timeframe, last_price, settle_at, predicted_return, NULL
                    FROM predictions
                    WHERE settled=0 AND settle_at <= ?
                    LIMIT 200
                    """,
                    (now,),
                )
                rows = cur.fetchall()
            except Exception:
                return 0

        existing_cols = set(self._existing_columns())
        settled = 0
        for row in rows:
            try:
                pid, symbol, timeframe, last_price, settle_at, predicted_return, predicted_direction = row
                payload = fetch_actual_return(symbol, timeframe, last_price, settle_at)
                if payload is None:
                    continue
                actual_return: Optional[float] = None
                actual_price: Optional[float] = None
                cost_adjusted_return: Optional[float] = None
                if isinstance(payload, dict):
                    if payload.get("actual_return") is not None:
                        actual_return = float(payload["actual_return"])
                    if payload.get("actual_price") is not None:
                        actual_price = float(payload["actual_price"])
                    if payload.get("cost_adjusted_return") is not None:
                        cost_adjusted_return = float(payload["cost_adjusted_return"])
                else:
                    actual_return = float(payload)
                if actual_return is None and actual_price is not None and last_price:
                    try:
                        actual_return = (actual_price - float(last_price)) / float(last_price)
                    except Exception:
                        actual_return = None
                if actual_return is None:
                    continue
                if actual_price is None and last_price:
                    try:
                        actual_price = float(last_price) * (1.0 + float(actual_return))
                    except Exception:
                        actual_price = None
                actual_direction = _classify_direction(actual_return, threshold=self.base_threshold)
                pred_dir = predicted_direction or _classify_direction(predicted_return, threshold=self.base_threshold)
                hit = 1 if (pred_dir == actual_direction and pred_dir != "flat") else (
                    1 if (pred_dir == "flat" and actual_direction == "flat") else 0
                )

                # 拼装 SET 子句，新列若旧库尚未存在则跳过该列写入。
                set_parts: List[str] = ["settled=1", "actual_return=?"]
                params: List[Any] = [float(actual_return)]
                col_value_pairs: List[Tuple[str, Any]] = [
                    ("actual_price", actual_price),
                    ("actual_direction", actual_direction),
                    ("hit", int(hit)),
                    ("cost_adjusted_return", cost_adjusted_return),
                    ("settled_at", int(time.time())),
                ]
                for name, value in col_value_pairs:
                    if name not in existing_cols:
                        continue
                    if value is None:
                        continue
                    set_parts.append(f"{name}=?")
                    params.append(value)
                params.append(int(pid))
                sql = f"UPDATE predictions SET {', '.join(set_parts)} WHERE id=?"
                self._conn.execute(sql, params)
                settled += 1
            except Exception:
                continue
        if settled:
            try:
                self._conn.commit()
            except Exception:
                pass
        return settled

    # ---------------------------------------------------------- 评估摘要导出
    def export_evaluation_summary(
        self,
        output_dir: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """根据 predictions 表导出评估摘要 JSON：

        * ``<output_dir>/summary.json`` —— 全局聚合 + 每个 (symbol, mode) 概要
        * ``<output_dir>/<SYMBOL>_<MODE>.json`` —— 每个组合的详细指标

        即便没有任何已结算样本（settled_count=0），也会输出 pending_count 与
        last_updated_at，便于前端展示“尚未结算”的真实状态，避免 P0 阶段
        伪造命中率。
        """
        out_dir = Path(output_dir) if output_dir is not None else _DEFAULT_EVAL_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        if not self._conn:
            placeholder = {
                "settled_count": 0,
                "pending_count": 0,
                "groups": {},
                "last_updated_at": _now_iso(),
                "note": "online_learning_db_unavailable",
            }
            try:
                (out_dir / "summary.json").write_text(
                    json.dumps(placeholder, indent=2, ensure_ascii=False)
                )
            except Exception:
                pass
            return placeholder

        existing_cols = set(self._existing_columns())
        # 老库缺新列时，对应字段读 NULL，避免 SELECT 失败。
        def _col(name: str) -> str:
            return name if name in existing_cols else "NULL"

        cols_select = ", ".join([
            "symbol", "mode", "settled", "predicted_return",
            _col("actual_return"), _col("predicted_direction"),
            _col("actual_direction"), _col("hit"),
            _col("cost_adjusted_return"),
        ])
        try:
            cur = self._conn.execute(
                f"SELECT {cols_select} FROM predictions"
            )
            rows = cur.fetchall()
        except Exception as exc:
            logger.debug(f"评估摘要 SELECT 失败: {exc}")
            rows = []

        groups: Dict[Tuple[str, str], Dict[str, List[Any]]] = {}
        for row in rows:
            symbol, mode, settled, predicted_return, actual_return, pred_dir, act_dir, hit, cost_adj = row
            key = (str(symbol or ""), str(mode or ""))
            g = groups.setdefault(key, {
                "settled_count": 0,
                "pending_count": 0,
                "pred_dirs": [],
                "act_dirs": [],
                "hits": [],
                "predicted_returns": [],
                "actual_returns": [],
                "cost_adjusted_returns": [],
            })
            if int(settled or 0) == 1:
                g["settled_count"] += 1
                if pred_dir is not None:
                    g["pred_dirs"].append(str(pred_dir))
                else:
                    g["pred_dirs"].append(_classify_direction(predicted_return, threshold=self.base_threshold))
                if act_dir is not None:
                    g["act_dirs"].append(str(act_dir))
                else:
                    g["act_dirs"].append(_classify_direction(actual_return, threshold=self.base_threshold))
                if hit is not None:
                    g["hits"].append(int(hit))
                if predicted_return is not None:
                    g["predicted_returns"].append(float(predicted_return))
                if actual_return is not None:
                    g["actual_returns"].append(float(actual_return))
                if cost_adj is not None:
                    g["cost_adjusted_returns"].append(float(cost_adj))
            else:
                g["pending_count"] += 1

        now_iso = _now_iso()
        per_group: Dict[str, Dict[str, Any]] = {}
        total_settled = 0
        total_pending = 0
        for (symbol, mode), g in groups.items():
            settled_count = int(g["settled_count"])
            pending_count = int(g["pending_count"])
            total_settled += settled_count
            total_pending += pending_count
            pred_dirs: List[str] = g["pred_dirs"]
            act_dirs: List[str] = g["act_dirs"]
            hits: List[int] = g["hits"]
            # 命中率优先用入库 hit，缺失时回退到 pred==act 比较。
            if hits:
                hit_rate = sum(hits) / float(len(hits))
            elif pred_dirs and act_dirs:
                pair_hits = sum(1 for p, a in zip(pred_dirs, act_dirs) if p == a)
                hit_rate = pair_hits / float(len(pred_dirs))
            else:
                hit_rate = None
            long_total = sum(1 for d in pred_dirs if d == "long")
            short_total = sum(1 for d in pred_dirs if d == "short")
            flat_total = sum(1 for d in pred_dirs if d == "flat")
            long_correct = sum(1 for p, a in zip(pred_dirs, act_dirs) if p == "long" and a == "long")
            short_correct = sum(1 for p, a in zip(pred_dirs, act_dirs) if p == "short" and a == "short")
            flat_correct = sum(1 for p, a in zip(pred_dirs, act_dirs) if p == "flat" and a == "flat")
            long_precision = (long_correct / long_total) if long_total else None
            short_precision = (short_correct / short_total) if short_total else None
            flat_accuracy = (flat_correct / flat_total) if flat_total else None
            summary_entry: Dict[str, Any] = {
                "symbol": symbol,
                "mode": mode,
                "settled_count": settled_count,
                "pending_count": pending_count,
                "hit_rate": hit_rate,
                "long_precision": long_precision,
                "short_precision": short_precision,
                "flat_accuracy": flat_accuracy,
                "avg_predicted_return": _safe_mean(g["predicted_returns"]),
                "avg_actual_return": _safe_mean(g["actual_returns"]),
                "avg_cost_adjusted_return": _safe_mean(g["cost_adjusted_returns"]),
                "last_updated_at": now_iso,
            }
            per_group[f"{symbol}_{mode}"] = summary_entry
            # 写每个 symbol_mode 详细 JSON
            try:
                detail_path = out_dir / f"{symbol}_{mode}.json"
                detail_path.write_text(
                    json.dumps(summary_entry, indent=2, ensure_ascii=False)
                )
            except Exception as exc:
                logger.debug(f"写 {symbol}_{mode}.json 评估失败: {exc}")

        summary: Dict[str, Any] = {
            "settled_count": total_settled,
            "pending_count": total_pending,
            "groups": per_group,
            "last_updated_at": now_iso,
        }
        try:
            (out_dir / "summary.json").write_text(
                json.dumps(summary, indent=2, ensure_ascii=False)
            )
        except Exception as exc:
            logger.debug(f"写 summary.json 失败: {exc}")
        return summary


__all__ = ["OnlinePredictionCalibrator", "_DEFAULT_EVAL_DIR"]
