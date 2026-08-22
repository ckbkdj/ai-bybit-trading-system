"""市场上下文 / 多因子融合中心。

模块职责：
1. 把本地 ``data/{BASE}.json`` 爆仓图与 ``data/coinglass_metrics/*.json`` 指标
   抽象成统一的 ``MarketFeatureSnapshot``。
2. 提供：

   - ``build_market_feature_snapshot``：构造预测期所用的特征快照
   - ``MARKET_FEATURE_COLUMNS`` / ``NEWS_FEATURE_COLUMNS``：统一列定义
   - ``compute_market_bias``：把市场结构因子折成 ``[-1, 1]`` 方向偏置
   - ``assess_context_completeness``：评估数据完整度
   - ``adaptive_context_weights``：完整度门控的自适应权重
   - ``fuse_direction_signals``：本地模型 + 市场因子 + 新闻 + LLM 辅助 多路融合
   - ``OpenAIFormatSignalClient``：OpenAI 兼容辅助预测器（仅做配置，失败返回中性）
3. 该模块不引入旧三方新闻 API，不强行加载 keras / tensorflow / talib。
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("MarketContext")


# ---------------------------------------------------------------------------
# 列定义
# ---------------------------------------------------------------------------

# 主结构因子（爆仓图 / 资金 / 持仓 / 成交 / 多空）
MARKET_FEATURE_COLUMNS: List[str] = [
    "funding_rate",
    "funding_acceleration",
    "long_short_ratio",
    "long_short_ratio_change",
    "open_interest_amount",
    "open_interest_change",
    "open_interest_value",
    "open_interest_notional_change",
    "volume_24h",
    "volume_24h_change",
    "volume_24h_notional",
    "volume_24h_notional_change",
    "long_liquidation_usd",
    "short_liquidation_usd",
    "total_liquidation_usd",
    "liquidation_imbalance",
    "liq_short_pressure_log",
    "liq_long_pressure_log",
    "liq_nearest_short_distance",
    "liq_nearest_long_distance",
    "liq_heat_total_log",
    "liq_levels_total",
    "taker_buy_sell_ratio",
    "top_trader_long_short_ratio",
]

# 新闻 / 上下文增强特征
NEWS_FEATURE_COLUMNS: List[str] = [
    "news_sentiment",
    "financial_calendar_score",
    "whale_alert_score",
    "fear_greed_score",
    "news_context_score",
    "macro_event_importance",
    "whale_net_flow_score",
]


# ---------------------------------------------------------------------------
# IO 工具
# ---------------------------------------------------------------------------

def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {"data": data}
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _source_timestamp(payload: Dict[str, Any], path: Path, *, allow_mtime: bool) -> Optional[float]:
    value = payload.get("ts") or payload.get("generated_at")
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    if isinstance(value, str) and value.strip():
        text = value.strip().replace("/", "-")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.astimezone()
            return parsed.timestamp()
        except ValueError:
            pass
    if allow_mtime:
        try:
            return path.stat().st_mtime
        except OSError:
            return None
    return None


def repair_mojibake_value(value: Any) -> Any:
    """Recursively repair UTF-8 Chinese text accidentally decoded as latin1/cp1252.

    Some OpenAI-compatible proxy/SSE gateways have returned strings like
    ``æä»éä¸é`` instead of ``持仓量下降``.  Keep the original value
    unless the latin1->utf-8 roundtrip clearly improves it.
    """
    def _score(text: str) -> int:
        return sum(text.count(ch) for ch in ("Ã", "Â", "æ", "è", "é", "å", "ä", "ç", "ï", "ð"))

    if isinstance(value, str):
        if _score(value) < 2:
            return value
        candidates = []
        for enc in ("latin1", "cp1252"):
            try:
                candidates.append(value.encode(enc).decode("utf-8"))
            except Exception:
                pass
        best = value
        best_score = _score(value)
        for cand in candidates:
            cand_score = _score(cand)
            if cand and cand_score < best_score:
                best = cand
                best_score = cand_score
        return best
    if isinstance(value, list):
        return [repair_mojibake_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(repair_mojibake_value(v) for v in value)
    if isinstance(value, dict):
        return {k: repair_mojibake_value(v) for k, v in value.items()}
    return value


def _safe_log1p_signed(x: float) -> float:
    sign = 1.0 if x >= 0 else -1.0
    return sign * math.log1p(min(1e12, abs(float(x))))


# ---------------------------------------------------------------------------
# 爆仓图摘要
# ---------------------------------------------------------------------------

def summarize_liquidation_map(payload: Dict[str, Any]) -> Dict[str, float]:
    """对单币爆仓图 ``data/{BASE}.json`` 生成结构化摘要。"""
    out = {
        "long_pressure": 0.0,
        "short_pressure": 0.0,
        "imbalance": 0.0,
        "nearest_long_distance_ratio": 0.0,
        "nearest_short_distance_ratio": 0.0,
        "heat_total": 0.0,
        "levels_total": 0,
        "last_price": 0.0,
    }
    if not isinstance(payload, dict):
        return out
    try:
        last_price = float(payload.get("lastPrice") or 0.0)
        liq = payload.get("liqMapV2") or {}
        if last_price <= 0 or not liq:
            return out
        long_p = 0.0
        short_p = 0.0
        nearest_long: Optional[float] = None
        nearest_short: Optional[float] = None
        levels = 0
        for k, entries in liq.items():
            try:
                p = float(k)
            except Exception:
                continue
            for entry in entries or ():
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                try:
                    h = float(entry[1])
                except Exception:
                    continue
                levels += 1
                if p < last_price:
                    long_p += h
                    d = (last_price - p) / last_price
                    if nearest_long is None or d < nearest_long:
                        nearest_long = d
                elif p > last_price:
                    short_p += h
                    d = (p - last_price) / last_price
                    if nearest_short is None or d < nearest_short:
                        nearest_short = d
        denom = long_p + short_p
        imbalance = 0.0 if denom <= 0 else (short_p - long_p) / denom
        out.update({
            "long_pressure": long_p,
            "short_pressure": short_p,
            "imbalance": max(-1.0, min(1.0, imbalance)),
            "nearest_long_distance_ratio": nearest_long or 0.0,
            "nearest_short_distance_ratio": nearest_short or 0.0,
            "heat_total": denom,
            "levels_total": int(levels),
            "last_price": last_price,
        })
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# 单值 + 变化提取（供 data_fetch 复用）
# ---------------------------------------------------------------------------

def _extract_last_and_change(payload: Any) -> "tuple[Optional[float], float]":
    """从多种 Coinglass 负载中抽取最新值与变化率。

    支持：
    - dict 单值（含 ``value`` / ``last`` 字段）
    - list of dicts 的时序（取最后两点比较）
    - list of [ts, value] 的 chart payload
    - dict[str, list-of-dicts]
    - dict[str, scalar]
    返回 ``(last, change_ratio)``，其中 ``change_ratio`` 为相对变化（可为 0）。
    若提取不到则返回 ``(None, 0.0)``。
    """
    def _to_float(v: Any) -> Optional[float]:
        try:
            return float(v)
        except Exception:
            return None

    def _series(values: List[float]) -> "tuple[Optional[float], float]":
        nums = [v for v in values if v is not None]
        if not nums:
            return None, 0.0
        last = nums[-1]
        prev = nums[-2] if len(nums) >= 2 else last
        if prev is None or abs(prev) < 1e-12:
            return last, 0.0
        return last, (last - prev) / abs(prev)

    if payload is None:
        return None, 0.0

    if isinstance(payload, (int, float)):
        return float(payload), 0.0

    if isinstance(payload, list):
        # list of [ts, value]
        if all(isinstance(x, (list, tuple)) and len(x) >= 2 for x in payload):
            return _series([_to_float(x[1]) for x in payload])
        # list of dicts，取常见字段
        if all(isinstance(x, dict) for x in payload):
            keys = ("value", "openInterest", "longShortRatio", "fundingRate", "vol", "volume", "rate", "ratio")
            for k in keys:
                vals = [_to_float(x.get(k)) for x in payload if k in x]
                if any(v is not None for v in vals):
                    return _series(vals)
            # 最后兜底：任何数值字段
            vals = []
            for x in payload:
                for v in x.values():
                    f = _to_float(v)
                    if f is not None:
                        vals.append(f)
                        break
            return _series(vals)
        return _series([_to_float(x) for x in payload])

    if isinstance(payload, dict):
        # 优先看常见 last/change 字段
        for k in ("value", "last", "lastValue", "openInterest", "longShortRatio", "fundingRate"):
            if k in payload:
                last = _to_float(payload.get(k))
                if last is not None:
                    # 如果同时有 change 字段
                    chg = _to_float(payload.get("change") or payload.get("changePercent") or payload.get("h24Change"))
                    if chg is not None:
                        return last, chg / 100.0 if abs(chg) > 1.0 else chg
                    return last, 0.0
        # values is list?
        for k, v in payload.items():
            if isinstance(v, list):
                last, chg = _extract_last_and_change(v)
                if last is not None:
                    return last, chg
        # dict of scalars
        nums = []
        for v in payload.values():
            f = _to_float(v)
            if f is not None:
                nums.append(f)
        return _series(nums)

    return None, 0.0


# ---------------------------------------------------------------------------
# 特征快照 / 因子 bias / 完整度 / 自适应权重
# ---------------------------------------------------------------------------

def build_market_feature_snapshot(
    *,
    liqmap_payload: Optional[Dict[str, Any]] = None,
    metrics_dir: Optional[Path] = None,
    base: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """构造统一的市场特征快照。

    本函数只读取本地 Coinglass 指标 wrapper，不发任何网络请求。
    """
    feat: Dict[str, float] = {col: 0.0 for col in MARKET_FEATURE_COLUMNS + NEWS_FEATURE_COLUMNS}

    if liqmap_payload:
        liq_summary = summarize_liquidation_map(liqmap_payload)
        feat["liquidation_imbalance"] = liq_summary["imbalance"]
        feat["liq_short_pressure_log"] = _safe_log1p_signed(liq_summary["short_pressure"])
        feat["liq_long_pressure_log"] = _safe_log1p_signed(liq_summary["long_pressure"])
        feat["liq_nearest_short_distance"] = liq_summary["nearest_short_distance_ratio"]
        feat["liq_nearest_long_distance"] = liq_summary["nearest_long_distance_ratio"]
        feat["liq_heat_total_log"] = _safe_log1p_signed(liq_summary["heat_total"])
        feat["liq_levels_total"] = float(liq_summary["levels_total"])

    if metrics_dir is not None and base:
        # funding_rate
        fr = _safe_load_json(metrics_dir / f"{base}_funding_rate.json")
        if fr and fr.get("status") == "ok":
            last, chg = _extract_last_and_change(fr.get("data"))
            if last is not None:
                feat["funding_rate"] = float(last)
                feat["funding_acceleration"] = float(chg)
        # long_short_ratio
        ls = _safe_load_json(metrics_dir / f"{base}_long_short_ratio.json")
        if ls and ls.get("status") == "ok":
            last, chg = _extract_last_and_change(ls.get("data"))
            if last is not None:
                feat["long_short_ratio"] = float(last)
                feat["long_short_ratio_change"] = float(chg)
        # open_interest
        oi = _safe_load_json(metrics_dir / f"{base}_open_interest.json")
        if oi and oi.get("status") == "ok":
            data = oi.get("data") or {}
            if isinstance(data, dict):
                feat["open_interest_amount"] = float(data.get("openInterest") or data.get("h24Vol") or 0.0)
                feat["open_interest_value"] = float(data.get("h24VolUsd") or data.get("volUsd") or 0.0)
                feat["open_interest_change"] = float(data.get("h24OIChangePercent") or data.get("oichangePercent") or 0.0) / 100.0
                feat["open_interest_notional_change"] = float(data.get("h4OIChangePercent") or 0.0) / 100.0
            else:
                last, chg = _extract_last_and_change(data)
                if last is not None:
                    feat["open_interest_amount"] = float(last)
                    feat["open_interest_change"] = float(chg)
        # volume_24h
        vol = _safe_load_json(metrics_dir / f"{base}_volume_24h.json")
        if vol and vol.get("status") == "ok":
            last, chg = _extract_last_and_change(vol.get("data"))
            if last is not None:
                feat["volume_24h"] = float(last)
                feat["volume_24h_change"] = float(chg)
                feat["volume_24h_notional"] = float(last)
                feat["volume_24h_notional_change"] = float(chg)
        # liquidation_today
        liq_today = _safe_load_json(metrics_dir / f"{base}_liquidation_today.json")
        if liq_today and liq_today.get("status") == "ok":
            data = liq_today.get("data") or {}
            if isinstance(data, dict):
                long_liq = float(data.get("longLiquidationUsd") or 0.0)
                short_liq = float(data.get("shortLiquidationUsd") or 0.0)
                total_liq = float(data.get("liquidationUsd") or (long_liq + short_liq))
                feat["long_liquidation_usd"] = long_liq
                feat["short_liquidation_usd"] = short_liq
                feat["total_liquidation_usd"] = total_liq

        # ---- news / 上下文 ----
        nc = _safe_load_json(metrics_dir / "news_context.json")
        if nc:
            scores = nc.get("scores") or {}
            feat["news_context_score"] = float(scores.get("news_context_score") or 0.0)
            feat["financial_calendar_score"] = float(scores.get("financial_calendar_score") or 0.0)
            feat["whale_alert_score"] = float(scores.get("whale_alert_score") or 0.0)
            feat["whale_net_flow_score"] = float(scores.get("whale_net_flow_score") or 0.0)
            feat["fear_greed_score"] = float(scores.get("fear_greed_score") or 0.0)
            feat["macro_event_importance"] = float(scores.get("macro_event_importance") or 0.0)
        # news_sentiment 由 events.json 直接派生
        ev = _safe_load_json(metrics_dir / "events.json")
        if ev:
            items = ev.get("items") or []
            if items:
                vals = [float(x.get("score") or x.get("sentiment_score") or 0.0) for x in items if isinstance(x, dict)]
                if vals:
                    feat["news_sentiment"] = float(sum(vals) / len(vals))

    if extra:
        for k, v in extra.items():
            try:
                feat[k] = float(v)
            except Exception:
                continue
    return feat


def append_market_columns(df, snapshot: Dict[str, float]):
    """把快照写到 DataFrame 末尾几列；本函数延迟导入 pandas。"""
    try:
        import pandas as pd  # noqa: F401
    except Exception:
        return df
    for col in MARKET_FEATURE_COLUMNS + NEWS_FEATURE_COLUMNS:
        df[col] = snapshot.get(col, 0.0)
    return df


def assess_context_completeness(
    metrics_dir: Optional[Path],
    base: Optional[str],
    *,
    data_dir: Optional[Path] = None,
    now_epoch: Optional[float] = None,
) -> Dict[str, Any]:
    """评估当前数据栈完整度。完整度越高，新闻上下文权重越高。

    参数 ``data_dir`` 用于定位爆仓图原文件 ``data/{BASE}.json``；
    若未传入则从 ``metrics_dir`` 的父目录推断。
    """
    sources: Dict[str, bool] = {}
    if metrics_dir is None or not base:
        return {"score": 0.0, "sources": sources, "missing": [], "generated_at": _now_iso()}
    if data_dir is None:
        # 兜底：metrics_dir 的父目录就是 data 根目录
        try:
            data_dir = Path(metrics_dir).resolve().parent
        except Exception:
            data_dir = None
    liqmap_path = (data_dir / f"{base}.json") if data_dir else None
    checks: List[tuple] = [
        ("liqmap", liqmap_path, 600, True),
        ("funding_rate", metrics_dir / f"{base}_funding_rate.json", 1800, False),
        ("open_interest", metrics_dir / f"{base}_open_interest.json", 900, False),
        ("long_short_ratio", metrics_dir / f"{base}_long_short_ratio.json", 900, False),
        ("volume_24h", metrics_dir / f"{base}_volume_24h.json", 900, False),
        ("liquidation_today", metrics_dir / f"{base}_liquidation_today.json", 900, False),
        ("news_context", metrics_dir / "news_context.json", 3600, False),
        ("financial_calendar", metrics_dir / "financial_calendar.json", 86400, False),
        ("whale_alert", metrics_dir / "whale_alert.json", 3600, False),
        ("fear_greed_index", metrics_dir / "fear_greed_index.json", 90000, False),
    ]
    ok = 0
    missing: List[str] = []
    stale: List[str] = []
    observed_now = float(now_epoch if now_epoch is not None else time.time())
    source_age_seconds: Dict[str, Optional[float]] = {}
    for name, path, maximum_age, allow_mtime in checks:
        try:
            if path is None:
                sources[name] = False
                missing.append(name)
                continue
            wrapper = _safe_load_json(path)
            if not wrapper:
                sources[name] = False
                missing.append(name)
                continue
            status = wrapper.get("status")
            # 爆仓图原始 JSON 没有 status 字段，但只要有 lastPrice/liqMapV2 就视为 ok
            structurally_ok = bool(
                status == "ok"
                or wrapper.get("lastPrice")
                or wrapper.get("liqMapV2")
                or (name == "news_context" and wrapper.get("scores"))
            )
            timestamp = _source_timestamp(wrapper, path, allow_mtime=allow_mtime)
            age = None if timestamp is None else max(0.0, observed_now - timestamp)
            source_age_seconds[name] = age
            is_fresh = age is not None and age <= maximum_age
            if structurally_ok and is_fresh and not bool(wrapper.get("synthetic", False)):
                sources[name] = True
                ok += 1
            else:
                sources[name] = False
                if structurally_ok and age is not None and age > maximum_age:
                    stale.append(name)
                else:
                    missing.append(name)
        except Exception:
            sources[name] = False
            missing.append(name)
    score = ok / float(len(checks)) if checks else 0.0
    return {
        "score": score,
        "sources": sources,
        "missing": missing,
        "stale": stale,
        "source_age_seconds": source_age_seconds,
        "generated_at": _now_iso(),
    }


def adaptive_context_weights(
    completeness: Dict[str, Any],
    *,
    news_context_score: float = 0.0,
    macro_event_importance: float = 0.0,
) -> Dict[str, float]:
    """完整度门控 + 事件强度调权。

    返回归一化权重字典；爆仓图 / 资金 / 持仓仍是主锚点。
    """
    score = float(completeness.get("score", 0.0))
    base = {
        "liquidation_map": 0.30,
        "funding_oi_volume": 0.25,
        "long_short_taker": 0.15,
        "local_model": 0.20,
        "news_context": 0.05,
        "llm_aux": 0.05,
    }
    # 完整度越高，新闻 + LLM 权重越高（最多再各加 0.08 / 0.04）
    base["news_context"] = 0.05 + 0.08 * score
    base["llm_aux"] = 0.05 + 0.04 * score
    # 强宏观事件再略上调新闻
    base["news_context"] = min(0.20, base["news_context"] + 0.05 * float(macro_event_importance))

    # 归一化
    total = sum(base.values())
    if total <= 0:
        return base
    return {k: v / total for k, v in base.items()}


def compute_market_bias(
    snapshot: Dict[str, float],
    completeness: Dict[str, Any],
) -> Dict[str, Any]:
    """把市场结构 + 新闻 + 完整度折成 ``[-1, 1]`` 方向偏置 ``factor_bias``。"""
    weights = adaptive_context_weights(
        completeness,
        news_context_score=float(snapshot.get("news_context_score") or 0.0),
        macro_event_importance=float(snapshot.get("macro_event_importance") or 0.0),
    )

    sources = completeness.get("sources") or {}

    def available(name: str) -> bool:
        return bool(sources.get(name, True)) if sources else True

    # Each component is masked when its source is absent, stale or synthetic.
    liq = float(snapshot.get("liquidation_imbalance") or 0.0) if available("liqmap") else 0.0
    fund = math.tanh(float(snapshot.get("funding_rate") or 0.0) * 50.0) if available("funding_rate") else 0.0
    oi = math.tanh(float(snapshot.get("open_interest_change") or 0.0) * 5.0) if available("open_interest") else 0.0
    vol = math.tanh(float(snapshot.get("volume_24h_change") or 0.0) * 2.0) if available("volume_24h") else 0.0
    ls = math.tanh((float(snapshot.get("long_short_ratio") or 1.0) - 1.0) * 2.0) if available("long_short_ratio") else 0.0
    lsc = math.tanh(float(snapshot.get("long_short_ratio_change") or 0.0) * 2.0) if available("long_short_ratio") else 0.0
    taker = math.tanh((float(snapshot.get("taker_buy_sell_ratio") or 1.0) - 1.0) * 2.0)
    news = float(snapshot.get("news_context_score") or 0.0) if available("news_context") else 0.0
    fg = float(snapshot.get("fear_greed_score") or 0.0) if available("fear_greed_index") else 0.0
    wh = float(snapshot.get("whale_alert_score") or 0.0) if available("whale_alert") else 0.0

    funding_oi_volume = (fund + oi + vol) / 3.0
    long_short_taker = (ls + lsc + taker) / 3.0
    news_signal = (news + 0.5 * fg + 0.5 * wh) / 2.0

    bias = (
        weights["liquidation_map"] * liq
        + weights["funding_oi_volume"] * funding_oi_volume
        + weights["long_short_taker"] * long_short_taker
        + weights["news_context"] * news_signal
    )
    bias = max(-1.0, min(1.0, bias))

    news_weight_total = weights["news_context"] + weights["llm_aux"]

    return {
        "factor_bias": bias,
        "components": {
            "liquidation_imbalance": liq,
            "funding_oi_volume": funding_oi_volume,
            "long_short_taker": long_short_taker,
            "news_signal": news_signal,
        },
        "weights": weights,
        "news_weight_total": news_weight_total,
        "context_completeness": completeness,
    }


def fuse_direction_signals(
    *,
    local_predicted_return: float,
    factor_bias: float,
    news_signal: float,
    llm_signal: float,
    completeness: Dict[str, Any],
    llm_available: bool = True,
) -> Dict[str, Any]:
    """四路融合：本地模型 + 市场因子 + 新闻 + LLM 辅助。"""
    score = float(completeness.get("score", 0.0))
    # 完整度越高，新闻 / LLM 权重越高（但本地模型仍主导）
    local_w = 0.55 - 0.05 * score      # 至少 0.50
    sources = completeness.get("sources") or {}
    source_known = bool(sources)
    factor_available = (
        any(
            bool(sources.get(name))
            for name in (
                "liqmap", "funding_rate", "open_interest", "long_short_ratio",
                "volume_24h", "liquidation_today",
            )
        )
        if source_known else True
    )
    news_available = bool(sources.get("news_context")) if source_known else True
    factor_w = 0.20 if factor_available else 0.0
    news_w = (0.10 + 0.08 * score) if news_available else 0.0
    llm_w = (0.10 + 0.04 * score) if llm_available else 0.0
    s = local_w + factor_w + news_w + llm_w
    local_w, factor_w, news_w, llm_w = local_w / s, factor_w / s, news_w / s, llm_w / s

    fused = (
        local_w * math.tanh(local_predicted_return * 50.0)
        + factor_w * factor_bias
        + news_w * news_signal
        + llm_w * llm_signal
    )
    fused = max(-1.0, min(1.0, fused))
    direction = "flat"
    if fused > 0.10:
        direction = "up"
    elif fused < -0.10:
        direction = "down"
    return {
        "fused_score": fused,
        "fused_weights": {
            "local_model": local_w,
            "factor_bias": factor_w,
            "news_context": news_w,
            "llm_aux": llm_w,
        },
        "direction": direction,
        "context_completeness": completeness,
    }


# ---------------------------------------------------------------------------
# OpenAI 兼容辅助预测（双预测的 LLM 通道）
# ---------------------------------------------------------------------------

class OpenAIFormatSignalClient:
    """OpenAI 兼容 ``/v1/chat/completions`` 的辅助预测客户端。

    设计原则：
    * 默认 ``enabled=False``，不会发任何请求；任何配置错误 / 网络失败都返回
      中性 ``score=0.0`` 与 ``error`` 字段，绝不让本地预测链路崩溃。
    * 响应必须严格 JSON，否则也回退中性。
    * 落盘到 ``model_results/llm_aux/{SYMBOL}_{mode}.json``，包含
      generated_at / direction / score / confidence / prediction_value /
      summary（中文）/ anchors / risk_flags / data_sources_generated_at。
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        self.enabled: bool = bool(cfg.get("enabled", False))
        self.base_url: str = str(
            os.environ.get("AI_BOT_LLM_BASE_URL") or cfg.get("base_url") or ""
        ).rstrip("/")
        self.api_key: str = str(
            os.environ.get("AI_BOT_LLM_API_KEY") or cfg.get("api_key") or ""
        )
        self.model: str = str(
            os.environ.get("AI_BOT_LLM_MODEL") or cfg.get("model") or "gpt-4o-mini"
        )
        raw_fallback_models = cfg.get("fallback_models") or cfg.get("model_fallbacks") or []
        if isinstance(raw_fallback_models, str):
            raw_fallback_models = [raw_fallback_models]
        self.fallback_models: List[str] = []
        for _m in list(raw_fallback_models or []):
            _name = str(_m or "").strip()
            if _name and _name != self.model and _name not in self.fallback_models:
                self.fallback_models.append(_name)
        self.timeout: int = int(cfg.get("timeout") or 120)
        self.connect_timeout: int = int(cfg.get("connect_timeout") or 10)
        self.stream: bool = bool(cfg.get("stream", True))
        self.cache_ttl: int = int(cfg.get("cache_ttl") or 300)
        persist_dir = Path(cfg.get("persist_dir") or "./model_results/llm_aux")
        if not persist_dir.is_absolute():
            # Resolve relative paths against the project root so nohup/systemd CWD
            # differences cannot write stale aux files outside /mnt/ai_bot.
            persist_dir = Path(__file__).resolve().parent.parent / persist_dir
        self.persist_dir = persist_dir.resolve()
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Dict[str, Any]] = {}

    # -------------------------------- 小工具：构造默认中性返回
    def _neutral(self, symbol: str, mode: str, error: str = "") -> Dict[str, Any]:
        return {
            "generated_at": _now_iso(),
            "symbol": symbol,
            "mode": mode,
            "direction": "flat",
            "score": 0.0,
            "confidence": 0.0,
            "prediction_value": 0.0,
            "summary": "OpenAI 辅助预测未启用或调用失败，已返回中性结果，本地预测仍然有效。",
            "anchors": [],
            "risk_flags": [],
            "data_sources_generated_at": {},
            "status": "disabled" if not self.enabled else "error",
            "error": error,
        }

    def _chat_endpoint(self) -> str:
        base = self.base_url.rstrip("/")
        if base.endswith("/v1"):
            return f"{base}/chat/completions"
        return f"{base}/v1/chat/completions"

    @staticmethod
    def _strip_thinking(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"(?im)^\s*(reasoning|thinking|思考过程|推理过程)\s*[:：].*$", "", text)
        return text.strip()

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        """Parse JSON from common OpenAI-compatible responses.

        Some Qwen/proxy gateways ignore response_format and return fenced JSON or
        a short preface followed by an object. Keep this parser strict enough to
        avoid fake success, but tolerant of markdown wrappers.
        """
        if not text:
            return {}
        candidates = [text.strip()]
        fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        if fence:
            candidates.insert(0, fence.group(1).strip())
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            candidates.append(text[start:end + 1].strip())
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                continue
        return {}

    @staticmethod
    def _content_from_chat_response(data: Dict[str, Any]) -> str:
        message = (data.get("choices") or [{}])[0].get("message") or {}
        return str(message.get("content") or "")

    @staticmethod
    def _reasoning_from_chat_response(data: Dict[str, Any]) -> str:
        message = (data.get("choices") or [{}])[0].get("message") or {}
        return str(message.get("reasoning") or message.get("reasoning_content") or "")

    @staticmethod
    def _content_from_stream_line(line: str) -> str:
        if not line:
            return ""
        line = line.strip()
        if not line or line == "data: [DONE]":
            return ""
        if line.startswith("data:"):
            line = line[5:].strip()
        try:
            data = json.loads(line)
        except Exception:
            return ""
        delta = (data.get("choices") or [{}])[0].get("delta") or {}
        # Tolerate Qwen / reasoning-only gateways: ignore reasoning_content /
        # reasoning fields entirely so we never mix internal CoT into the
        # user-facing content stream. Only the explicit ``content`` field is
        # treated as the final answer.
        content_val = delta.get("content")
        if content_val is None:
            # Some gateways emit chunks that ONLY carry reasoning_content /
            # reasoning. Skip those silently — they are not user-facing text.
            if ("reasoning_content" in delta) or ("reasoning" in delta):
                return ""
            return ""
        return str(content_val or "")

    def _post_chat(self, requests_mod: Any, body: Dict[str, Any]) -> tuple[str, str]:
        """Call OpenAI-compatible chat API, streaming when enabled.

        Streaming prevents slow Qwen/proxy responses from hitting a short full-body
        read timeout as long as chunks arrive. Returns (content, reasoning).
        """
        timeout = (self.connect_timeout, self.timeout)
        req_body = dict(body)
        if self.stream:
            req_body["stream"] = True
        resp = requests_mod.post(
            self._chat_endpoint(),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=req_body,
            timeout=timeout,
            stream=self.stream,
        )
        resp.raise_for_status()
        if not self.stream:
            data = resp.json()
            return self._content_from_chat_response(data), self._reasoning_from_chat_response(data)
        chunks: List[str] = []
        for raw in resp.iter_lines(decode_unicode=True):
            piece = self._content_from_stream_line(str(raw or ""))
            if piece:
                chunks.append(piece)
        return "".join(chunks), ""

    def _persist(self, payload: Dict[str, Any]) -> None:
        try:
            sym = str(payload.get("symbol") or "UNKNOWN")
            mode = str(payload.get("mode") or "default")
            path = self.persist_dir / f"{sym}_{mode}.json"
            tmp = path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            tmp.replace(path)
        except Exception as exc:
            logger.debug(f"OpenAI 辅助结果落盘失败: {exc}")

    def predict(
        self,
        *,
        symbol: str,
        mode: str,
        snapshot: Dict[str, float],
        completeness: Dict[str, Any],
        data_sources_generated_at: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """同步调用，返回 ``{ direction, score, confidence, prediction_value, summary, ... }``。

        失败时返回中性 ``score=0.0`` 与 ``error`` 字段。
        """
        if not self.enabled or not self.base_url or not self.api_key:
            payload = self._neutral(symbol, mode, error="not configured")
            payload["data_sources_generated_at"] = data_sources_generated_at or {}
            self._persist(payload)
            return payload

        cache_key = f"{symbol}:{mode}"
        cached = self._cache.get(cache_key)
        if cached and time.time() - cached.get("_cached_at", 0) < self.cache_ttl:
            return cached["payload"]

        try:
            import requests
        except Exception:
            payload = self._neutral(symbol, mode, error="requests unavailable")
            payload["data_sources_generated_at"] = data_sources_generated_at or {}
            self._persist(payload)
            return payload

        # 构造 prompt（中文摘要 / 严格 JSON）
        prompt = (
            "你是一个量化研判助手。基于下列 Coinglass 多维度本地结构化指标快照，"
            "请只输出严格的 JSON，字段为：direction(必须是 up/down/flat)、"
            "score(范围-1到1)、confidence(范围0到1)、prediction_value(浮点)、"
            "summary(中文不超过 200 字)、anchors(中文列表)、risk_flags(中文列表)。"
            "不要输出额外文字。\n"
            f"币种: {symbol} 模式: {mode}\n"
            f"数据完整度: {json.dumps(completeness, ensure_ascii=False)}\n"
            f"指标快照: {json.dumps(snapshot, ensure_ascii=False)}\n"
        )
        last_error = ""
        used_model = self.model
        parsed: Dict[str, Any] = {}
        for candidate_model in [self.model] + self.fallback_models:
            try:
                body = {
                    "model": candidate_model,
                    "messages": [
                        {"role": "system", "content": "/no_think 你是严谨的量化辅助研判助手，只输出最终严格 JSON，不要输出推理、thinking、reasoning。"},
                        {"role": "user", "content": "/no_think " + prompt},
                    ],
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                    "max_tokens": 1200,
                    # Anti-"reasoning-only" flags for Qwen / vLLM compatible gateways:
                    # force the model to emit the final answer instead of swallowing
                    # all tokens inside reasoning_content. Plain OpenAI servers will
                    # simply ignore these extras.
                    "enable_thinking": False,
                    "thinking": False,
                    "chat_template_kwargs": {"enable_thinking": False},
                }
                raw_content, reasoning = self._post_chat(requests, body)
                text = self._strip_thinking(raw_content)
                parsed = self._extract_json_object(text)
                parsed = repair_mojibake_value(parsed)
                if not parsed:
                    # Retry once with streaming disabled. Some Qwen / reasoning-only
                    # gateways emit reasoning_content via SSE and only deliver the
                    # final ``content`` JSON in the non-stream response body.
                    prev_stream = self.stream
                    try:
                        self.stream = False
                        raw_content2, reasoning2 = self._post_chat(requests, body)
                    finally:
                        self.stream = prev_stream
                    text2 = self._strip_thinking(raw_content2)
                    parsed = self._extract_json_object(text2)
                    parsed = repair_mojibake_value(parsed)
                    if not parsed:
                        if reasoning or reasoning2:
                            raise ValueError("LLM returned reasoning without final JSON content")
                        raise ValueError("LLM returned no parseable JSON content")
                used_model = candidate_model
                if candidate_model != self.model:
                    logger.warning("OpenAI 辅助预测主模型 %s 失败后，已切换 fallback 模型 %s", self.model, candidate_model)
                break
            except Exception as exc:
                last_error = f"{candidate_model}: {exc}"
                parsed = {}
                continue
        if not parsed:
            payload = self._neutral(symbol, mode, error=last_error or "LLM returned no parseable JSON content")
            payload["data_sources_generated_at"] = data_sources_generated_at or {}
            payload["model"] = self.model
            payload["fallback_models"] = self.fallback_models
            self._persist(payload)
            return payload

        try:
            score = float(parsed.get("score", 0.0))
            score = max(-1.0, min(1.0, score))
            confidence = float(parsed.get("confidence", 0.0))
            confidence = max(0.0, min(1.0, confidence))
            direction = str(parsed.get("direction") or "flat")
            if direction not in {"up", "down", "flat"}:
                direction = "flat"
            prediction_value = float(parsed.get("prediction_value") or 0.0)
            summary = str(repair_mojibake_value(parsed.get("summary") or ""))[:600]
            anchors = repair_mojibake_value(parsed.get("anchors") or [])
            if not isinstance(anchors, list):
                anchors = [str(anchors)]
            risk_flags = repair_mojibake_value(parsed.get("risk_flags") or [])
            if not isinstance(risk_flags, list):
                risk_flags = [str(risk_flags)]
            anchors = [str(repair_mojibake_value(x)) for x in anchors]
            risk_flags = [str(repair_mojibake_value(x)) for x in risk_flags]
        except Exception as exc:
            payload = self._neutral(symbol, mode, error=f"parse error: {exc}")
            payload["data_sources_generated_at"] = data_sources_generated_at or {}
            self._persist(payload)
            return payload

        payload = {
            "generated_at": _now_iso(),
            "symbol": symbol,
            "mode": mode,
            "direction": direction,
            "score": score,
            "confidence": confidence,
            "prediction_value": prediction_value,
            "summary": summary,
            "anchors": anchors,
            "risk_flags": risk_flags,
            "data_sources_generated_at": data_sources_generated_at or {},
            "status": "ok",
            "error": None,
            "model": used_model,
            "primary_model": self.model,
        }
        self._cache[cache_key] = {"_cached_at": time.time(), "payload": payload}
        self._persist(payload)
        return payload


__all__ = [
    "MARKET_FEATURE_COLUMNS",
    "NEWS_FEATURE_COLUMNS",
    "summarize_liquidation_map",
    "build_market_feature_snapshot",
    "append_market_columns",
    "assess_context_completeness",
    "adaptive_context_weights",
    "compute_market_bias",
    "fuse_direction_signals",
    "OpenAIFormatSignalClient",
    "repair_mojibake_value",
    "_extract_last_and_change",
]
