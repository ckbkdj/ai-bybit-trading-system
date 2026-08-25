"""活跃 API Server。

由 ``run_v3.sh`` 拉起。提供：

- ``/symbols`` 系列：目标币种维护
- ``/liqmap/{symbol}``：返回原始 Coinglass 爆仓图 JSON
- ``/liqmap/points/{symbol}``：基于爆仓图计算多 / 空点位
- ``/predict/{symbol}``：返回 ``model_results/`` 下该币种当前所有模式的预测
  结果，含本地预测 + OpenAI 辅助预测 + 训练元数据 + 数据源时间
- ``/results/{symbol_}``：聚合 BTC/XRP 等硬编码币种的所有模式预测结果
- ``/news-context/{symbol}``：返回 Coinglass 新闻 / 财经日历 / 鲸鱼 / 恐惧贪婪
  的合成上下文
- ``/status``：健康检查

所有用户可见文本 / 错误信息使用中文。
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Iterable

import ccxt
import requests
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

try:
    from point_finder import find_three_points
    from support_strategy import find_support_points
except ModuleNotFoundError:  # Allow importing api.api_server from project-root tests/tools.
    from api.point_finder import find_three_points
    from api.support_strategy import find_support_points
from core.market_context import (
    OpenAIFormatSignalClient,
    assess_context_completeness,
    build_market_feature_snapshot,
    repair_mojibake_value,
)
from api.control_plane_api import create_control_plane_router, validate_control_plane_bind

app = FastAPI()

_configured_origins = [
    item.strip()
    for item in os.environ.get(
        "AI_BOT_CORS_ORIGINS", "http://127.0.0.1,http://localhost"
    ).split(",")
    if item.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_configured_origins,
    allow_credentials="*" not in _configured_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_ROOT / "data"
METRICS_DIR = DATA_PATH / "coinglass_metrics"
SYMBOL_FILE = DATA_PATH / "symbols.json"
CONFIG_FILE = PROJECT_ROOT / "config.yml"
RESULTS_DIR = PROJECT_ROOT / "model_results"
LLM_AUX_DIR = RESULTS_DIR / "llm_aux"
EVALUATION_DIR = RESULTS_DIR / "evaluation"
AI_OVERVIEW_DIR = RESULTS_DIR / "ai_overview"

DATA_PATH.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LLM_AUX_DIR.mkdir(parents=True, exist_ok=True)
AI_OVERVIEW_DIR.mkdir(parents=True, exist_ok=True)
# 评估闭环输出目录：/evaluation/summary、/evaluation/{symbol} 仅读取此处，
# 不在请求路径上触发任何 evaluation 生成，避免阻塞 /predict。
EVALUATION_DIR.mkdir(parents=True, exist_ok=True)

app.include_router(create_control_plane_router(PROJECT_ROOT))

# 后端 AI 缓存刷新周期：AI总览/AI流式分析每 60 秒，AI摘要/per-mode 每 180 秒。
AI_OVERVIEW_REFRESH_SECONDS = 60
AI_AUX_REFRESH_SECONDS = 180

if not SYMBOL_FILE.exists():
    SYMBOL_FILE.write_text(
        json.dumps({"symbols": ["BTC", "ETH", "XRP", "SOL"]}, ensure_ascii=False)
    )

bybit = ccxt.bybit({"timeout": 5000})
binance = ccxt.binance({"timeout": 5000})
last_price_map: Dict[str, float] = {}

FILE_MODES = ["scalping", "mid_short", "trend", "trend_swing", "swing"]
MODE_TIMEFRAMES = {
    "scalping": "3m",
    "mid_short": "15m",
    "trend": "2h",
    "trend_swing": "4h",
    "swing": "1d",
}
TF_MAX_AGE_SECONDS = {
    "1m": 5 * 60,
    "3m": 10 * 60,
    "5m": 15 * 60,
    "15m": 45 * 60,
    "30m": 90 * 60,
    "1h": 2 * 3600,
    "2h": 4 * 3600,
    "4h": 8 * 3600,
    "1d": 36 * 3600,
}


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _safe_write_json(path: Path, payload: Dict[str, Any]) -> bool:
    """Atomically write ``payload`` as JSON to ``path``.

    Writes to a temp file in the same directory then ``os.replace`` so
    readers never see a half-written file. Returns True on success.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str)
        os.replace(tmp, path)
        return True
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def _symbol_list_full() -> List[str]:
    """Return tracked symbols (USDT pairs) for backend AI cache refresh.

    Reads ``data/symbols.json``; falls back to BTC/ETH/XRP/SOL when the
    file is missing or malformed.
    """
    default = ["BTC", "ETH", "XRP", "SOL"]
    try:
        data = _safe_load_json(SYMBOL_FILE) or {}
        syms = data.get("symbols") or default
        if not isinstance(syms, list) or not syms:
            syms = default
    except Exception:
        syms = default
    full: List[str] = []
    seen = set()
    for s in syms:
        if not isinstance(s, str) or not s.strip():
            continue
        u = s.strip().upper()
        if not u.endswith("USDT"):
            u = u + "USDT"
        if u in seen:
            continue
        seen.add(u)
        full.append(u)
    if not full:
        full = [b + "USDT" for b in default]
    return full


def _safe_load_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {"data": data}
    except Exception:
        return {}


def _safe_load_config() -> Dict[str, Any]:
    try:
        if not CONFIG_FILE.exists():
            return {}
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _compact_json(payload: Any, limit: int = 16000) -> str:
    text = json.dumps(payload, ensure_ascii=False, default=str)
    if len(text) <= limit:
        return text
    return text[:limit] + "...<已截断>"


def _llm_endpoint(base_url: str) -> str:
    base = (base_url or "").rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


def _llm_models_from_cfg(cfg: Dict[str, Any]) -> List[str]:
    """Return configured primary model plus configured fallbacks in order."""
    primary = str(cfg.get("model") or "gpt-4o-mini").strip()
    raw_fallbacks = cfg.get("fallback_models") or cfg.get("model_fallbacks") or []
    if isinstance(raw_fallbacks, str):
        raw_fallbacks = [raw_fallbacks]
    models: List[str] = []
    for model in [primary] + list(raw_fallbacks or []):
        name = str(model or "").strip()
        if name and name not in models:
            models.append(name)
    return models


def _strip_ai_thinking(text: str) -> str:
    """Remove visible reasoning/thinking wrappers from model output."""
    if not text:
        return ""
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"(?is)^\s*(reasoning|thinking|思考过程|推理过程)\s*[:：].*?(最终答案|最终分析|结论)\s*[:：]", "", cleaned)
    cleaned = re.sub(r"(?im)^\s*(reasoning|thinking|思考过程|推理过程)\s*[:：].*$", "", cleaned)
    return cleaned.strip()


def _repair_mojibake(text: str) -> str:
    repaired = repair_mojibake_value(text)
    return repaired if isinstance(repaired, str) else text


def _sse(event: str, payload: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


def _chat_parts_from_line(line: str) -> tuple[str, str]:
    """Extract ``(content, reasoning)`` from one OpenAI/Qwen-compatible response line.

    Robust to several upstream shapes:
      * ``data: {...}`` SSE lines with ``choices[0].delta.content``
      * ``choices[0].delta.reasoning_content`` / ``reasoning`` (returned as
        ``reasoning`` so callers can flag reasoning-only chunks).
      * Final non-stream JSON objects with ``choices[0].message.content``
        (and ``message.reasoning_content`` / ``reasoning``).
      * Plain JSON object lines without the ``data:`` prefix.
      * Some gateways emit ``output_text`` or top-level ``content``.

    Returns ``("", "")`` for empty / ``[DONE]`` / unparseable lines.
    Mojibake repair is left to the caller.
    """
    raw = (line or "").strip()
    if not raw:
        return "", ""
    if raw in ("data: [DONE]", "[DONE]"):
        return "", ""
    if raw.startswith("data:"):
        raw = raw[5:].strip()
        if not raw or raw == "[DONE]":
            return "", ""
    try:
        data = json.loads(raw)
    except Exception:
        return "", ""
    if not isinstance(data, dict):
        return "", ""

    content_str = ""
    reasoning_str = ""

    choices = data.get("choices") or []
    if choices and isinstance(choices[0], dict):
        first = choices[0]
        delta = first.get("delta") if isinstance(first.get("delta"), dict) else {}
        message = first.get("message") if isinstance(first.get("message"), dict) else {}

        # Streaming delta content.
        c = delta.get("content")
        if isinstance(c, str):
            content_str = c
        elif isinstance(c, list):
            parts: List[str] = []
            for item in c:
                if isinstance(item, dict):
                    t = item.get("text") or item.get("content")
                    if isinstance(t, str):
                        parts.append(t)
                elif isinstance(item, str):
                    parts.append(item)
            content_str = "".join(parts)

        # Final non-stream message content (Qwen-compatible last frame).
        if not content_str and isinstance(message.get("content"), str):
            content_str = message["content"]

        # Reasoning fields (delta or final message); both are aggregated.
        for key in ("reasoning_content", "reasoning"):
            r = delta.get(key)
            if isinstance(r, str) and r:
                reasoning_str += r
            r2 = message.get(key)
            if isinstance(r2, str) and r2:
                reasoning_str += r2

    # Some non-OpenAI gateways stream a top-level field.
    if not content_str:
        for key in ("output_text", "content"):
            v = data.get(key)
            if isinstance(v, str) and v:
                content_str = v
                break

    return content_str, reasoning_str


def _chat_delta_from_sse_line(line: str) -> str:
    """Backwards-compatible content-only extractor (kept for older callers/tests)."""
    content, _ = _chat_parts_from_line(line)
    return content


def _trend_label(value: Any) -> str:
    text = str(value or "").lower()
    if "up" in text or "long" in text or "bull" in text or "看多" in text:
        return "看多"
    if "down" in text or "short" in text or "bear" in text or "看空" in text:
        return "看空"
    return "中性"


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _fmt_num(value: Any, digits: int = 4) -> str:
    num = _num(value)
    return "-" if num is None else f"{num:.{digits}f}"


def _fmt_pct(value: Any) -> str:
    num = _num(value)
    return "-" if num is None else f"{num * 100:.3f}%"


# Threshold (~5bps) at/under which we treat (pred-last)/last as flat for display.
_DISPLAY_TREND_THRESHOLD = 0.0005


def _normalize_prediction_display_fields(pred: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize displayed prediction fields so dashboard UI cannot show a
    direction/return/signal that contradicts ``pred`` vs ``last``.

    When ``pred['pred']`` and ``pred['last']`` are both valid numbers and last
    is non-zero, recompute the relative return and use it as the single source
    of truth for direction-style display fields (return, price trend, trade
    badge, direction). Any pre-existing potentially-stale values are preserved
    under ``source_*`` keys for debugging. Brain/calibrated metadata fields
    (``brain_*``, ``calibrated_*``, etc.) are never deleted; they remain in the
    payload but are no longer the preferred source for the dashboard badge.
    """
    if not isinstance(pred, dict):
        return pred
    pred_value = _num(pred.get("pred"))
    last_value = _num(pred.get("last"))
    if pred_value is None or last_value is None or last_value == 0:
        return pred
    ret = (pred_value - last_value) / last_value
    if ret > _DISPLAY_TREND_THRESHOLD:
        trend = "up"
        direction = "long"
    elif ret < -_DISPLAY_TREND_THRESHOLD:
        trend = "down"
        direction = "short"
    else:
        trend = "flat"
        direction = "flat"

    # Preserve previous (possibly stale) values for traceability.
    for src_key, dst_key in (
        ("price_predicted_return", "source_price_predicted_return"),
        ("price_trend", "source_price_trend"),
        ("trade_trend_display", "source_trade_trend_display"),
    ):
        if src_key in pred:
            old = pred.get(src_key)
            new = ret if src_key == "price_predicted_return" else trend
            if old != new and dst_key not in pred:
                pred[dst_key] = old

    # Signed price move: pred vs current price. This stays negative when price is expected down.
    pred["display_price_return"] = ret
    pred["display_predicted_return"] = ret
    pred["price_predicted_return"] = ret
    pred["display_price_trend"] = trend
    pred["price_trend"] = trend
    pred["display_trade_signal"] = trend
    pred["trade_trend_display"] = trend
    pred["display_direction"] = direction

    # Trade-side return for display/target: calculate only from pred-vs-last price move.
    # 涨：收益 = (pred-last)/last；跌：收益 = (last-pred)/last。
    # 不再使用 Brain 的 flat/long/short 覆盖收益，避免出现明明有涨跌却显示 0 或负数。
    brain = pred.get("brain_prediction") if isinstance(pred.get("brain_prediction"), dict) else {}
    trade_direction = direction
    if direction == "short":
        trade_ret = -ret
    elif direction == "long":
        trade_ret = ret
    else:
        trade_ret = 0.0
    pred["display_trade_return"] = trade_ret
    pred["trade_predicted_return"] = trade_ret
    pred["trade_return_direction"] = trade_direction
    pred["trade_direction"] = trade_direction

    # 达标口径：交易收益 × 当前币种倍率 >= 31%。倍率来自 brain/config：
    # BTC/ETH=100x，XRP/SOL/1000PEPE=75x，默认75x。
    lev = _num(pred.get("target_leverage"))
    if lev is None:
        lev = _num(brain.get("leverage"))
    if lev is None or lev <= 0:
        lev = 75.0
    target = _num(pred.get("target_leveraged_profit"))
    if target is None:
        target = _num(brain.get("target_leveraged_profit"))
    if target is None or target <= 0:
        target = 0.31
    leveraged_trade_ret = trade_ret * lev
    pred["target_leverage"] = lev
    pred["target_leveraged_profit"] = target
    pred["display_trade_leveraged_return"] = leveraged_trade_ret
    pred["trade_leveraged_return"] = leveraged_trade_ret
    pred["trade_target_met"] = bool(leveraged_trade_ret >= target)
    return pred


def _llm_aux_usable(payload: Dict[str, Any]) -> bool:
    summary = str(payload.get("summary") or "")
    if payload.get("status") != "ok" or not summary:
        return False
    bad_markers = ("未启用或调用失败", "未启用，请", "not configured")
    return not any(marker in summary for marker in bad_markers)

def _ai_payload_failed(payload: Any) -> bool:
    """Return True when a per-mode aux or overview payload should be
    considered a failure that justifies an immediate retry.

    Treats missing dict, ``cache_missing``, ``error`` status, or any
    per-mode aux that is not :func:`_llm_aux_usable`-usable as failed.
    Overview ``ok``/``fallback``/``disabled`` count as non-failed so we
    don't keep retrying disabled gateways every cycle.
    """
    if not isinstance(payload, dict) or not payload:
        return True
    status = str(payload.get("status") or "").lower()
    if status in ("cache_missing", "error"):
        return True
    # Per-mode aux: when status=ok but summary clearly says not enabled / failed,
    # _llm_aux_usable returns False; we still mark as failed.
    if status == "ok" and not _llm_aux_usable(payload):
        return True
    return False


def _retry_call(fn, attempts: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    """Invoke ``fn()`` up to ``attempts`` times, returning the first payload
    that is not :func:`_ai_payload_failed`.

    The returned payload is annotated with ``attempts`` (number of tries
    executed). When every attempt failed we additionally tag
    ``retry_exhausted=True`` and ``last_failed_at`` so the scheduler can
    skip re-running indefinitely.
    """
    attempts = max(1, int(attempts or 1))
    last: Dict[str, Any] = {}
    for i in range(1, attempts + 1):
        try:
            payload = fn()
        except Exception as exc:
            payload = {"status": "error", "error": str(exc)}
        if not isinstance(payload, dict):
            payload = {"status": "error", "error": "non-dict payload"}
        last = payload
        if not _ai_payload_failed(payload):
            try:
                payload["attempts"] = i
            except Exception:
                pass
            return payload
        if i < attempts:
            try:
                time.sleep(max(0.0, float(delay)))
            except Exception:
                pass
    try:
        last["attempts"] = attempts
        last["retry_exhausted"] = True
        last["last_failed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass
    return last


# Per-mode AI summary freshness window. Generated payloads older than this many
# seconds are considered stale and trigger regeneration in /predict and the
# /ai-analysis/.../stream per-mode loop. Manual click flows go through the same
# precondition because they reuse `_read_prediction_bundle`.
LLM_AUX_FRESH_TTL_SECONDS = 180  # 3 minutes


def _parse_generated_at(value: Any) -> float | None:
    """Parse an ISO-8601 timestamp (with or without timezone, optional ``Z``)
    into a POSIX timestamp; return ``None`` when unparseable so callers can
    treat it as "no fresh marker"."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        try:
            return float(text)
        except Exception:
            return None
    try:
        if dt.tzinfo is None:
            return dt.timestamp()
        return dt.timestamp()
    except Exception:
        return None


def _parse_kline_ts_utc(value: Any) -> float | None:
    """Parse Binance/pandas K-line timestamps for freshness checks.

    K-line timestamps persisted by ``core.data_fetch`` come from pandas
    UTC-naive values. For example ``2026-05-15T04:54:00`` means 04:54 UTC,
    not local CST. Treat timezone-less values as UTC here only; keep
    ``_parse_generated_at`` unchanged for local ``generated_at`` fields.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        try:
            return float(text)
        except Exception:
            return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _is_aux_stale(payload: Dict[str, Any], ttl_seconds: int = LLM_AUX_FRESH_TTL_SECONDS) -> bool:
    """Return True when ``payload['generated_at']`` is missing or older than
    ``ttl_seconds`` from now. Used to decide whether to regenerate a per-mode
    LLM aux summary instead of trusting cached local JSON."""
    if not isinstance(payload, dict) or not payload:
        return True
    ts = _parse_generated_at(payload.get("generated_at"))
    if ts is None:
        return True
    try:
        age = time.time() - ts
    except Exception:
        return True
    return age > max(0, int(ttl_seconds))


def _prediction_source_warning(pred: Dict[str, Any], mode: str) -> str | None:
    """Return a user/API-visible warning when a prediction is based on missing
    or stale market data. Never call such data normal."""
    if not isinstance(pred, dict):
        return "预测数据无效，无法确认行情来源"
    current_warning = pred.get("current_price_warning")
    current_source = str(pred.get("current_price_source") or "")
    if current_warning:
        return str(current_warning)
    if current_source and current_source not in {"coinglass_liqmap", "ohlcv_binance"}:
        return f"当前价格来源异常: {current_source}"
    status = pred.get("data_source_status")
    latest = pred.get("latest_kline_ts")
    tf = str(pred.get("timeframe") or MODE_TIMEFRAMES.get(mode) or "")
    if status and status != "ok":
        return f"行情数据源状态异常: {status}"
    if not latest:
        return "缺少 latest_kline_ts，无法确认预测使用的行情是否为实时数据"
    ts = _parse_kline_ts_utc(latest)
    if ts is None:
        return f"latest_kline_ts 无法解析: {latest}"
    max_age = TF_MAX_AGE_SECONDS.get(tf, 8 * 3600)
    age = time.time() - ts
    if age > max_age:
        return f"行情K线已过期: latest_kline_ts={latest}, timeframe={tf}, age_sec={int(age)}, max_age_sec={max_age}"
    return None


def _normalize_llm_aux_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Repair known proxy mojibake in cached LLM aux payloads before API/UI use."""
    repaired = repair_mojibake_value(payload)
    return repaired if isinstance(repaired, dict) else payload


def _build_llm_aux_on_demand(symbol: str, mode: str) -> Dict[str, Any]:
    """Generate a missing/stale per-mode LLM aux payload via configured API."""
    cfg = (_safe_load_config().get("llm_aux") or {}).copy()
    if not cfg.get("enabled") or not cfg.get("base_url") or not cfg.get("api_key"):
        return {
            "status": "disabled",
            "summary": "OpenAI 辅助预测未启用：请检查 config.yml 的 llm_aux.enabled/base_url/api_key/model。",
            "error": "llm_aux not configured",
        }
    cfg["persist_dir"] = str(LLM_AUX_DIR)
    base = _base_of(symbol)
    try:
        snapshot = build_market_feature_snapshot(metrics_dir=METRICS_DIR, base=base)
    except Exception:
        snapshot = {}
    try:
        completeness = assess_context_completeness(METRICS_DIR, base, data_dir=DATA_PATH)
    except Exception:
        completeness = {"score": 0.0, "sources": {}, "missing": [], "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
    client = OpenAIFormatSignalClient(cfg)
    return client.predict(
        symbol=symbol,
        mode=mode,
        snapshot=snapshot,
        completeness=completeness,
        data_sources_generated_at={},
    )


def _fallback_ai_summary(sym_full: str, predict_payload: Dict[str, Any], context_payload: Dict[str, Any]) -> str:
    """Create a deterministic Chinese final summary when the LLM exposes only reasoning."""
    best_mode = "-"
    best_local: Dict[str, Any] = {}
    best_rank = -10**9
    for mode, bundle in (predict_payload.get("modes") or {}).items():
        local = bundle.get("local_prediction") or {}
        if local.get("message"):
            continue
        confidence = _num(local.get("direction_confidence") or local.get("confidence")) or 0.0
        score = _num(local.get("ensemble_score") or local.get("score")) or 0.0
        # Pick an overview representative by model quality, not by raw return magnitude.
        rmse = abs(_num(local.get("rmse")) or 0.0)
        last = abs(_num(local.get("last")) or 0.0)
        rmse_pct = (rmse / last) if last else 999.0
        rmse_quality = 1.0 / (1.0 + min(999.0, rmse_pct * 10000.0))
        rank = rmse_quality * 10.0 + max(0.0, score) * 2.0 + confidence
        if rank > best_rank:
            best_rank = rank
            best_mode = mode
            best_local = local

    if not best_local:
        return f"一句话结论：{sym_full} 暂无完整本地预测结果，AI 解读已降级为本地规则摘要。\n方向信号：中性，等待下一轮预测调度生成有效模式。\n关键依据：当前可读预测文件不足，不能给出可靠方向。\n风险提示：请勿基于缺失数据开仓。"

    trend = _trend_label(best_local.get("calibrated_trend") or best_local.get("trend") or best_local.get("raw_trend"))
    ret = best_local.get("trade_predicted_return") if best_local.get("trade_predicted_return") is not None else best_local.get("display_trade_return")
    if ret is None:
        pred_num = _num(best_local.get("pred"))
        last_num = _num(best_local.get("last"))
        if pred_num is not None and last_num not in (None, 0):
            ret = (pred_num - last_num) / last_num
    conf = best_local.get("direction_confidence") if best_local.get("direction_confidence") is not None else best_local.get("confidence")
    if conf is None:
        score_num = abs(_num(best_local.get("ensemble_score") or best_local.get("score")) or 0.0)
        ret_num = abs(_num(ret) or 0.0)
        conf = min(0.95, max(0.05, score_num / (score_num + 1.0) if score_num else ret_num * 20))
    source_bits = []
    for label, key in (("新闻", "news_context"), ("鲸鱼", "whale_alert"), ("恐惧贪婪", "fear_greed_index"), ("宏观日历", "financial_calendar")):
        status = (context_payload.get(key) or {}).get("status") or "ok"
        source_bits.append(f"{label}:{status}")
    return (
        f"一句话结论：{sym_full} 当前最优模式为 {best_mode}，综合信号偏{trend}，需结合爆仓密集区确认入场。\n"
        f"方向信号：{trend}；方向置信度约 {_fmt_pct(conf)}，预测收益约 {_fmt_pct(ret)}。\n"
        f"关键依据：预测价 {_fmt_num(best_local.get('pred'))}，当前价 {_fmt_num(best_local.get('last'))}，RMSE {_fmt_num(best_local.get('rmse'), 6)}，综合分 {_fmt_num(best_local.get('ensemble_score') or best_local.get('score'), 3)}。\n"
        f"数据源状态：{'; '.join(source_bits)}。\n"
        "风险提示：AI 网关未返回标准最终正文，本段为本地规则降级摘要；仅作量化参考，不构成投资建议。"
    )


def _base_of(symbol: str) -> str:
    return symbol.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")


# ---------------------------------------------------------------------------
# 币种维护
# ---------------------------------------------------------------------------

@app.get("/symbols")
def get_symbols():
    return {"symbols": _safe_load_json(SYMBOL_FILE).get("symbols", [])}


@app.post("/symbols/{symbol}")
def add_symbol(symbol: str):
    syms = _safe_load_json(SYMBOL_FILE).get("symbols", [])
    sym = symbol.upper()
    if sym not in syms:
        syms.append(sym)
        SYMBOL_FILE.write_text(json.dumps({"symbols": syms}, ensure_ascii=False))
    return {"symbols": syms}


@app.delete("/symbols/{symbol}")
def delete_symbol(symbol: str):
    syms = _safe_load_json(SYMBOL_FILE).get("symbols", [])
    sym = symbol.upper()
    if sym in syms:
        syms.remove(sym)
        SYMBOL_FILE.write_text(json.dumps({"symbols": syms}, ensure_ascii=False))
    return {"symbols": syms}


# ---------------------------------------------------------------------------
# 爆仓图与点位
# ---------------------------------------------------------------------------

@app.get("/liqmap/{symbol}")
def get_liqmap(symbol: str):
    """返回 ``data/{BASE}.json`` 原始爆仓图 + 实时价格覆盖。"""
    base = _base_of(symbol)
    file_path = DATA_PATH / f"{base}.json"
    if not file_path.exists():
        return {"error": "未找到爆仓图数据"}

    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, str):
            data = json.loads(data)

    try:
        if base == "TURBO":
            ticker = binance.fetch_ticker(f"{base}/USDT:USDT")
        else:
            ticker = bybit.fetch_ticker(f"{base}/USDT:USDT")
        last_price_map[base] = float(ticker["last"])
        data["lastPrice"] = str(last_price_map[base])
    except Exception as exc:
        print(f"实时价格获取失败，使用本地价格: {exc}")
        try:
            last_price_map[base] = float(data.get("lastPrice") or 0.0)
        except Exception:
            pass
    return data


@app.get("/liqmap/points/{symbol}")
def get_points(
    symbol: str,
    model: str = Query("auto"),
    threshold: float = Query(0.38),
    priority: str = Query("near"),
    strategy: str = Query("classic"),
):
    base = _base_of(symbol)
    file_path = DATA_PATH / f"{base}.json"
    if not file_path.exists():
        return {"points": {"long": [], "short": []}, "model": "未知模型", "model_key": "unknown"}

    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, str):
            data = json.loads(data)

    if strategy == "support":
        support_result = find_support_points(data, threshold)
        long_points = support_result.get("long", [])
        short_points = support_result.get("short", [])
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {base} -- support_result -- {support_result}")
        return {
            "model": "支撑点优先",
            "points": {"long": long_points, "short": short_points},
        }

    # 经典点位：使用最新价格覆盖
    if base in last_price_map:
        data["lastPrice"] = str(last_price_map[base])
    else:
        try:
            last_price_map[base] = float(data.get("lastPrice") or 0.0)
        except Exception:
            pass
    points = find_three_points(data=data, model=model, threshold=threshold, priority=priority)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] -- {base} -- 点位: {points.get('model_key')}")
    return points


# ---------------------------------------------------------------------------
# 预测结果（含 OpenAI 辅助、训练元数据、数据源时间）
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 评估闭环只读辅助
# ---------------------------------------------------------------------------

def _read_evaluation_summary() -> Dict[str, Any]:
    """Return ``model_results/evaluation/summary.json`` if present.

    Falls back to an empty-but-shaped payload when summary is missing or
    cannot be parsed. Never raises; never triggers regeneration. This is
    intentionally identical in shape to ``OnlinePredictionCalibrator``'s
    placeholder so frontend code can read it unconditionally.
    """
    path = EVALUATION_DIR / "summary.json"
    payload = _safe_load_json(path)
    if isinstance(payload, dict) and ("settled_count" in payload or "groups" in payload):
        return payload
    return {
        "settled_count": 0,
        "pending_count": 0,
        "groups": {},
        "last_updated_at": None,
        "note": "evaluation_summary_unavailable",
        "available": False,
    }


def _read_evaluation_for_symbol_mode(symbol: str, mode: str) -> Dict[str, Any]:
    """Return per ``(symbol, mode)`` evaluation JSON if present, else empty.

    Non-blocking: only file I/O on already-exported JSON. Never triggers
    OnlinePredictionCalibrator.export_evaluation_summary.
    """
    sym = (symbol or "").upper()
    detail_path = EVALUATION_DIR / f"{sym}_{mode}.json"
    detail = _safe_load_json(detail_path)
    if isinstance(detail, dict) and detail:
        return detail
    # Fallback to summary.groups[key] if a per-group file is missing.
    summary = _read_evaluation_summary()
    groups = summary.get("groups") or {}
    if isinstance(groups, dict):
        entry = groups.get(f"{sym}_{mode}")
        if isinstance(entry, dict):
            return entry
    return {}


def _evaluation_files_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """List existing per-mode evaluation files matching ``symbol``."""
    sym = (symbol or "").upper()
    items: List[Dict[str, Any]] = []
    try:
        for child in sorted(EVALUATION_DIR.glob(f"{sym}_*.json")):
            if child.name == "summary.json":
                continue
            payload = _safe_load_json(child)
            if not isinstance(payload, dict) or not payload:
                continue
            items.append({"file": child.name, "payload": payload})
    except Exception:
        pass
    return items


def _read_prediction_bundle(symbol: str, mode: str, refresh_stale_aux: bool = False) -> Dict[str, Any]:
    """聚合本地预测 + OpenAI 辅助 + 训练元数据。

    ``refresh_stale_aux`` is intentionally false for plain /predict so normal
    chart/data refreshes don't trigger expensive AI regeneration. The streaming
    AI endpoint sets it/equivalent checks when the user explicitly requests AI.
    """
    pred_path = RESULTS_DIR / f"{symbol}_{mode}.json"
    train_meta_path = RESULTS_DIR / f"{symbol}_{mode}_training.json"
    llm_aux_path = LLM_AUX_DIR / f"{symbol}_{mode}.json"

    out: Dict[str, Any] = {"mode": mode, "symbol": symbol}
    pred = _safe_load_json(pred_path)
    if pred:
        _normalize_prediction_display_fields(pred)
        warning = _prediction_source_warning(pred, mode)
        pred["data_source_warning"] = warning
        pred["data_source_reliable"] = warning is None and pred.get("data_source_status") == "ok"
        out["local_prediction"] = pred
        out["data_source_status"] = pred.get("data_source_status")
        out["data_source_warning"] = warning
        out["data_source_reliable"] = pred["data_source_reliable"]
    else:
        out["local_prediction"] = {"message": "暂无本地预测结果"}
        out["data_source_status"] = "missing"
        out["data_source_warning"] = "暂无本地预测结果，无法确认行情来源"
        out["data_source_reliable"] = False

    train_meta = _safe_load_json(train_meta_path)
    if train_meta:
        out["training_metadata"] = train_meta
    else:
        out["training_metadata"] = {"message": "暂无训练元数据"}

    llm = _safe_load_json(llm_aux_path)
    if llm:
        llm = _normalize_llm_aux_payload(llm)
    # Plain /predict is used by the 120s chart/data refresh; do not regenerate
    # merely because an ok aux is older than 3 minutes. Explicit AI generation
    # paths pass refresh_stale_aux=True / run the same stale check themselves.
    # If there is no usable cached aux during plain /predict, return an explicit
    # cached-missing status instead of doing a slow LLM request on the read path.
    if refresh_stale_aux and ((not _llm_aux_usable(llm)) or _is_aux_stale(llm)):
        llm = _build_llm_aux_on_demand(symbol, mode)
    if llm:
        out["openai_prediction"] = llm
    else:
        out["openai_prediction"] = {
            "status": "cache_missing",
            "summary": "暂无缓存AI辅助预测；普通预测接口不触发慢速AI生成，请使用AI流式分析刷新。",
        }

    out["data_sources_generated_at"] = pred.get("data_sources_generated_at") if pred else {}
    # 注入只读评估摘要；缺失时给空字典，不会触发任何重算。
    try:
        eval_info = _read_evaluation_for_symbol_mode(symbol, mode)
    except Exception:
        eval_info = {}
    out["evaluation"] = eval_info if isinstance(eval_info, dict) else {}
    return out


@app.get("/predict/{symbol}")
async def get_prediction(symbol: str):
    """返回单币种所有模式的本地 + OpenAI + 训练元数据。"""
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    by_mode: Dict[str, Any] = {}
    for mode in FILE_MODES:
        by_mode[mode] = _read_prediction_bundle(sym_full, mode)
    if not any((by_mode[m].get("local_prediction") or {}).get("trend") for m in by_mode):
        return {"symbol": sym_full, "modes": by_mode, "message": "暂无完整预测结果，请等待预测调度生成"}
    return {"symbol": sym_full, "modes": by_mode}


@app.get(
    "/results/{symbol_}",
    summary="获取硬编码币种（XRPUSDT, BTCUSDT）所有模型预测结果（结构化）",
)
async def get_all_model_results_structured(symbol_: str):
    """与旧前端兼容：聚合 ``symbol_`` 与 ``BTCUSDT`` 的所有模式结果。

    返回结构在原有基础上新增 ``training_metadata`` / ``openai_prediction`` /
    ``data_sources_generated_at`` 字段。
    """
    all_results: Dict[str, Any] = {}
    target_symbols = [symbol_.upper(), "BTCUSDT"]

    for symbol in target_symbols:
        symbol_modes_data: Dict[str, Any] = {}
        any_mode_data_found = False

        for mode in FILE_MODES:
            file_path = RESULTS_DIR / f"{symbol}_{mode}.json"
            if not file_path.exists():
                symbol_modes_data[mode] = {"message": "暂无该模式预测结果"}
                continue
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                # 数值字段稳健化
                for k in ("pred", "last", "score", "rmse", "predicted_return", "raw_predicted_return"):
                    v = data.get(k)
                    try:
                        data[k] = float(v) if v is not None else None
                    except Exception:
                        data[k] = None
                ci = data.get("ci")
                if isinstance(ci, list) and len(ci) == 2:
                    try:
                        data["ci"] = [float(ci[0]), float(ci[1])]
                    except Exception:
                        data["ci"] = [None, None]
                else:
                    data["ci"] = [None, None]
                # 合并训练元数据 / OpenAI 辅助
                meta = _safe_load_json(RESULTS_DIR / f"{symbol}_{mode}_training.json")
                if meta:
                    data["training_metadata"] = meta
                llm = _safe_load_json(LLM_AUX_DIR / f"{symbol}_{mode}.json")
                if llm:
                    data["openai_prediction"] = _normalize_llm_aux_payload(llm)
                symbol_modes_data[mode] = data
                any_mode_data_found = True
            except json.JSONDecodeError:
                symbol_modes_data[mode] = {"error": "Invalid JSON content", "message": "文件内容无效或损坏"}
            except Exception as exc:
                symbol_modes_data[mode] = {"error": str(exc), "message": f"读取文件时发生错误: {exc}"}
        if any_mode_data_found:
            all_results[symbol] = symbol_modes_data
        else:
            all_results[symbol] = {"message": f"未找到 {symbol} 的任何有效模式结果文件。"}

    if not all_results.get(symbol_.upper()) and not all_results.get("BTCUSDT"):
        raise HTTPException(
            status_code=404,
            detail="未找到任何 XRPUSDT 或 BTCUSDT 的模型结果文件，请确认 model_results 目录已生成数据。",
        )
    return all_results


# ---------------------------------------------------------------------------
# 新闻 / 上下文
# ---------------------------------------------------------------------------

@app.get("/news-context/{symbol}")
def get_news_context(symbol: str):
    """返回 Coinglass 新闻 / 财经日历 / 鲸鱼 / 恐惧贪婪综合上下文。"""
    base = _base_of(symbol)
    nc = _safe_load_json(METRICS_DIR / "news_context.json")
    fc = _safe_load_json(METRICS_DIR / "financial_calendar.json")
    wh = _safe_load_json(METRICS_DIR / "whale_alert.json")
    fg = _safe_load_json(METRICS_DIR / "fear_greed_index.json")
    ev = _safe_load_json(METRICS_DIR / "events.json")
    return {
        "symbol": symbol.upper(),
        "base": base,
        "news_context": nc or {"status": "unavailable"},
        "financial_calendar": fc or {"status": "unavailable"},
        "whale_alert": wh or {"status": "unavailable"},
        "fear_greed_index": fg or {"status": "unavailable"},
        "events": ev or {"status": "unavailable"},
    }


# ---------------------------------------------------------------------------
# 后端 AI 缓存：定期生成 AI 总览 + per-mode 摘要，前端只读
# ---------------------------------------------------------------------------

# Locks per-symbol so the background refresher and any best-effort
# request-triggered refresh never overlap on the same symbol.
_AI_REFRESH_LOCKS: Dict[str, threading.Lock] = {}
_AI_REFRESH_LOCKS_GUARD = threading.Lock()


def _ai_lock_for(symbol: str) -> threading.Lock:
    sym = (symbol or "").upper()
    with _AI_REFRESH_LOCKS_GUARD:
        lk = _AI_REFRESH_LOCKS.get(sym)
        if lk is None:
            lk = threading.Lock()
            _AI_REFRESH_LOCKS[sym] = lk
        return lk


def _build_ai_overview(symbol: str) -> Dict[str, Any]:
    """Generate the AI overview payload for ``symbol`` (non-streaming).

    Mirrors the original get_ai_analysis logic: gathers /predict modes +
    news context + cached per-mode summaries, calls the configured LLM
    with anti-thinking flags, falls back to local rule summary when the
    gateway returns empty content. Returns a dict with status/symbol/
    model/summary/saved_summaries/generated_at/updated_at[/error].
    """
    cfg = _safe_load_config().get("llm_aux") or {}
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    base = _base_of(symbol)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    predict_payload = {"symbol": sym_full, "modes": {m: _read_prediction_bundle(sym_full, m) for m in FILE_MODES}}
    try:
        context_payload = get_news_context(base)
    except Exception:
        context_payload = {}

    saved_summaries: Dict[str, Any] = {}
    for mode in FILE_MODES:
        llm = _safe_load_json(LLM_AUX_DIR / f"{sym_full}_{mode}.json")
        if llm:
            saved_summaries[mode] = _normalize_llm_aux_payload(llm)

    if not cfg.get("enabled") or not cfg.get("base_url") or not cfg.get("api_key"):
        return {
            "status": "disabled",
            "symbol": sym_full,
            "summary": "AI分析未启用：请在 config.yml 的 llm_aux 中配置 enabled/base_url/api_key/model。",
            "saved_summaries": saved_summaries,
            "generated_at": now_iso,
            "updated_at": now_iso,
        }

    prompt_payload = {
        "predict": predict_payload,
        "news_context": context_payload,
        "saved_llm_summaries": saved_summaries,
    }
    system_prompt = (
        "/no_think 你是专业加密货币量化交易分析师。只输出最终中文分析，不输出推理过程、思考过程、reasoning、thinking 或 <think> 标签。"
        "必须基于给定 JSON，不要编造不存在的数据。输出保持紧凑，固定四段：\n"
        "一句话结论：...\n方向信号：...\n关键依据：...\n风险提示：...\n"
        "语气克制专业，明确仅作量化参考，不构成投资建议。"
    )
    user_prompt = (
        "/no_think 请根据下面 JSON 生成最终中文交易解读。禁止解释你如何分析，禁止输出英文推理，禁止输出隐藏思维链。\n"
        f"JSON:\n{_compact_json(prompt_payload)}"
    )
    models = _llm_models_from_cfg(cfg)
    primary_model = models[0] if models else str(cfg.get("model") or "gpt-4o-mini")
    last_error = ""
    for model in models:
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 1200,
            "enable_thinking": False,
            "thinking": False,
            "chat_template_kwargs": {"enable_thinking": False},
        }
        try:
            resp = requests.post(
                _llm_endpoint(str(cfg.get("base_url") or "")),
                headers={"Authorization": f"Bearer {cfg.get('api_key')}", "Content-Type": "application/json"},
                json=body,
                timeout=(int(cfg.get("connect_timeout") or 10), int(cfg.get("timeout") or 30)),
            )
            resp.raise_for_status()
            data = resp.json()
            message = (data.get("choices") or [{}])[0].get("message") or {}
            raw_content = message.get("content") or ""
            content = _strip_ai_thinking(_repair_mojibake(str(raw_content)))
            if not content:
                raise ValueError("LLM returned empty final content")
            return {
                "status": "ok",
                "symbol": sym_full,
                "model": model,
                "primary_model": primary_model,
                "summary": content,
                "saved_summaries": saved_summaries,
                "generated_at": now_iso,
                "updated_at": now_iso,
            }
        except Exception as exc:
            last_error = f"{model}: {exc}"
            continue
    return {
        "status": "error",
        "symbol": sym_full,
        "model": primary_model,
        "fallback_models": [m for m in models if m != primary_model],
        "summary": f"AI分析生成失败：{last_error}",
        "saved_summaries": saved_summaries,
        "error": last_error,
        "generated_at": now_iso,
        "updated_at": now_iso,
    }


def _read_ai_overview(symbol: str) -> Dict[str, Any]:
    """Return the cached overview payload for ``symbol``.

    If the cache file is missing, returns a shaped ``cache_missing`` payload
    so the frontend always receives a stable schema and never gets a 404.
    """
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    path = AI_OVERVIEW_DIR / f"{sym_full}.json"
    payload = _safe_load_json(path)
    if not payload:
        # Also surface any per-mode summaries already on disk so the UI can
        # render partial cards even before the first overview write.
        saved_summaries: Dict[str, Any] = {}
        for mode in FILE_MODES:
            llm = _safe_load_json(LLM_AUX_DIR / f"{sym_full}_{mode}.json")
            if llm:
                saved_summaries[mode] = _normalize_llm_aux_payload(llm)
        return {
            "status": "cache_missing",
            "symbol": sym_full,
            "summary": "AI总览缓存尚未生成，后端将在 1 分钟内补齐；本展示只读缓存，不会触发慢速 LLM 调用。",
            "saved_summaries": saved_summaries,
            "generated_at": None,
            "updated_at": None,
        }
    # Repair any latin1->utf8 mojibake at read time for safety.
    repaired = repair_mojibake_value(payload)
    return repaired if isinstance(repaired, dict) else payload


def _overview_is_stale(payload: Dict[str, Any], ttl_seconds: int = AI_OVERVIEW_REFRESH_SECONDS) -> bool:
    if not isinstance(payload, dict) or not payload:
        return True
    if payload.get("status") in (None, "cache_missing", "error"):
        return True
    ts = _parse_generated_at(payload.get("updated_at") or payload.get("generated_at"))
    if ts is None:
        return True
    return (time.time() - ts) > max(0, int(ttl_seconds))


def _refresh_ai_for_symbol(
    symbol: str,
    *,
    force_aux: bool = False,
    force_overview: bool = False,
    failed_only: bool = False,
    retry_attempts: int = 3,
) -> Dict[str, Any]:
    """Backend AI cache refresh entrypoint for one symbol.

    Sequence:
      1) For each FILE_MODES, regenerate per-mode llm_aux when missing/error
         or older than ``AI_AUX_REFRESH_SECONDS`` (or ``force_aux``).
         Generation uses :func:`_retry_call` so a transient gateway error
         is retried immediately up to ``retry_attempts`` times in the same
         refresh cycle, instead of waiting for the next 3-minute window.
      2) Build the AI overview JSON when the cache file is missing, errored
         or older than ``AI_OVERVIEW_REFRESH_SECONDS`` (or ``force_overview``).

    When ``failed_only=True`` we only retry per-mode aux / overview entries
    whose current cached payload is :func:`_ai_payload_failed` -- OK / fresh
    cached items are left untouched, and exhausted errors are retried only
    once (``retry_attempts=1`` is the typical caller default).

    Scheduled callers (force_aux=False, failed_only=False) never re-run
    items that have already been marked ``retry_exhausted=True`` -- those
    are skipped to avoid hammering the gateway every 1-/3-minute tick.

    Guarded by a per-symbol lock so overlapping refresh attempts are skipped.
    """
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    lk = _ai_lock_for(sym_full)
    if not lk.acquire(blocking=False):
        return {"symbol": sym_full, "skipped": True, "reason": "locked"}
    refreshed_modes: List[str] = []
    failed_modes: List[str] = []
    attempts_int = max(1, int(retry_attempts or 1))
    manual = bool(failed_only or force_aux or force_overview)
    try:
        cfg = _safe_load_config().get("llm_aux") or {}
        llm_configured = bool(cfg.get("enabled") and cfg.get("base_url") and cfg.get("api_key"))
        # 1) per-mode summaries (3-min window, with immediate retry).
        if llm_configured:
            for mode in FILE_MODES:
                path = LLM_AUX_DIR / f"{sym_full}_{mode}.json"
                cached = _safe_load_json(path)
                cached_failed = _ai_payload_failed(cached)
                cached_exhausted = bool(isinstance(cached, dict) and cached.get("retry_exhausted"))
                stale = (not _llm_aux_usable(cached)) or _is_aux_stale(cached, AI_AUX_REFRESH_SECONDS)
                should_refresh = False
                if force_aux:
                    should_refresh = True
                elif failed_only:
                    # Manual failed_only: only retry failed items, never OK/fresh ones.
                    should_refresh = cached_failed
                else:
                    # Scheduled: refresh on stale, but skip exhausted errors.
                    if cached_exhausted and not cached:
                        should_refresh = True
                    elif cached_exhausted:
                        should_refresh = False
                    else:
                        should_refresh = stale
                effective_attempts = 1 if (failed_only or manual and not force_aux) else attempts_int
                if cached_exhausted and not force_aux:
                    # The previous primary model may have exhausted before the
                    # fallback model was configured. Retry once so Mine-mimo-auto
                    # can recover the cache, then normal exhausted-skip rules apply.
                    should_refresh = True
                    effective_attempts = 1
                if not should_refresh:
                    continue
                # force_aux uses retry_attempts as supplied (default 3).
                if force_aux:
                    effective_attempts = attempts_int
                def _do_aux(_mode=mode):
                    return _build_llm_aux_on_demand(sym_full, _mode)
                try:
                    aux = _retry_call(_do_aux, attempts=effective_attempts, delay=1.0)
                except Exception:
                    aux = {"status": "error", "error": "retry_call_crashed"}
                if isinstance(aux, dict):
                    if not aux.get("generated_at"):
                        aux["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    # Persist the (possibly errored) payload so the UI can
                    # see the failure marker and so the scheduler knows it
                    # is retry_exhausted.
                    try:
                        _safe_write_json(path, aux)
                    except Exception:
                        pass
                if _ai_payload_failed(aux):
                    failed_modes.append(mode)
                else:
                    refreshed_modes.append(mode)
        # 2) overview (1-min window, with immediate retry).
        overview_path = AI_OVERVIEW_DIR / f"{sym_full}.json"
        existing = _safe_load_json(overview_path)
        existing_failed = _ai_payload_failed(existing)
        existing_exhausted = bool(isinstance(existing, dict) and existing.get("retry_exhausted"))
        should_overview = False
        if force_overview:
            should_overview = True
        elif failed_only:
            should_overview = existing_failed
        else:
            # Scheduled: refresh on stale, but skip exhausted errors unless
            # the cache file is genuinely missing (cache_missing).
            cache_missing = (not isinstance(existing, dict)) or (str(existing.get("status") or "") == "cache_missing")
            if existing_exhausted and not cache_missing:
                # Allow one recovery attempt after adding fallback_models; if
                # both primary and fallback fail, _retry_call will mark it
                # exhausted again and future cycles skip it.
                should_overview = True
            else:
                should_overview = _overview_is_stale(existing, AI_OVERVIEW_REFRESH_SECONDS)
        if not should_overview:
            status_val = existing.get("status") if isinstance(existing, dict) else None
            return {
                "symbol": sym_full,
                "refreshed": False,
                "modes": refreshed_modes,
                "failed_modes": failed_modes,
                "status": status_val,
                "failed_only": failed_only,
            }
        effective_overview_attempts = 1 if failed_only else attempts_int
        if force_overview:
            effective_overview_attempts = attempts_int
        def _do_overview():
            return _build_ai_overview(sym_full)
        try:
            payload = _retry_call(_do_overview, attempts=effective_overview_attempts, delay=1.0)
        except Exception as exc:
            payload = {"status": "error", "error": str(exc), "retry_exhausted": True}
        try:
            _safe_write_json(overview_path, payload)
        except Exception:
            pass
        return {
            "symbol": sym_full,
            "refreshed": True,
            "modes": refreshed_modes,
            "failed_modes": failed_modes,
            "status": payload.get("status") if isinstance(payload, dict) else None,
            "failed_only": failed_only,
        }
    finally:
        try:
            lk.release()
        except Exception:
            pass



_AI_REFRESHER_STARTED = False
_AI_REFRESHER_GUARD = threading.Lock()


def _ai_refresher_loop() -> None:
    """Background daemon: loop over symbols, refreshing AI cache on schedule.

    Sleeps in 5-second increments so a graceful shutdown / config reload can
    affect the cadence without waiting a full minute. Each symbol refresh is
    wrapped in try/except so one broken symbol never stops the loop.
    """
    # Stagger initial cold start so we don't hammer the gateway at boot.
    time.sleep(2)
    while True:
        try:
            symbols = _symbol_list_full()
            for sym in symbols:
                try:
                    _refresh_ai_for_symbol(sym)
                except Exception:
                    pass
                # Brief gap between symbols to avoid bursts.
                time.sleep(1)
        except Exception:
            pass
        # Sleep ~AI_OVERVIEW_REFRESH_SECONDS in 5s chunks.
        total = max(5, int(AI_OVERVIEW_REFRESH_SECONDS))
        slept = 0
        while slept < total:
            time.sleep(5)
            slept += 5


def _start_ai_refresher() -> None:
    """Kick off the AI cache background thread; never blocks startup."""
    global _AI_REFRESHER_STARTED
    with _AI_REFRESHER_GUARD:
        if _AI_REFRESHER_STARTED:
            return
        _AI_REFRESHER_STARTED = True
    try:
        t = threading.Thread(target=_ai_refresher_loop, name="ai-cache-refresher", daemon=True)
        t.start()
    except Exception:
        # Don't crash app on thread start failure; the read endpoints still
        # serve cache_missing payloads and trigger a best-effort refresh on
        # first request.
        pass


# FastAPI 0.133 removed ``FastAPI.add_event_handler`` while Starlette keeps
# the router-level lifecycle API.  Register there so old deployments and the
# current runtime share one compatible startup path.
app.router.add_event_handler("startup", _start_ai_refresher)


@app.get("/ai-analysis/{symbol}")
def get_ai_analysis(symbol: str):
    """返回后端缓存的 AI 总览（按 60s 调度生成）。

    The browser never triggers a live LLM call from this endpoint; the
    backend cache is owned by the ``_ai_refresher_loop`` daemon. When the
    cache file is missing we kick off a best-effort, non-blocking refresh
    in a daemon thread so the next poll receives data; we still return the
    shaped ``cache_missing`` payload immediately so the UI never hangs.
    """
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    payload = _read_ai_overview(sym_full)
    if payload.get("status") == "cache_missing":
        lk = _ai_lock_for(sym_full)
        if not lk.locked():
            def _kick():
                try:
                    _refresh_ai_for_symbol(sym_full)
                except Exception:
                    pass
            try:
                threading.Thread(target=_kick, name=f"ai-kick-{sym_full}", daemon=True).start()
            except Exception:
                pass
    return payload


@app.get("/ai-analysis/{symbol}/stream")
def stream_ai_analysis(symbol: str):
    """兼容旧前端：仅按缓存数据 SSE 流式输出，不调用 LLM。

    Emits: start, mode_done (cached per-mode aux), overview_done, done.
    Generation is owned by the backend scheduler; this endpoint exists
    only so older clients keep working without forcing a live LLM call.
    """
    def gen() -> Iterable[str]:
        sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
        sym_full = sym_full.upper()
        overview = _read_ai_overview(sym_full)
        model = str(overview.get("model") or (_safe_load_config().get("llm_aux") or {}).get("model") or "-")
        yield _sse("start", {"symbol": sym_full, "model": model, "message": "读取后端缓存的AI数据...", "cache": True})
        saved_summaries: Dict[str, Any] = {}
        for idx, mode in enumerate(FILE_MODES, start=1):
            aux = _safe_load_json(LLM_AUX_DIR / f"{sym_full}_{mode}.json")
            if aux:
                aux = _normalize_llm_aux_payload(aux)
            if not aux:
                aux = {
                    "status": "cache_missing",
                    "summary": f"{mode} 暂无缓存AI摘要；后端将在 3 分钟内补齐。",
                }
            saved_summaries[mode] = aux
            event = "mode_done" if _llm_aux_usable(aux) else "mode_error"
            yield _sse(event, {"mode": mode, "index": idx, "total": len(FILE_MODES), "payload": aux, "error": aux.get("error"), "cache": True})
        if overview.get("status") in ("ok", "fallback"):
            yield _sse("overview_done", {
                "status": overview.get("status"),
                "symbol": sym_full,
                "model": overview.get("model") or model,
                "summary": overview.get("summary") or "",
                "saved_summaries": saved_summaries,
                "generated_at": overview.get("generated_at"),
                "updated_at": overview.get("updated_at"),
                "cache": True,
            })
            yield _sse("done", {"status": overview.get("status"), "summary": overview.get("summary") or "", "cache": True})
        else:
            yield _sse("overview_error", {
                "status": overview.get("status") or "cache_missing",
                "summary": overview.get("summary") or "AI总览缓存尚未生成。",
                "error": overview.get("error"),
                "generated_at": overview.get("generated_at"),
                "updated_at": overview.get("updated_at"),
                "cache": True,
            })
            yield _sse("done", {"status": overview.get("status") or "cache_missing", "cache": True})

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/ai-analysis/{symbol}/retry-failed")
def retry_failed_ai_analysis(symbol: str):
    """Retry only failed per-mode aux / overview cache entries for one symbol.

    * If a refresh / retry is already in progress for this symbol the per-
      symbol lock is held; we return ``status="running"`` with
      ``queued=False`` and never spawn another worker -- this prevents
      repeated UI clicks from queuing infinite background jobs.
    * Otherwise we launch ONE daemon thread that calls
      :func:`_refresh_ai_for_symbol` with ``failed_only=True`` and
      ``retry_attempts=1`` so OK/fresh items are NOT regenerated, and the
      retry happens immediately (not on the 1-/3-minute schedule).
    """
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    lk = _ai_lock_for(sym_full)
    if lk.locked():
        return {
            "status": "running",
            "queued": False,
            "symbol": sym_full,
            "message": "后台正在生成/重试，未创建重复任务",
        }
    def _worker():
        try:
            _refresh_ai_for_symbol(sym_full, failed_only=True, retry_attempts=1)
        except Exception:
            pass
    try:
        threading.Thread(
            target=_worker,
            name=f"ai-retry-failed-{sym_full}",
            daemon=True,
        ).start()
    except Exception:
        return {
            "status": "error",
            "queued": False,
            "symbol": sym_full,
            "message": "无法启动重试线程",
        }
    return {"status": "started", "queued": True, "symbol": sym_full}


@app.get("/ai-analysis/{symbol}/cache")
def get_ai_analysis_cache(symbol: str):
    """显式只读缓存端点（等同于 GET /ai-analysis/{symbol} 的缓存读取）。"""
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    return _read_ai_overview(sym_full)


# ---------------------------------------------------------------------------
# 评估闭环 API（只读）
# ---------------------------------------------------------------------------

@app.get("/evaluation/summary")
def get_evaluation_summary():
    """返回 ``model_results/evaluation/summary.json``；不存在时返回安全空载荷。

    本接口只读已落地文件，绝不触发评估生成；调度器在结算后会异步重写该 JSON。
    """
    payload = _read_evaluation_summary()
    return payload


@app.get("/evaluation/{symbol}")
def get_evaluation_for_symbol(symbol: str):
    """返回某币种所有已存在模式的评估 JSON 列表。

    * 不存在评估文件时，``modes`` 为空且 ``available=False``；
    * 该接口为非阻塞只读，不依赖在线计算。
    """
    sym_full = symbol if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
    sym_full = sym_full.upper()
    files = _evaluation_files_for_symbol(sym_full)
    by_mode: Dict[str, Any] = {}
    for item in files:
        name = item.get("file") or ""
        if not isinstance(name, str):
            continue
        # 文件名格式 SYMBOL_MODE.json
        stem = name[:-5] if name.endswith(".json") else name
        mode_part = stem[len(sym_full) + 1:] if stem.startswith(sym_full + "_") else stem
        if mode_part:
            by_mode[mode_part] = item.get("payload") or {}
    summary = _read_evaluation_summary()
    return {
        "symbol": sym_full,
        "available": bool(by_mode),
        "modes": by_mode,
        "summary_last_updated_at": summary.get("last_updated_at"),
    }


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------

@app.get("/status")
def get_status():
    return {"status": "ok"}


if __name__ == "__main__":
    bind_host = os.environ.get("CONTROL_PLANE_BIND_HOST", "127.0.0.1")
    validate_control_plane_bind(bind_host)
    uvicorn.run(app, host=bind_host, port=8000)
