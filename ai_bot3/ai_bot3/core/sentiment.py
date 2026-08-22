"""Coinglass 中心化的事件 / 情绪信号模块。

历史背景
--------
原来的 ``Sentiment`` 走 Finnhub / NewsData / Reddit / TextBlob 第三方新闻链。
按照用户明确指令，这些通道全部作废，且 ``run_v3.sh`` 的活跃链路必须由
``liqmap_fetcher.py`` 抓取的 Coinglass 反向接口数据驱动。

当前实现要点
------------
* 同步接口签名仍然是 ``Sentiment(cfg, http)`` 与 ``await Sentiment.score(sym)``，
  保证 ``core/trainer3.py`` / ``core/inferencer3_fixed.py`` /
  ``core/portfolio3_3_fixed.py`` 不需要改调用点。
* 评分来源：

  1. 本地 ``data/{BASE}.json`` Coinglass 爆仓图：
     - dict key 为价格档位
     - entry[1] 为该档位 heat / notional
     - 当前价上方 short heat 越强 -> 越偏多 / squeeze
     - 当前价下方 long heat 越强 -> 越偏空 / cascade
  2. 本地 ``data/coinglass_metrics/events.json`` 事件分（含 BTC 锚点）。
  3. 本地 ``data/coinglass_metrics/news_context.json`` 综合上下文分。
* BTC 作为锚点：预测非 BTC 币时，BTC 自身爆仓图压力会按权重融合。
* 30~45 分钟随机刷新；BTC 急涨急跌时强制刷新。
* 评分输出 ``[-1, 1]``：正数偏多、负数偏空。
* 元数据写入 ``self.last_events`` / ``self.last_details`` 便于 API/前端调试。
"""

from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .http_client import HTTPClient


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = _PROJECT_ROOT / "data"
_METRICS_DIR = _DATA_DIR / "coinglass_metrics"


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


class Sentiment:
    """Coinglass-only 情绪 / 事件评分器。

    由 ``core/portfolio3_3_fixed.PortfolioPredictor`` 注入到训练 / 推理链。
    """

    # 30 分钟到 45 分钟之间随机刷新
    refresh_min_sec = 30 * 60
    refresh_max_sec = 45 * 60

    # BTC 价格变化超过 0.8% 时强制刷新
    btc_move_threshold = 0.008

    def __init__(self, cfg: dict, http: HTTPClient):
        self.cfg = cfg
        self.http = http
        self.log = logging.getLogger("Sentiment")
        self.data_dir = Path(cfg.get("general", {}).get("db_dir", str(_DATA_DIR)))
        self.metrics_dir = self.data_dir / "coinglass_metrics"
        self.events_file = self.metrics_dir / "events.json"
        self.news_context_file = self.metrics_dir / "news_context.json"

        # 缓存：每个 symbol 的 (next_refresh_ts, last_score, last_details)
        self.cache: Dict[str, Tuple[float, float, Dict[str, Any]]] = {}
        self.last_events: List[Dict[str, Any]] = []
        self.last_details: Dict[str, Dict[str, Any]] = {}
        self._last_btc_price: Optional[float] = None

        # 配置：events 融合权重 / 新鲜度上限
        sent_cfg = cfg.get("sentiment", {}) if isinstance(cfg.get("sentiment"), dict) else {}
        self.event_blend_weight = float(sent_cfg.get("event_blend_weight", 0.5))
        self.event_max_age_sec = int(sent_cfg.get("event_max_age_sec", 4 * 3600))
        self.event_symbol_bonus = float(sent_cfg.get("event_symbol_bonus", 0.2))
        # BTC 锚点权重（预测非 BTC 时使用）
        self.btc_anchor_weight = float(sent_cfg.get("btc_anchor_weight", 0.15))

    # ------------------------------------------------------------------ helpers
    def _liqmap_path(self, sym: str) -> Path:
        base = sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
        return self.data_dir / f"{base}.json"

    def _analyse(self, sym: str) -> Dict[str, Any]:
        """读取本地爆仓图 ``liqMapV2`` 计算多空 heat / imbalance。"""
        path = self._liqmap_path(sym)
        result = {
            "symbol": sym,
            "last_price": None,
            "long_below": 0.0,
            "short_above": 0.0,
            "total_heat": 0.0,
            "imbalance": 0.0,
            "nearest_long_distance": None,
            "nearest_short_distance": None,
            "available": False,
        }
        if not path.exists():
            return result
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                return result
            last_price = float(payload.get("lastPrice") or 0.0)
            liq_map = payload.get("liqMapV2") or {}
            if last_price <= 0 or not liq_map:
                return result
            long_below = 0.0
            short_above = 0.0
            nearest_long: Optional[float] = None
            nearest_short: Optional[float] = None
            for key_str, entries in liq_map.items():
                try:
                    # liqMapV2 schema：dict key 即价格档
                    key_price = float(key_str)
                except Exception:
                    continue
                for entry in entries or ():
                    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                        continue
                    try:
                        # entry[1] 是 heat / notional
                        h = float(entry[1])
                    except Exception:
                        continue
                    p = key_price
                    if p < last_price:
                        long_below += h
                        d = (last_price - p) / last_price
                        if nearest_long is None or d < nearest_long:
                            nearest_long = d
                    elif p > last_price:
                        short_above += h
                        d = (p - last_price) / last_price
                        if nearest_short is None or d < nearest_short:
                            nearest_short = d
            denom = long_below + short_above
            imbalance = 0.0 if denom <= 0 else (short_above - long_below) / denom
            result.update({
                "last_price": last_price,
                "long_below": long_below,
                "short_above": short_above,
                "total_heat": denom,
                "imbalance": max(-1.0, min(1.0, imbalance)),
                "nearest_long_distance": nearest_long,
                "nearest_short_distance": nearest_short,
                "available": True,
            })
        except Exception as exc:
            self.log.debug(f"_analyse({sym}) 异常: {exc}")
        return result

    def _load_events(self) -> Tuple[List[Dict[str, Any]], bool, Optional[str]]:
        """读取 events.json，返回 (events, synthetic, generated_at)."""
        wrapper = _safe_load_json(self.events_file) or {}
        items = wrapper.get("items") or wrapper.get("events") or []
        synthetic = bool(wrapper.get("synthetic", False))
        generated_at = wrapper.get("generated_at")
        # 新鲜度过滤
        now = time.time()
        fresh: List[Dict[str, Any]] = []
        for it in items:
            try:
                ts = it.get("ts")
                if isinstance(ts, str):
                    ts = float(ts)
                if not ts:
                    fresh.append(it)
                    continue
                if now - float(ts) <= self.event_max_age_sec:
                    fresh.append(it)
            except Exception:
                continue
        return fresh, synthetic, generated_at

    def _events_score_for(
        self,
        events: List[Dict[str, Any]],
        sym: str,
    ) -> Tuple[float, int]:
        """对一组事件聚合分数，对 sym 直接相关的条目加权。"""
        if not events:
            return 0.0, 0
        base = sym.upper().replace("USDT", "").replace("USD", "").replace("BINANCE_", "")
        total = 0.0
        cnt = 0
        for it in events:
            try:
                s = float(it.get("score") if it.get("score") is not None else it.get("sentiment_score") or 0.0)
            except Exception:
                continue
            title = str(it.get("title") or "").upper()
            if base and base in title:
                s = max(-1.0, min(1.0, s + (self.event_symbol_bonus if s >= 0 else -self.event_symbol_bonus)))
            total += s
            cnt += 1
        return total / cnt if cnt else 0.0, cnt

    # ----------------------------------------------------------------- API
    async def score(self, sym: str) -> float:
        """对外接口：返回 ``[-1, 1]`` 的情绪分。"""
        key = sym.upper()
        now = time.time()

        # 缓存命中且未到刷新时间
        cached = self.cache.get(key)
        if cached and now < cached[0]:
            self.last_details[key] = cached[2]
            return cached[1]

        # 1) 本地 liqmap 分析
        my_snap = self._analyse(sym)
        liqmap_score = my_snap["imbalance"]

        # 2) BTC 锚点
        btc_snap = self._analyse("BTC") if not key.startswith("BTC") else my_snap
        btc_anchor = btc_snap["imbalance"] if btc_snap["available"] else 0.0

        # BTC 急涨急跌：清理缓存（强制下一次重新计算）
        if btc_snap["available"] and self._last_btc_price:
            try:
                change = abs(btc_snap["last_price"] - self._last_btc_price) / self._last_btc_price
                if change >= self.btc_move_threshold:
                    self.cache.clear()
            except Exception:
                pass
        if btc_snap["available"]:
            self._last_btc_price = btc_snap["last_price"]

        # 3) events.json
        events, synthetic, events_generated_at = self._load_events()
        events_score, events_count = self._events_score_for(events, sym)
        self.last_events = events

        # 4) news_context.json（如果存在）
        ctx_wrapper = _safe_load_json(self.news_context_file) or {}
        ctx_score = 0.0
        try:
            scores = ctx_wrapper.get("scores") or {}
            ctx_score = float(scores.get("news_context_score") or 0.0)
        except Exception:
            ctx_score = 0.0

        # 融合：本地 liqmap 主权 + 事件融合权 + 上下文权 + BTC 锚点权
        base_score = (1.0 - self.event_blend_weight) * liqmap_score \
                   + self.event_blend_weight * events_score
        if not key.startswith("BTC"):
            base_score = (1.0 - self.btc_anchor_weight) * base_score \
                       + self.btc_anchor_weight * btc_anchor
        # news_context 给一个轻量加权
        base_score = max(-1.0, min(1.0, 0.85 * base_score + 0.15 * ctx_score))

        details = {
            "symbol": sym,
            "liqmap_score": liqmap_score,
            "events_score": events_score,
            "events_count": events_count,
            "events_blend_weight": self.event_blend_weight,
            "events_synthetic": synthetic,
            "events_generated_at": events_generated_at,
            "news_context_score": ctx_score,
            "news_context_generated_at": ctx_wrapper.get("generated_at"),
            "btc_anchor_score": btc_anchor,
            "btc_anchor_weight": self.btc_anchor_weight,
            "final_score": base_score,
            "snapshot": my_snap,
        }
        self.last_details[key] = details

        # 写缓存：30~45 分钟随机刷新
        next_refresh = now + random.randint(self.refresh_min_sec, self.refresh_max_sec)
        self.cache[key] = (next_refresh, base_score, details)
        return base_score
