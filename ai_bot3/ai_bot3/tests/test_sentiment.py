"""core/sentiment.py 综合测试。"""

import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sentiment import Sentiment


class _DummyHttp:
    pass


def _build_cfg(data_dir: Path):
    return {
        "general": {"db_dir": str(data_dir)},
        "api": {"intervals": {}, "reddit": {"user_agent": "x"}},
        "prediction": {"news_cache_ttl": 600},
        "sentiment": {
            "event_blend_weight": 0.5,
            "event_max_age_sec": 4 * 3600,
            "event_symbol_bonus": 0.2,
            "btc_anchor_weight": 0.0,
        },
    }


def test_short_heat_above_price_returns_positive():
    """合成 liqmap：上方 short heat > 下方 long heat -> 正分 / 偏多。"""
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        liq_payload = {
            "lastPrice": "100",
            "liqMapV2": {
                "99":  [[0, 10, 0]],
                "98":  [[1, 5, 0]],
                "101": [[2, 30, 0]],
                "102": [[3, 40, 0]],
            },
        }
        (data_dir / "BTC.json").write_text(json.dumps(liq_payload), encoding="utf-8")
        (data_dir / "BTCUSDT.json").write_text(json.dumps(liq_payload), encoding="utf-8")
        # 无 events
        cfg = _build_cfg(data_dir)
        s = Sentiment(cfg, _DummyHttp())
        score = asyncio.run(s.score("BTCUSDT"))
        assert isinstance(score, float)
        assert score > 0, f"上方 short 压力更强应返回正分，实际 {score}"


def test_long_heat_below_price_returns_negative():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        liq_payload = {
            "lastPrice": "100",
            "liqMapV2": {
                "99":  [[0, 80, 0]],
                "98":  [[1, 60, 0]],
                "101": [[2, 5, 0]],
                "102": [[3, 5, 0]],
            },
        }
        (data_dir / "BTC.json").write_text(json.dumps(liq_payload), encoding="utf-8")
        (data_dir / "BTCUSDT.json").write_text(json.dumps(liq_payload), encoding="utf-8")
        cfg = _build_cfg(data_dir)
        s = Sentiment(cfg, _DummyHttp())
        score = asyncio.run(s.score("BTCUSDT"))
        assert score < 0


def test_event_blends_with_liqmap():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        liq_payload = {
            "lastPrice": "100",
            "liqMapV2": {"99": [[0, 1, 0]], "101": [[1, 1, 0]]},  # 平衡
        }
        (data_dir / "BTC.json").write_text(json.dumps(liq_payload))
        (data_dir / "BTCUSDT.json").write_text(json.dumps(liq_payload))
        # 强烈正向 events
        metrics_dir = data_dir / "coinglass_metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        (metrics_dir / "events.json").write_text(json.dumps({
            "generated_at": "2026-01-01T00:00:00",
            "ts": int(time.time()),
            "items": [{"ts": int(time.time()), "title": "BTCUSDT BULL", "score": 0.9}],
        }))
        cfg = _build_cfg(data_dir)
        s = Sentiment(cfg, _DummyHttp())
        score = asyncio.run(s.score("BTCUSDT"))
        assert score > 0.0
