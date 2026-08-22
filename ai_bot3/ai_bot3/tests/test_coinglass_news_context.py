"""liqmap_fetcher 上下文/新闻数据栈语义测试（不发网络请求）。"""

import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_persist_wrapper_unavailable_keeps_old_data_and_alerts(monkeypatch):
    """status=unavailable 时应保留旧 data/items，并触发飞书告警。"""
    import liqmap_fetcher as lf

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "metric.json"
        # 先放一份旧数据
        path.write_text(json.dumps({
            "generated_at": "2026-01-01T00:00:00",
            "status": "ok",
            "data": {"value": 1},
            "items": [{"a": 1}],
        }))
        # 用 monkeypatch 拦截飞书告警，不真正发送
        sent = {}

        def fake_send(key, msg, cooldown_seconds=1800):
            sent["key"] = key
            sent["msg"] = msg
            return True

        monkeypatch.setattr(lf, "send_error_warning_with_cooldown", fake_send)

        fetcher = lf.CoinglassFetcher.__new__(lf.CoinglassFetcher)
        fetcher.alert_cooldown_sec = 1800

        wrapper = lf.CoinglassFetcher._persist_wrapper(
            fetcher, path,
            source="x", metric="financial_calendar",
            status="unavailable", error="endpoint not verified",
        )
        # 旧 data/items 应被保留
        assert wrapper["data"] == {"value": 1}
        assert wrapper["items"] == [{"a": 1}]
        # 飞书告警被触发
        assert sent.get("key") == "coinglass:financial_calendar:unavailable"
        assert "Coinglass" in sent.get("msg", "")


def test_send_error_warning_with_cooldown_dedupes_within_window(monkeypatch):
    """同一 key 在 cooldown_seconds 内只发送一次。"""
    from utils import send_util as su

    sent_count = {"n": 0}

    def fake_send(msg):
        sent_count["n"] += 1

    monkeypatch.setattr(su, "send_error_warning", fake_send)
    # 重置内部冷却字典
    with su._alert_cooldown_lock:
        su._alert_cooldown_state.clear()

    key = "coinglass:financial_calendar:unavailable"
    assert su.send_error_warning_with_cooldown(key, "msg1", cooldown_seconds=10) is True
    # 第二次立即触发，应被冷却拦截
    assert su.send_error_warning_with_cooldown(key, "msg2", cooldown_seconds=10) is False
    assert sent_count["n"] == 1


def test_normalize_financial_calendar_synthetic_payload():
    """合成 Coinglass 财经日历原始 list 应正确归一化。"""
    from liqmap_fetcher import normalize_financial_calendar

    raw = [
        {
            "title": "US CPI",
            "country": "US",
            "currency": "USD",
            "importance": 3,
            "time": "2026-05-06T13:30:00Z",
            "actual": 3.4, "forecast": 3.2, "previous": 3.5,
            "type": "indicator",
            "impactTags": ["macro", "rates"],
        },
        {"event": "Powell speech", "region": "US", "currency": "USD", "importance": 2},
        "garbage_should_be_skipped",
    ]
    items = normalize_financial_calendar(raw, source="coinglass_calendar")
    assert len(items) == 2
    cpi = items[0]
    assert cpi["title"] == "US CPI"
    assert cpi["country"] == "US"
    assert cpi["importance"] == 3
    # actual > forecast 应该是正向 sentiment
    assert cpi["sentiment_score"] > 0
    # 第二条没有 actual/forecast，sentiment 应为 0
    assert items[1]["sentiment_score"] == 0.0
    # 必备字段都在
    for required in ("generated_at", "source", "type", "title", "event_time", "sentiment_score"):
        assert required in cpi


def test_normalize_whale_alert_dedupes_with_stable_id():
    """鲸鱼大额转账归一化：稳定 ID 去重，已见 ID 不重复输出。"""
    from liqmap_fetcher import normalize_whale_alert, _stable_whale_id

    raw = [
        {"txId": "0xaaaa", "amountUsd": 5_000_000, "to": "BINANCE_HOT", "from": "wallet1", "symbol": "BTC"},
        {"txId": "0xaaaa", "amountUsd": 5_000_000, "to": "BINANCE_HOT", "from": "wallet1", "symbol": "BTC"},
        {"hash": "0xbbbb", "amountUsd": 2_000_000, "to": "wallet2", "from": "okx_cold", "symbol": "ETH"},
    ]
    items, new_seen = normalize_whale_alert(raw, seen={})
    # 第二条与第一条相同 txId，应被去重
    assert len(items) == 2
    assert _stable_whale_id(raw[0]) in new_seen
    # 流入交易所的 sentiment 应为负
    btc_item = [i for i in items if i["symbol"] == "BTC"][0]
    assert btc_item["direction_hint"] == "inflow_to_exchange"
    assert btc_item["sentiment_score"] < 0
    # 流出交易所的 sentiment 应为正
    eth_item = [i for i in items if i["symbol"] == "ETH"][0]
    assert eth_item["direction_hint"] == "outflow_from_exchange"
    assert eth_item["sentiment_score"] > 0

    # 第二次调用，传入上次 seen，应没有任何新增
    items2, _ = normalize_whale_alert(raw, seen=new_seen)
    assert items2 == []


def test_normalize_fear_greed_maps_to_sentiment_range():
    """恐惧贪婪指数 0~100 应映射到 [-1, +1]。"""
    from liqmap_fetcher import normalize_fear_greed

    # 极度恐惧
    fg = normalize_fear_greed({"value": 0, "classification": "extreme fear"})
    assert fg["sentiment_score"] == -1.0
    # 中性
    fg = normalize_fear_greed({"value": 50, "classification": "neutral"})
    assert abs(fg["sentiment_score"]) < 1e-9
    # 极度贪婪
    fg = normalize_fear_greed({"value": 100, "classification": "extreme greed"})
    assert fg["sentiment_score"] == 1.0
    # 非法 payload
    assert normalize_fear_greed("not a dict") is None
    assert normalize_fear_greed({"value": "n/a"}) is None


def test_persist_wrapper_status_ok_does_not_alert(monkeypatch):
    """status=ok 时不应触发飞书告警。"""
    import liqmap_fetcher as lf

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "metric.json"
        sent = {"called": False}

        def fake_send(key, msg, cooldown_seconds=1800):
            sent["called"] = True
            return True

        monkeypatch.setattr(lf, "send_error_warning_with_cooldown", fake_send)

        fetcher = lf.CoinglassFetcher.__new__(lf.CoinglassFetcher)
        fetcher.alert_cooldown_sec = 1800

        wrapper = lf.CoinglassFetcher._persist_wrapper(
            fetcher, path,
            source="coinglass_calendar", metric="financial_calendar",
            status="ok", items=[{"x": 1}],
        )
        assert wrapper["status"] == "ok"
        assert sent["called"] is False


def test_news_context_aggregation_per_symbol(monkeypatch):
    """合成上下文 wrappers，refresh_news_context 应输出 per_symbol 与 anchor_weights。"""
    import liqmap_fetcher as lf

    with tempfile.TemporaryDirectory() as tmp:
        # 改写 wrapper 文件目标到临时目录
        metrics_dir = Path(tmp) / "coinglass_metrics"
        metrics_dir.mkdir()
        monkeypatch.setattr(lf, "METRICS_DIR", metrics_dir)
        monkeypatch.setattr(lf, "EVENTS_FILE", metrics_dir / "events.json")
        monkeypatch.setattr(lf, "FINANCIAL_CALENDAR_FILE", metrics_dir / "financial_calendar.json")
        monkeypatch.setattr(lf, "WHALE_ALERT_FILE", metrics_dir / "whale_alert.json")
        monkeypatch.setattr(lf, "FEAR_GREED_FILE", metrics_dir / "fear_greed_index.json")
        monkeypatch.setattr(lf, "NEWS_CONTEXT_FILE", metrics_dir / "news_context.json")

        # 写 events / financial_calendar / whale / fear_greed 三类文件
        (metrics_dir / "events.json").write_text(json.dumps({
            "generated_at": "2026-05-06T00:00:00",
            "status": "ok",
            "items": [{"score": 0.4}, {"score": 0.6}],
        }))
        (metrics_dir / "financial_calendar.json").write_text(json.dumps({
            "generated_at": "2026-05-06T00:00:00",
            "status": "ok",
            "items": [{"sentiment_score": 0.3, "importance": 3}],
        }))
        (metrics_dir / "whale_alert.json").write_text(json.dumps({
            "generated_at": "2026-05-06T00:00:00",
            "status": "ok",
            "items": [
                {"symbol": "BTC", "sentiment_score": -0.6, "direction_hint": "inflow_to_exchange"},
                {"symbol": "ETH", "sentiment_score": 0.6, "direction_hint": "outflow_from_exchange"},
            ],
        }))
        (metrics_dir / "fear_greed_index.json").write_text(json.dumps({
            "generated_at": "2026-05-06T00:00:00",
            "status": "ok",
            "data": {"sentiment_score": 0.2},
        }))

        fetcher = lf.CoinglassFetcher.__new__(lf.CoinglassFetcher)
        fetcher.symbol_list = ["Binance_BTCUSDT", "Binance_ETHUSDT"]
        # 调用合成
        lf.CoinglassFetcher.refresh_news_context(fetcher)

        nc = json.loads((metrics_dir / "news_context.json").read_text())
        scores = nc["scores"]
        # 财经日历 importance=3 应对应 macro_event_importance ≈ 1.0
        assert abs(scores["macro_event_importance"] - 1.0) < 1e-6
        # BTC 大额转入交易所 -> per_symbol BTC 应为负
        assert nc["per_symbol"]["BTC"]["whale_sentiment_avg"] < 0
        # ETH 流出交易所 -> 正
        assert nc["per_symbol"]["ETH"]["whale_sentiment_avg"] > 0
        # anchor_weights 五项必须都存在
        for k in ("macro_calendar_weight", "whale_flow_weight", "fear_greed_weight",
                  "liquidation_map_weight", "btc_anchor_weight"):
            assert k in nc["anchor_weights"]
        assert "source_times" in nc and "source_status" in nc
