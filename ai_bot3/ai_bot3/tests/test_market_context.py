"""core/market_context.py 综合测试。"""

import os
import sys
import tempfile
import json
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_context import (
    summarize_liquidation_map,
    build_market_feature_snapshot,
    assess_context_completeness,
    adaptive_context_weights,
    compute_market_bias,
    fuse_direction_signals,
    OpenAIFormatSignalClient,
    _extract_last_and_change,
)


def test_summarize_liquidation_map_imbalance():
    payload = {
        "lastPrice": "100",
        "liqMapV2": {
            "99": [[0, 10, 0]],
            "101": [[0, 30, 0]],
        },
    }
    s = summarize_liquidation_map(payload)
    assert s["imbalance"] > 0  # short heat 更强
    assert s["levels_total"] == 2


def test_extract_last_and_change_chart_payload():
    last, chg = _extract_last_and_change([[1, 100], [2, 110]])
    assert abs(last - 110) < 1e-6
    assert abs(chg - 0.1) < 1e-6


def test_extract_last_and_change_list_of_dicts():
    last, chg = _extract_last_and_change([{"value": 1.0}, {"value": 1.2}])
    assert abs(last - 1.2) < 1e-6
    assert abs(chg - 0.2) < 1e-6


def test_extract_last_and_change_dict_chart():
    last, chg = _extract_last_and_change({"list": [[1, 0.01], [2, 0.012]]})
    assert abs(last - 0.012) < 1e-6
    assert abs(chg - 0.2) < 1e-6


def test_completeness_and_adaptive_weights():
    # 高完整度
    completeness = {"score": 0.9, "sources": {}, "missing": [], "generated_at": "x"}
    w = adaptive_context_weights(completeness, news_context_score=0.3, macro_event_importance=0.5)
    assert abs(sum(w.values()) - 1.0) < 1e-6
    # 低完整度
    completeness2 = {"score": 0.1, "sources": {}, "missing": [], "generated_at": "x"}
    w2 = adaptive_context_weights(completeness2, news_context_score=0.0)
    # 高完整度时新闻权重应更高
    assert w["news_context"] > w2["news_context"]


def test_compute_market_bias_includes_required_fields():
    snap = {"liquidation_imbalance": 0.5, "funding_rate": 0.0001,
            "open_interest_change": 0.05, "volume_24h_change": 0.1,
            "long_short_ratio": 1.2, "long_short_ratio_change": 0.05,
            "taker_buy_sell_ratio": 1.1, "news_context_score": 0.2,
            "fear_greed_score": 0.3, "whale_alert_score": -0.1,
            "macro_event_importance": 0.0}
    completeness = {"score": 0.7, "sources": {}, "missing": [], "generated_at": "x"}
    bias = compute_market_bias(snap, completeness)
    assert -1.0 <= bias["factor_bias"] <= 1.0
    assert "weights" in bias and "news_weight_total" in bias
    assert "context_completeness" in bias


def test_fuse_direction_signals_produces_direction():
    completeness = {"score": 0.6, "sources": {}, "missing": [], "generated_at": "x"}
    fused = fuse_direction_signals(
        local_predicted_return=0.005,
        factor_bias=0.4,
        news_signal=0.3,
        llm_signal=0.2,
        completeness=completeness,
    )
    assert fused["direction"] in {"up", "down", "flat"}
    assert -1.0 <= fused["fused_score"] <= 1.0
    assert abs(sum(fused["fused_weights"].values()) - 1.0) < 1e-6


def test_missing_factor_news_and_llm_do_not_dilute_local_model():
    completeness = {
        "score": 0.0,
        "sources": {"liqmap": False, "funding_rate": False, "news_context": False},
        "missing": ["liqmap", "funding_rate", "news_context"],
    }
    fused = fuse_direction_signals(
        local_predicted_return=0.005,
        factor_bias=0.0,
        news_signal=0.0,
        llm_signal=0.0,
        completeness=completeness,
        llm_available=False,
    )
    assert fused["fused_weights"]["local_model"] == 1.0


def test_llm_aux_disabled_returns_neutral():
    client = OpenAIFormatSignalClient({"enabled": False})
    payload = client.predict(
        symbol="BTCUSDT", mode="scalping",
        snapshot={}, completeness={"score": 0.0},
    )
    assert payload["score"] == 0.0
    assert payload["direction"] == "flat"
    assert payload["status"] == "disabled"


def test_llm_credentials_are_loaded_from_environment_not_repository(monkeypatch):
    monkeypatch.setenv("AI_BOT_LLM_API_KEY", "test-runtime-key")
    client = OpenAIFormatSignalClient({"enabled": False, "api_key": ""})
    assert client.api_key == "test-runtime-key"


def test_llm_aux_network_failure_returns_neutral(monkeypatch):
    """OpenAI 兼容接口网络失败时应回退中性，不抛异常。"""
    client = OpenAIFormatSignalClient({
        "enabled": True,
        "base_url": "http://127.0.0.1:1",  # 故意不可达
        "api_key": "sk-fake",
        "model": "gpt-4o-mini",
        "timeout": 1,
        "cache_ttl": 0,
    })
    payload = client.predict(
        symbol="BTCUSDT", mode="scalping",
        snapshot={}, completeness={"score": 0.0},
    )
    assert payload["score"] == 0.0
    assert payload["direction"] == "flat"
    assert payload["status"] in {"error", "disabled"}


def test_llm_aux_parses_fenced_json_response(monkeypatch):
    """OpenAI/Qwen compatible gateways may wrap JSON in markdown fences."""
    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{
                    "message": {
                        "content": '```json\n{"direction":"up","score":0.42,"confidence":0.7,"prediction_value":1.23,"summary":"AI摘要","anchors":["a"],"risk_flags":["r"]}\n```'
                    }
                }]
            }

        def iter_lines(self, decode_unicode=False):
            yield 'data: {"choices":[{"delta":{"content":"```json\\n{\\\"direction\\\":\\\"up\\\",\\\"score\\\":0.42,\\\"confidence\\\":0.7,"}}]}'
            yield 'data: {"choices":[{"delta":{"content":"\\\"prediction_value\\\":1.23,\\\"summary\\\":\\\"AI摘要\\\",\\\"anchors\\\":[\\\"a\\\"],\\\"risk_flags\\\":[\\\"r\\\"]}\\n```"}}]}'
            yield 'data: [DONE]'

    def fake_post(*args, **kwargs):
        return FakeResp()

    import requests
    monkeypatch.setattr(requests, "post", fake_post)
    with tempfile.TemporaryDirectory() as tmp:
        client = OpenAIFormatSignalClient({
            "enabled": True,
            "base_url": "http://example.test/v1",
            "api_key": "sk-fake",
            "model": "qwen-test",
            "persist_dir": tmp,
            "cache_ttl": 0,
        })
        payload = client.predict(symbol="BTCUSDT", mode="scalping", snapshot={}, completeness={"score": 1.0})
    assert payload["status"] == "ok"
    assert payload["direction"] == "up"
    assert payload["summary"] == "AI摘要"


def test_build_snapshot_with_local_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        metrics = Path(tmp) / "coinglass_metrics"
        metrics.mkdir(parents=True)
        (metrics / "BTC_funding_rate.json").write_text(json.dumps({
            "status": "ok",
            "data": [{"value": 0.0001}, {"value": 0.00012}],
        }))
        (metrics / "BTC_open_interest.json").write_text(json.dumps({
            "status": "ok",
            "data": {"openInterest": 100, "h24OIChangePercent": 5.0},
        }))
        snap = build_market_feature_snapshot(metrics_dir=metrics, base="BTC")
        assert abs(snap["funding_rate"] - 0.00012) < 1e-9
        assert snap["open_interest_amount"] == 100
        assert abs(snap["open_interest_change"] - 0.05) < 1e-9


def test_completeness_explicit_data_dir_finds_liqmap():
    """显式传入 data_dir 时应能识别本地爆仓图为可用源。"""
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        metrics = data_dir / "coinglass_metrics"
        metrics.mkdir()
        # 爆仓图原文件没有 status 字段，但 lastPrice + liqMapV2 即视为 ok
        (data_dir / "BTC.json").write_text(json.dumps({
            "lastPrice": "100",
            "liqMapV2": {"99": [[0, 1, 0]]},
        }))
        out = assess_context_completeness(metrics, "BTC", data_dir=data_dir)
        assert out["sources"]["liqmap"] is True
        # 数据源越少，score 越低
        assert 0.0 < out["score"] <= 1.0


def test_stale_or_synthetic_context_is_not_counted_or_fused():
    with tempfile.TemporaryDirectory() as tmp:
        metrics = Path(tmp) / "coinglass_metrics"
        metrics.mkdir()
        (metrics / "BTC_funding_rate.json").write_text(json.dumps({
            "status": "ok", "ts": 1, "data": [{"value": 0.01}]
        }))
        (metrics / "BTC_open_interest.json").write_text(json.dumps({
            "status": "ok", "ts": 3999, "synthetic": True,
            "data": {"openInterest": 100, "h24OIChangePercent": 50}
        }))
        completeness = assess_context_completeness(
            metrics, "BTC", data_dir=Path(tmp), now_epoch=4000
        )
        assert completeness["sources"]["funding_rate"] is False
        assert "funding_rate" in completeness["stale"]
        assert completeness["sources"]["open_interest"] is False
        bias = compute_market_bias(
            {"funding_rate": 0.01, "open_interest_change": 0.5}, completeness
        )
        assert bias["components"]["funding_oi_volume"] == 0.0


def test_news_context_score_propagates_to_snapshot():
    """news_context.json 中的 score 应进入 snapshot 的对应特征。"""
    with tempfile.TemporaryDirectory() as tmp:
        metrics = Path(tmp) / "coinglass_metrics"
        metrics.mkdir(parents=True)
        (metrics / "news_context.json").write_text(json.dumps({
            "scores": {
                "news_context_score": 0.5,
                "financial_calendar_score": 0.2,
                "whale_alert_score": -0.3,
                "fear_greed_score": 0.4,
                "macro_event_importance": 0.7,
                "whale_net_flow_score": -0.3,
            }
        }))
        snap = build_market_feature_snapshot(metrics_dir=metrics, base="BTC")
        assert snap["news_context_score"] == 0.5
        assert snap["macro_event_importance"] == 0.7
        assert snap["fear_greed_score"] == 0.4
        assert snap["whale_alert_score"] == -0.3


def test_repair_mojibake_value_user_samples():
    from core.market_context import repair_mojibake_value

    payload = {
        "anchors": [
            "æä»éä¸é3.02%",
            "èµéè´¹çå éè½¬è´",
        ],
        "risk_flags": [
            "å®è§äºä»¶éè¦æ§æé«",
            "ç¼ºå¤±ä»æ¥æ°æ®",
        ],
    }
    repaired = repair_mojibake_value(payload)
    assert repaired["anchors"][0] == "持仓量下降3.02%"
    assert repaired["anchors"][1] == "资金费率加速转负"
    assert repaired["risk_flags"][0] == "宏观事件重要性极高"
    assert repaired["risk_flags"][1] == "缺失今日数据"
