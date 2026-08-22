"""point_finder.py 综合测试。"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from point_finder import find_three_points, MODEL_NAME_MAP


def _synthesize_liqmap(last_price: float = 100.0):
    """构造一个真实 liqMapV2 schema 的合成数据：

    - dict key 是价格档位（字符串）
    - entry[1] 是 heat / notional
    """
    liq_map = {}
    # 多侧（低于现价）：放 5 个不同热度的价位
    for i, (delta, heat) in enumerate([(0.5, 30), (1.5, 40), (2.5, 80), (3.5, 25), (4.5, 60)]):
        p = last_price - delta
        liq_map[str(p)] = [[i, heat, 0]]
    # 空侧（高于现价）：放 5 个不同热度的价位
    for i, (delta, heat) in enumerate([(0.5, 35), (1.5, 50), (2.5, 70), (3.5, 30), (4.5, 90)]):
        p = last_price + delta
        liq_map[str(p)] = [[i + 100, heat, 0]]
    return {"lastPrice": str(last_price), "liqMapV2": liq_map}


def test_three_per_side_and_correct_sides():
    data = _synthesize_liqmap(100.0)
    out = find_three_points(data, model="auto", threshold=0.20, priority="near")
    long_pts = out["points"]["long"]
    short_pts = out["points"]["short"]
    assert len(long_pts) >= 3, f"long 应至少返回 3 个，实际 {len(long_pts)}"
    assert len(short_pts) >= 3, f"short 应至少返回 3 个，实际 {len(short_pts)}"
    for p in long_pts:
        assert p["price"] < 100.0, f"多头点位应低于现价: {p}"
    for p in short_pts:
        assert p["price"] > 100.0, f"空头点位应高于现价: {p}"
    # 标签为中文
    assert any("多" in p["label"] for p in long_pts)
    assert any("空" in p["label"] for p in short_pts)
    # 模型识别为已知
    assert out["model_key"] in MODEL_NAME_MAP


def test_empty_payload_safe():
    assert find_three_points(None)["points"]["long"] == []
    assert find_three_points({})["points"]["long"] == []
