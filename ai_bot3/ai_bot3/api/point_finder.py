"""爆仓图点位识别引擎。

数据 schema（Coinglass liqMapV2）::

    {
        "lastPrice": "67000",
        "liqMapV2": {
            "66950":  [[<x>, <heat/notional>, ...], ...],
            "67050":  [[<x>, <heat/notional>, ...], ...],
            ...
        }
    }

约定：
* dict key 是价格档位
* 每条 entry 中 ``entry[1]`` 是该档位 heat / notional
* 多头点位（long）必须低于当前价；空头点位（short）必须高于当前价
* 点位标签全部使用中文

输出包含 19 种形态模型识别（``MODEL_NAME_MAP``），并按 ``priority`` (``near`` /
``far`` / ``balanced``) 与归一化热度、与现价距离、聚类质量等综合打分。
当数据足够时，每边返回 ≥ 3 个点位（狙击位 / 加仓位 / 兜底位 / 兜底1 / 兜底2 等）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


MODEL_NAME_MAP: Dict[str, str] = {
    "single_peak":        "单峰模型",
    "double_peak":        "双峰模型",
    "multi_peak":         "多峰模型",
    "cliff":              "断崖模型",
    "ladder":             "阶梯模型",
    "cliff_ladder":       "断崖+阶梯模型",
    "double_peak_cliff":  "双峰+断崖模型",
    "full_ladder":        "全阶梯模型",
    "fragmented_peak":    "碎峰模型",
    "multi_cliff":        "多断层模型",
    "squeeze_zone":       "夹击区模型",
    "long_trap":          "多头陷阱模型",
    "short_trap":         "空头陷阱模型",
    "balanced_wall":      "对称墙模型",
    "asymmetric_wall":    "非对称墙模型",
    "vacuum_gap":         "真空缺口模型",
    "pin_bar_liq":        "插针清算模型",
    "combined":           "聚合模型",
    "unknown":            "未知模型",
}

DATA_PATH = Path(__file__).resolve().parent.parent / "data"


# ---------------------------------------------------------------------------
# 形态识别
# ---------------------------------------------------------------------------

def _local_peaks(volumes: List[float]) -> List[int]:
    peaks: List[int] = []
    n = len(volumes)
    for i in range(1, n - 1):
        if volumes[i] > 0 and volumes[i] > volumes[i - 1] and volumes[i] > volumes[i + 1]:
            peaks.append(i)
    return peaks


def _cliffs(volumes: List[float]) -> List[int]:
    cliffs: List[int] = []
    for i in range(1, len(volumes) - 1):
        prev = volumes[i - 1]
        cur = volumes[i]
        if prev > 0 and cur < prev * 0.4:
            cliffs.append(i)
    return cliffs


def _ladder_score(volumes: List[float]) -> float:
    if len(volumes) < 4:
        return 0.0
    rises = sum(1 for i in range(1, len(volumes)) if volumes[i] > volumes[i - 1])
    return rises / float(len(volumes) - 1)


def _vacuum_runs(volumes: List[float], threshold: float) -> int:
    run = 0
    longest = 0
    for v in volumes:
        if v <= threshold:
            run += 1
            longest = max(longest, run)
        else:
            run = 0
    return longest


def identify_model(volumes: List[float], long_pressure: float, short_pressure: float) -> str:
    """根据 volumes 序列与多空压力综合识别形态。"""
    if not volumes:
        return "unknown"

    peaks = _local_peaks(volumes)
    cliffs = _cliffs(volumes)
    ladder = _ladder_score(volumes)
    max_v = max(volumes) or 0.0
    vacuum = _vacuum_runs(volumes, threshold=max_v * 0.05) if max_v > 0 else 0

    total = long_pressure + short_pressure
    imbalance = 0.0 if total <= 0 else (short_pressure - long_pressure) / total

    if abs(imbalance) >= 0.7 and len(peaks) >= 1:
        return "asymmetric_wall"
    if abs(imbalance) <= 0.1 and total > 0 and len(peaks) >= 2:
        return "balanced_wall"
    if vacuum >= max(4, len(volumes) // 4) and len(peaks) >= 1:
        return "vacuum_gap"
    if len(peaks) >= 4 and not cliffs:
        return "fragmented_peak"
    if len(cliffs) >= 2 and len(peaks) >= 3:
        return "multi_cliff"
    if len(peaks) == 2 and len(cliffs) >= 1:
        return "double_peak_cliff"
    if len(peaks) >= 1 and len(cliffs) >= 1:
        return "cliff_ladder"
    if ladder >= 0.85:
        return "full_ladder"
    if ladder >= 0.65 and len(peaks) <= 2:
        return "ladder"
    if len(peaks) == 1 and len(cliffs) >= 1:
        return "cliff"
    if len(peaks) == 1:
        return "single_peak"
    if len(peaks) == 2:
        return "double_peak"
    if len(peaks) >= 3:
        return "multi_peak"
    if total > 0 and abs(imbalance) >= 0.4 and len(peaks) >= 1:
        return "squeeze_zone"
    if imbalance >= 0.4:
        return "long_trap"
    if imbalance <= -0.4:
        return "short_trap"
    if max_v > 0 and any(v >= max_v * 0.85 for v in volumes[:2] + volumes[-2:]):
        return "pin_bar_liq"
    if peaks or cliffs:
        return "combined"
    return "unknown"


# ---------------------------------------------------------------------------
# 点位选择
# ---------------------------------------------------------------------------

def _normalize_heat(price_volumes: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    if not price_volumes:
        return []
    max_v = max((v for _, v in price_volumes), default=0.0)
    if max_v <= 0:
        return [(p, 0.0) for p, _ in price_volumes]
    return [(p, v / max_v) for p, v in price_volumes]


def _score(price: float, heat_norm: float, last_price: float, priority: str) -> float:
    """点位综合得分：归一化热度 + 距离因子（near/far/balanced）。"""
    if last_price <= 0:
        return heat_norm
    dist = abs(price - last_price) / last_price
    if priority == "near":
        # 越近越好
        dist_score = max(0.0, 1.0 - min(1.0, dist * 8.0))
    elif priority == "far":
        dist_score = min(1.0, dist * 8.0)
    else:  # balanced
        dist_score = 1.0 - abs(0.5 - min(1.0, dist * 4.0))
    return 0.7 * heat_norm + 0.3 * dist_score


_SIDE_LABELS_LONG = ("多-狙击位", "多-加仓位", "多-兜底位", "多-兜底1", "多-兜底2")
_SIDE_LABELS_SHORT = ("空-狙击位", "空-加仓位", "空-兜底位", "空-兜底1", "空-兜底2")


def _select_side(
    price_volumes: List[Tuple[float, float]],
    last_price: float,
    direction: str,
    threshold: float,
    priority: str,
    target_count: int = 3,
) -> List[Dict[str, Any]]:
    """对单边（long/short）选 ≥ target_count 个点位。"""
    # 强制方向：long 必须低于当前价；short 必须高于当前价
    if direction == "long":
        candidates = [(p, h) for p, h in price_volumes if p < last_price]
    else:
        candidates = [(p, h) for p, h in price_volumes if p > last_price]
    if not candidates:
        return []

    norm = _normalize_heat(candidates)
    norm_map = {p: hn for p, hn in norm}

    # 第一轮按 threshold 过滤
    scored = [
        (p, h, norm_map[p], _score(p, norm_map[p], last_price, priority))
        for (p, h) in candidates if norm_map[p] >= threshold
    ]
    # 不够则降一半阈值再来一遍
    if len(scored) < target_count:
        relaxed = threshold * 0.5
        scored = [
            (p, h, norm_map[p], _score(p, norm_map[p], last_price, priority))
            for (p, h) in candidates if norm_map[p] >= relaxed
        ]
    # 还不够：用全部 candidates 兜底
    if len(scored) < target_count:
        scored = [
            (p, h, norm_map[p], _score(p, norm_map[p], last_price, priority))
            for (p, h) in candidates
        ]

    scored.sort(key=lambda x: x[3], reverse=True)

    # 去重 + 间距控制：保证点位之间至少有 0.2% 价差
    selected: List[Tuple[float, float, float, float]] = []
    min_spacing = max(last_price * 0.002, 1e-6)
    for item in scored:
        p, h, hn, sc = item
        if not selected:
            selected.append(item)
            continue
        if all(abs(p - s[0]) >= min_spacing for s in selected):
            selected.append(item)
        if len(selected) >= max(target_count, 4):
            break

    # 仍不足 target_count：从距离当前价最远 / 最近的位次按 quantile 兜底
    if len(selected) < target_count:
        sorted_candidates = sorted(candidates, key=lambda x: x[0], reverse=(direction == "long"))
        for p, h in sorted_candidates:
            if any(abs(p - s[0]) < min_spacing for s in selected):
                continue
            selected.append((p, h, norm_map.get(p, 0.0), 0.0))
            if len(selected) >= target_count:
                break

    selected = selected[:max(target_count, 5)]

    labels = _SIDE_LABELS_LONG if direction == "long" else _SIDE_LABELS_SHORT
    out: List[Dict[str, Any]] = []
    # 排序：long 由近到远价格降序；short 由近到远价格升序
    if direction == "long":
        selected.sort(key=lambda x: -x[0])
    else:
        selected.sort(key=lambda x: x[0])
    for idx, (price, heat, heat_norm, score) in enumerate(selected):
        label = labels[idx] if idx < len(labels) else f"{labels[0][0]}-补{idx + 1}"
        out.append({
            "price": float(price),
            "label": label,
            "heat": float(heat),
            "heat_norm": float(heat_norm),
            "score": float(score),
        })
    return out


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def find_three_points(
    data: Optional[Dict[str, Any]],
    model: str = "auto",
    threshold: float = 0.38,
    priority: str = "near",
) -> Dict[str, Any]:
    """对外主接口；保持旧签名。"""
    if not data:
        return {"points": {"long": [], "short": []}, "model": "未知模型", "model_key": "unknown"}

    try:
        last_price = float(data.get("lastPrice") or 0.0)
    except Exception:
        last_price = 0.0
    liq_map = data.get("liqMapV2") or {}
    if last_price <= 0 or not isinstance(liq_map, dict) or not liq_map:
        return {"points": {"long": [], "short": []}, "model": "未知模型", "model_key": "unknown"}

    # 聚合每个价格档位的总 heat
    price_volumes: List[Tuple[float, float]] = []
    for key_str, entries in liq_map.items():
        try:
            price = float(key_str)
        except Exception:
            continue
        total = 0.0
        for entry in entries or ():
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                continue
            try:
                # entry[1] 是 heat / notional
                total += float(entry[1])
            except Exception:
                continue
        if total > 0:
            price_volumes.append((price, total))

    price_volumes.sort(key=lambda x: x[0])
    prices_sorted = [p for p, _ in price_volumes]
    volumes_sorted = [v for _, v in price_volumes]

    long_pressure = sum(v for p, v in price_volumes if p < last_price)
    short_pressure = sum(v for p, v in price_volumes if p > last_price)

    model_key = (
        identify_model(volumes_sorted, long_pressure, short_pressure)
        if model in ("auto", "", None)
        else model
    )
    if model_key not in MODEL_NAME_MAP:
        model_key = "unknown"

    long_points = _select_side(price_volumes, last_price, "long", threshold, priority, target_count=3)
    short_points = _select_side(price_volumes, last_price, "short", threshold, priority, target_count=3)

    return {
        "model": MODEL_NAME_MAP[model_key],
        "model_key": model_key,
        "points": {
            "long": long_points,
            "short": short_points,
        },
        "analysis": {
            "last_price": last_price,
            "long_pressure": long_pressure,
            "short_pressure": short_pressure,
            "imbalance": (
                0.0 if (long_pressure + short_pressure) <= 0
                else (short_pressure - long_pressure) / (long_pressure + short_pressure)
            ),
            "level_count": len(price_volumes),
        },
    }


__all__ = ["find_three_points", "identify_model", "MODEL_NAME_MAP"]
