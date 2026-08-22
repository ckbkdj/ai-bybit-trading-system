from __future__ import annotations

from typing import Iterable


def normalize_scenarios(scenarios: Iterable[dict]) -> list[dict]:
    items = [dict(item) for item in scenarios]
    if not items or any(float(item.get("probability", -1)) < 0 for item in items):
        raise ValueError("scenarios require non-negative probabilities")
    total = sum(float(item["probability"]) for item in items)
    if total <= 0:
        raise ValueError("scenario probability total must be positive")
    for item in items:
        item["probability"] = float(item["probability"]) / total
    return items
