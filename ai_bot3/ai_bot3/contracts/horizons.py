from __future__ import annotations

from types import MappingProxyType
from typing import Mapping, Sequence


MODE_HORIZONS: Mapping[str, int] = MappingProxyType(
    {
        "scalping": 180,
        "mid_short": 900,
        "trend": 7200,
        "trend_swing": 14400,
        "swing": 86400,
    }
)
HORIZONS_SEC: tuple[int, ...] = tuple(MODE_HORIZONS.values())
HORIZON_TIMEFRAME: Mapping[int, str] = MappingProxyType(
    {
        180: "3m",
        900: "15m",
        7200: "2h",
        14400: "4h",
        86400: "1d",
    }
)
MAX_CANDIDATE_KLINE_AGE_SEC: Mapping[int, int] = MappingProxyType(
    {
        180: 10 * 60,
        900: 45 * 60,
        7200: 4 * 60 * 60,
        14400: 8 * 60 * 60,
        86400: 36 * 60 * 60,
    }
)


def horizon_for_mode(mode: str) -> int:
    try:
        return MODE_HORIZONS[str(mode)]
    except KeyError as exc:
        raise ValueError(f"unsupported prediction mode: {mode}") from exc


__all__: Sequence[str] = (
    "HORIZONS_SEC",
    "HORIZON_TIMEFRAME",
    "MAX_CANDIDATE_KLINE_AGE_SEC",
    "MODE_HORIZONS",
    "horizon_for_mode",
)
