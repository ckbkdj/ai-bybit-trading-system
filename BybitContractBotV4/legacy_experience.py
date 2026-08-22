"""Readable, side-effect-free rules recovered from the two-year v4.1 runtime.

Nothing in this module places or changes an order.  It is the compatibility
specification used by tests, shadow evaluation and the migration report.  A rule is
activated in live execution only after its own cost-aware validation and release
gate; preserving experience does not mean preserving an unsafe implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


Side = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class LegacyRuleProvenance:
    source_version: str = "bot_threshold_super_v4_1.py@3f36b65"
    entry_timeout_function: str = "fetch_open_order"
    protection_function: str = "lock_profits"
    activation: str = "shadow_only_until_validated"


@dataclass(frozen=True)
class LegacyScaleInPolicy:
    """Recovered limits; signal predicates remain disabled pending replay."""

    initial_equity_fraction: Decimal = Decimal("0.006")
    maximum_entry_count: int = 3
    maximum_profitable_adds: int = 2
    enabled: bool = False
    disabled_reason: str = (
        "v4.1 temporarily removed position TP/SL after adding and used mutable INI counters"
    )


@dataclass(frozen=True)
class StagedExit:
    leveraged_return_pct: Decimal
    close_fraction: Decimal


def entry_wait_seconds(timeframe: str) -> int:
    """v4.1 waited two candles minus 12 seconds before cancelling an entry."""

    candle_seconds = {"3m": 180, "5m": 300, "15m": 900}
    try:
        return candle_seconds[timeframe] * 2 - 12
    except KeyError as exc:
        raise ValueError(f"v4.1 has no evidenced timeout for timeframe {timeframe}") from exc


def locked_leveraged_return_pct(
    current_leveraged_return_pct: Decimal | float,
    leverage: Decimal | float,
) -> Decimal | None:
    """Return the v4.1 profit floor, expressed in leveraged return percent."""

    current = Decimal(str(current_leveraged_return_pct))
    lev = Decimal(str(leverage))
    if lev <= 0:
        raise ValueError("leverage must be positive")
    scaled = lev / Decimal("100")
    if current > Decimal("120") * scaled:
        return current - Decimal("15")
    if current > Decimal("100") * scaled:
        return current - Decimal("10")
    if current > Decimal("80") * scaled:
        return current - Decimal("15")
    if current > Decimal("60") * scaled:
        return current - Decimal("10") * scaled
    if current > Decimal("50") * scaled:
        return current - Decimal("8") * scaled
    if current > Decimal("40") * scaled:
        return Decimal(int(Decimal("14") * scaled))
    return None


def stop_price_from_leveraged_return(
    *,
    side: Side,
    average_entry_price: Decimal | float,
    leverage: Decimal | float,
    locked_return_pct: Decimal | float,
) -> Decimal:
    """Convert a leveraged return floor into the equivalent unleveraged stop price."""

    entry = Decimal(str(average_entry_price))
    lev = Decimal(str(leverage))
    locked = Decimal(str(locked_return_pct))
    if entry <= 0 or lev <= 0:
        raise ValueError("entry price and leverage must be positive")
    move = locked / (Decimal("100") * lev)
    if side == "BUY":
        return entry * (Decimal("1") + move)
    if side == "SELL":
        return entry * (Decimal("1") - move)
    raise ValueError("side must be BUY or SELL")


def monotonic_stop(*, side: Side, existing: Decimal | float | None, candidate: Decimal | float) -> Decimal:
    """Preserve v4.1's most valuable invariant: protection can only tighten."""

    new = Decimal(str(candidate))
    if existing in (None, "", 0, "0"):
        return new
    old = Decimal(str(existing))
    if side == "BUY":
        return max(old, new)
    if side == "SELL":
        return min(old, new)
    raise ValueError("side must be BUY or SELL")


def staged_exits(*, wide_volatility_band: bool, entry_count: int) -> tuple[StagedExit, ...]:
    """Recovered v4.1 exit ladder, including the intentional residual runner."""

    if not wide_volatility_band:
        return (StagedExit(Decimal("32"), Decimal("1")),)
    if entry_count >= 2:
        return (
            StagedExit(Decimal("41"), Decimal("0.8")),
            StagedExit(Decimal("58"), Decimal("0.2")),
        )
    return (
        StagedExit(Decimal("41"), Decimal(1) / Decimal(6)),
        StagedExit(Decimal("58"), Decimal(1) / Decimal(5)),
        StagedExit(Decimal("80"), Decimal(1) / Decimal(5)),
        StagedExit(Decimal("100"), Decimal(1) / Decimal(4)),
    )


PROVENANCE = LegacyRuleProvenance()
SCALE_IN = LegacyScaleInPolicy()

