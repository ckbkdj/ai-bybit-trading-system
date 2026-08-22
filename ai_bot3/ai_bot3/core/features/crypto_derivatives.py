from __future__ import annotations


def derivatives_features(
    *,
    open_interest_now: float,
    open_interest_previous: float,
    funding_rate: float,
    mark_price: float,
    index_price: float,
    long_liquidation_usdt: float,
    short_liquidation_usdt: float,
) -> dict[str, float]:
    if open_interest_now < 0 or open_interest_previous < 0 or mark_price <= 0 or index_price <= 0:
        raise ValueError("invalid derivative inputs")
    oi_change = (
        (open_interest_now - open_interest_previous) / open_interest_previous
        if open_interest_previous > 0
        else 0.0
    )
    liquidation_total = max(0.0, long_liquidation_usdt) + max(0.0, short_liquidation_usdt)
    return {
        "open_interest_change_1h": oi_change,
        "funding_rate": funding_rate,
        "mark_index_basis_bps": (mark_price - index_price) / index_price * 10_000,
        "liquidation_imbalance": (
            (short_liquidation_usdt - long_liquidation_usdt) / liquidation_total
            if liquidation_total else 0.0
        ),
    }
