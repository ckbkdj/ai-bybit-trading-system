"""Point-in-time execution-aware research labels."""

from .triple_barrier import (
    EntrySpec,
    MarketBar,
    TripleBarrierConfig,
    TripleBarrierLabel,
    build_triple_barrier_label,
)

__all__ = [
    "EntrySpec",
    "MarketBar",
    "TripleBarrierConfig",
    "TripleBarrierLabel",
    "build_triple_barrier_label",
]
