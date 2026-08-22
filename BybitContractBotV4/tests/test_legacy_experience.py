from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from legacy_experience import (
    SCALE_IN,
    entry_wait_seconds,
    locked_leveraged_return_pct,
    monotonic_stop,
    staged_exits,
    stop_price_from_leveraged_return,
)


class LegacyExperienceSpecificationTests(unittest.TestCase):
    def test_v41_entry_waits_are_preserved_exactly(self):
        self.assertEqual(entry_wait_seconds("3m"), 348)
        self.assertEqual(entry_wait_seconds("5m"), 588)
        self.assertEqual(entry_wait_seconds("15m"), 1788)
        with self.assertRaisesRegex(ValueError, "no evidenced timeout"):
            entry_wait_seconds("2h")

    def test_profit_lock_thresholds_and_price_conversion_are_symmetric(self):
        locked = locked_leveraged_return_pct(61, 100)
        self.assertEqual(locked, Decimal("51"))
        self.assertEqual(
            stop_price_from_leveraged_return(
                side="BUY",
                average_entry_price=100,
                leverage=100,
                locked_return_pct=locked,
            ),
            Decimal("100.51"),
        )
        self.assertEqual(
            stop_price_from_leveraged_return(
                side="SELL",
                average_entry_price=100,
                leverage=100,
                locked_return_pct=locked,
            ),
            Decimal("99.49"),
        )

    def test_stop_can_tighten_but_never_move_backwards(self):
        self.assertEqual(monotonic_stop(side="BUY", existing=100, candidate=101), Decimal("101"))
        self.assertEqual(monotonic_stop(side="BUY", existing=101, candidate=100), Decimal("101"))
        self.assertEqual(monotonic_stop(side="SELL", existing=100, candidate=99), Decimal("99"))
        self.assertEqual(monotonic_stop(side="SELL", existing=99, candidate=100), Decimal("99"))

    def test_staged_exit_ladder_preserves_runner_and_add_position_variant(self):
        runner = staged_exits(wide_volatility_band=True, entry_count=1)
        self.assertEqual([item.leveraged_return_pct for item in runner], [41, 58, 80, 100])
        self.assertLess(sum(item.close_fraction for item in runner), Decimal("1"))
        scaled = staged_exits(wide_volatility_band=True, entry_count=2)
        self.assertEqual(sum(item.close_fraction for item in scaled), Decimal("1.0"))
        self.assertFalse(SCALE_IN.enabled)
        self.assertEqual(SCALE_IN.maximum_entry_count, 3)


if __name__ == "__main__":
    unittest.main()

