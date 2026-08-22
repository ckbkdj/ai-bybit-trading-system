from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.time_series_split import (
    PurgedWalkForwardSplit,
    purged_holdout_boundary,
)


class PurgedModelValidationTests(unittest.TestCase):
    def test_holdout_never_overlaps_training_or_purge(self):
        boundary = purged_holdout_boundary(
            1000,
            validation_fraction=0.2,
            minimum_train_size=500,
            minimum_validation_size=100,
            purge_size=180,
        )
        self.assertEqual(boundary.train_end, 620)
        self.assertEqual(boundary.validation_start, 800)
        self.assertEqual(boundary.validation_size, 200)
        self.assertEqual(boundary.validation_start - boundary.train_end, 180)

    def test_holdout_fails_closed_when_history_is_too_short(self):
        with self.assertRaises(ValueError):
            purged_holdout_boundary(
                100,
                validation_fraction=0.2,
                minimum_train_size=80,
                minimum_validation_size=20,
                purge_size=10,
            )

    def test_walk_forward_respects_label_gap_and_embargo(self):
        folds = list(
            PurgedWalkForwardSplit(
                train_size=30,
                test_size=10,
                purge_size=5,
                embargo_size=3,
                expanding=True,
            ).split(list(range(100)))
        )
        self.assertGreaterEqual(len(folds), 3)
        for fold in folds:
            self.assertGreaterEqual(min(fold.test_indices) - max(fold.train_indices) - 1, 5)
        for previous, current in zip(folds, folds[1:]):
            self.assertGreaterEqual(min(current.test_indices) - max(previous.test_indices) - 1, 3)


if __name__ == "__main__":
    unittest.main()
