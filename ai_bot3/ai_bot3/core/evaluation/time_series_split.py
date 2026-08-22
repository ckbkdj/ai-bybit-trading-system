from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Sequence


@dataclass(frozen=True)
class Split:
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]


class PurgedWalkForwardSplit:
    def __init__(
        self,
        *,
        train_size: int,
        test_size: int,
        purge_size: int = 0,
        embargo_size: int = 0,
        expanding: bool = False,
    ):
        if train_size <= 0 or test_size <= 0 or purge_size < 0 or embargo_size < 0:
            raise ValueError("invalid split sizes")
        self.train_size = train_size
        self.test_size = test_size
        self.purge_size = purge_size
        self.embargo_size = embargo_size
        self.expanding = expanding

    def split(self, samples: Sequence[object]) -> Iterator[Split]:
        total = len(samples)
        test_start = self.train_size + self.purge_size
        while test_start + self.test_size <= total:
            train_end = test_start - self.purge_size
            train_start = 0 if self.expanding else max(0, train_end - self.train_size)
            yield Split(
                tuple(range(train_start, train_end)),
                tuple(range(test_start, test_start + self.test_size)),
            )
            test_start += self.test_size + self.embargo_size
