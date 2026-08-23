from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Sequence


@dataclass(frozen=True)
class Split:
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]


@dataclass(frozen=True)
class HoldoutBoundary:
    """Chronological holdout with a gap large enough to remove label/window overlap."""

    train_start: int
    train_end: int
    validation_start: int
    validation_end: int
    purge_size: int

    @property
    def train_size(self) -> int:
        return self.train_end - self.train_start

    @property
    def validation_size(self) -> int:
        return self.validation_end - self.validation_start


@dataclass(frozen=True)
class ThreeWayBoundary:
    train_start: int
    train_end: int
    validation_start: int
    validation_end: int
    test_start: int
    test_end: int
    purge_size: int

    @property
    def train_size(self) -> int:
        return self.train_end - self.train_start

    @property
    def validation_size(self) -> int:
        return self.validation_end - self.validation_start

    @property
    def test_size(self) -> int:
        return self.test_end - self.test_start


def purged_holdout_boundary(
    sample_count: int,
    *,
    validation_fraction: float = 0.2,
    minimum_train_size: int = 1,
    minimum_validation_size: int = 1,
    purge_size: int = 0,
) -> HoldoutBoundary:
    """Create a strict chronological train/validation boundary.

    ``train_end`` and ``validation_start`` are exclusive/inclusive Python slice
    boundaries. Rows in between are never used for fitting or scoring.
    """

    if sample_count <= 0 or not 0 < validation_fraction < 1:
        raise ValueError("sample_count and validation_fraction are invalid")
    if minimum_train_size <= 0 or minimum_validation_size <= 0 or purge_size < 0:
        raise ValueError("minimum sizes must be positive and purge_size non-negative")
    validation_size = max(minimum_validation_size, int(round(sample_count * validation_fraction)))
    validation_size = min(validation_size, sample_count - minimum_train_size - purge_size)
    if validation_size < minimum_validation_size:
        raise ValueError("not enough samples for requested purged holdout")
    validation_start = sample_count - validation_size
    train_end = validation_start - purge_size
    if train_end < minimum_train_size:
        raise ValueError("not enough training samples before purge gap")
    return HoldoutBoundary(0, train_end, validation_start, sample_count, purge_size)


def purged_three_way_boundary(
    sample_count: int,
    *,
    validation_fraction: float = 0.15,
    test_fraction: float = 0.15,
    minimum_train_size: int = 1,
    minimum_validation_size: int = 1,
    minimum_test_size: int = 1,
    purge_size: int = 0,
) -> ThreeWayBoundary:
    """Chronological train/validation/test slices separated by two purge gaps."""

    if sample_count <= 0 or validation_fraction <= 0 or test_fraction <= 0:
        raise ValueError("sample count and holdout fractions must be positive")
    if validation_fraction + test_fraction >= 1:
        raise ValueError("validation and test fractions must leave training history")
    if min(minimum_train_size, minimum_validation_size, minimum_test_size) <= 0:
        raise ValueError("minimum split sizes must be positive")
    if purge_size < 0:
        raise ValueError("purge_size must be non-negative")
    validation_size = max(
        minimum_validation_size, int(round(sample_count * validation_fraction))
    )
    test_size = max(minimum_test_size, int(round(sample_count * test_fraction)))
    test_start = sample_count - test_size
    validation_end = test_start - purge_size
    validation_start = validation_end - validation_size
    train_end = validation_start - purge_size
    if train_end < minimum_train_size or validation_start < 0:
        raise ValueError("not enough samples for requested purged three-way split")
    return ThreeWayBoundary(
        0,
        train_end,
        validation_start,
        validation_end,
        test_start,
        sample_count,
        purge_size,
    )


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
