from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    from .common import SaleRow
except ImportError:  # pragma: no cover
    from common import SaleRow

TASK22_HEADER = [
    "sku",
    "month",
    "method",
    "percentile_level",
    "threshold",
    "qualifying_order_count",
    "amount_value_count",
    "amount_stddev_pop",
]

DEFAULT_ACCURACY = 10000


@dataclass(frozen=True)
class BaseRow:
    record_id: int
    promotion_count: int
    amount: float | None


@dataclass(frozen=True)
class ThresholdRow:
    sku: str
    month: str
    method: str
    percentile_level: str
    threshold: float


def _ordered_base_rows(rows: Sequence[BaseRow], accuracy: int) -> List[BaseRow]:
    ordered = sorted(rows, key=lambda row: (row.promotion_count, row.record_id))
    if len(ordered) <= accuracy:
        return ordered
    if accuracy <= 1:
        return [ordered[len(ordered) // 2]]

    sample: List[BaseRow] = []
    span = len(ordered) - 1
    denominator = accuracy - 1
    for index in range(accuracy):
        position = int(math.floor((index * span) / denominator + 0.5))
        sample.append(ordered[position])
    return sample


def _nearest_rank(values: Sequence[BaseRow], percentile: float) -> float:
    index = max(0, math.ceil(len(values) * percentile) - 1)
    if index >= len(values):
        index = len(values) - 1
    return float(values[index].promotion_count)


def _thresholds_for_group(rows: Sequence[BaseRow], method: str, accuracy: int | None = None) -> List[ThresholdRow]:
    ordered = sorted(rows, key=lambda row: (row.promotion_count, row.record_id))
    if accuracy is not None:
        ordered = _ordered_base_rows(ordered, accuracy)

    return [
        ThresholdRow(
            sku="",
            month="",
            method=method,
            percentile_level="P80",
            threshold=_nearest_rank(ordered, 0.8),
        ),
        ThresholdRow(
            sku="",
            month="",
            method=method,
            percentile_level="P90",
            threshold=_nearest_rank(ordered, 0.9),
        ),
    ]


def _build_thresholds(groups: Dict[Tuple[str, str], List[BaseRow]], method: str, accuracy: int | None = None) -> List[ThresholdRow]:
    thresholds: List[ThresholdRow] = []
    for (sku, month), rows in groups.items():
        base_thresholds = _thresholds_for_group(rows, method=method, accuracy=accuracy)
        for threshold in base_thresholds:
            thresholds.append(
                ThresholdRow(
                    sku=sku,
                    month=month,
                    method=method,
                    percentile_level=threshold.percentile_level,
                    threshold=threshold.threshold,
                )
            )
    return thresholds


def _population_stddev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.pstdev(values)


def build_task22(rows: Iterable[SaleRow], accuracy: int = DEFAULT_ACCURACY) -> List[Tuple[object, ...]]:
    base_groups: Dict[Tuple[str, str], List[BaseRow]] = defaultdict(list)
    for row in rows:
        if row.sku is None:
            continue
        base_groups[(row.sku, row.month)].append(
            BaseRow(
                record_id=row.record_id,
                promotion_count=len(row.promotion_ids),
                amount=row.amount,
            )
        )

    exact_thresholds = _build_thresholds(base_groups, method="exact")
    approximate_thresholds = _build_thresholds(base_groups, method="approx", accuracy=accuracy)
    thresholds = exact_thresholds + approximate_thresholds

    thresholds_by_group: Dict[Tuple[str, str], List[ThresholdRow]] = defaultdict(list)
    for threshold in thresholds:
        thresholds_by_group[(threshold.sku, threshold.month)].append(threshold)

    output_rows: List[Tuple[object, ...]] = []
    for (sku, month), rows_for_group in base_groups.items():
        for threshold in thresholds_by_group.get((sku, month), []):
            qualifying = [row for row in rows_for_group if row.promotion_count >= threshold.threshold]
            amount_values = [row.amount for row in qualifying if row.amount is not None]
            stddev = 0.0 if len(qualifying) < 2 or not amount_values else _population_stddev(amount_values)
            output_rows.append(
                (
                    sku,
                    month,
                    threshold.method,
                    threshold.percentile_level,
                    threshold.threshold,
                    len(qualifying),
                    len(amount_values),
                    stddev,
                )
            )

    output_rows.sort(key=lambda row: (row[0], row[1], row[2], row[3]))
    return output_rows

