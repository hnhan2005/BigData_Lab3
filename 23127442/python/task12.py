from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

try:
    from .common import SaleRow, is_at_least_xxl, is_bought, normalize_dimension
except ImportError:  # pragma: no cover
    from common import SaleRow, is_at_least_xxl, is_bought, normalize_dimension

TASK12_HEADER = [
    "state",
    "month",
    "median_variety",
    "qualifying_style_count",
]


def _median(values: List[int]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return float(ordered[middle])
    return ordered[middle - 1] / 2.0 + ordered[middle] / 2.0


def build_task12(rows: Iterable[SaleRow]) -> List[Tuple[object, ...]]:
    groups: Dict[Tuple[str, str, str], Dict[str, object]] = defaultdict(lambda: {"skus": set(), "has_xxl": False})

    for row in rows:
        if not is_bought(row.status, row.qty):
            continue
        state = normalize_dimension(row.state)
        style = normalize_dimension(row.style)
        sku = normalize_dimension(row.sku)
        if state is None or style is None or sku is None:
            continue
        bucket = groups[(state, row.month, style)]
        bucket["skus"].add(sku)
        if row.size is not None and is_at_least_xxl(row.size):
            bucket["has_xxl"] = True

    state_month_values: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    for (state, month, _style), payload in groups.items():
        if payload["has_xxl"]:
            state_month_values[(state, month)].append(len(payload["skus"]))

    output_rows: List[Tuple[object, ...]] = []
    for (state, month), values in sorted(state_month_values.items(), key=lambda item: (item[0][0], item[0][1])):
        output_rows.append((state, month, _median(values), len(values)))

    return output_rows

