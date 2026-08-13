from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

try:
    from .common import SaleRow
except ImportError:  # pragma: no cover
    from common import SaleRow

TASK21_HEADER = [
    "state",
    "city",
    "cancelled_standard_orders",
    "qualifying_orders",
    "percentage",
]


def build_task21(rows: Iterable[SaleRow]) -> List[Tuple[object, ...]]:
    rows = list(rows)

    promotion_dates: Dict[str, List] = defaultdict(list)
    for row in rows:
        for promotion_id in row.promotion_ids:
            promotion_dates[promotion_id].append(row.order_date)

    valid_promotions = {
        promotion_id
        for promotion_id, dates in promotion_dates.items()
        if (max(dates) - min(dates)).days >= 2
    }

    valid_counts = {
        row.record_id: sum(1 for promotion_id in row.promotion_ids if promotion_id in valid_promotions)
        for row in rows
    }

    state_totals: Dict[str, List[float]] = defaultdict(lambda: [0.0, 0.0])
    for row in rows:
        if row.state is None or row.amount is None:
            continue
        if row.fulfilment != "MERCHANT" or row.courier_status != "SHIPPED":
            continue
        state_totals[row.state][0] += row.amount
        state_totals[row.state][1] += 1.0

    state_average = {
        state: total / count
        for state, (total, count) in state_totals.items()
        if count > 0
    }

    groups: Dict[Tuple[str, str], List[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        if row.status != "CANCELLED" or row.service_level != "STANDARD":
            continue
        if row.state is None or row.city is None:
            continue
        qualifying = (
            valid_counts.get(row.record_id, 0) >= 3
            and row.amount is not None
            and state_average.get(row.state) is not None
            and row.amount < state_average[row.state]
        )
        bucket = groups[(row.state, row.city)]
        bucket[0] += 1
        if qualifying:
            bucket[1] += 1

    output_rows: List[Tuple[object, ...]] = []
    for (state, city), (cancelled_count, qualifying_count) in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1])):
        percentage = 100.0 * float(qualifying_count) / float(cancelled_count)
        output_rows.append((state, city, cancelled_count, qualifying_count, percentage))

    return output_rows

