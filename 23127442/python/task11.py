from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date as Date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from .common import SaleRow, is_bought, is_at_least_xxl, normalize_dimension
except ImportError:  # pragma: no cover
    from common import SaleRow, is_bought, is_at_least_xxl, normalize_dimension

TASK11_HEADER = [
    "state",
    "window_date",
    "window_days",
    "winning_size",
    "frequency",
    "population_variance",
]


@dataclass
class Moment:
    count: int = 0
    amount_count: int = 0
    total: float = 0.0
    total_squares: float = 0.0

    def add(self, amount: Optional[float]) -> None:
        self.count += 1
        if amount is None:
            return
        self.amount_count += 1
        self.total += amount
        self.total_squares += amount * amount

    def population_variance(self) -> Optional[float]:
        if self.amount_count == 0:
            return None
        mean = self.total / float(self.amount_count)
        raw = self.total_squares / float(self.amount_count) - mean * mean
        if raw < 0.0 and raw > -1e-9:
            return 0.0
        return raw


@dataclass
class SizeCandidate:
    size: str
    moment: Moment


def _window_days(total_bought_rows: int) -> int:
    return 5 if total_bought_rows > 10000 else 10


def _bucket_dates(purchase_date: Date, days: int) -> List[Date]:
    return [purchase_date + timedelta(days=offset) for offset in range(1, days + 1)]


def _winner_key(candidate: SizeCandidate) -> Tuple[int, int, float, str]:
    variance = candidate.moment.population_variance()
    variance_rank = 1 if variance is None else 0
    variance_value = 0.0 if variance is None else variance
    return (-candidate.moment.count, variance_rank, variance_value, candidate.size)


def build_task11(rows: Iterable[SaleRow]) -> List[Tuple[object, ...]]:
    rows = list(rows)
    bought_rows = [row for row in rows if is_bought(row.status, row.qty) and normalize_dimension(row.state)]
    state_windows = Counter(row.state for row in bought_rows if row.state is not None)
    state_to_days = {state: _window_days(count) for state, count in state_windows.items()}

    bucket_moments: Dict[Tuple[str, Date, str], Moment] = defaultdict(Moment)
    for row in bought_rows:
        state = row.state
        size = row.size
        days = state_to_days.get(state or "")
        if state is None or size is None or days is None:
            continue
        for bucket_date in _bucket_dates(row.order_date, days):
            bucket_moments[(state, bucket_date, size)].add(row.amount)

    window_candidates: Dict[Tuple[str, Date], List[SizeCandidate]] = defaultdict(list)
    for (state, bucket_date, size), moment in bucket_moments.items():
        window_candidates[(state, bucket_date)].append(SizeCandidate(size=size, moment=moment))

    output_rows: List[Tuple[object, ...]] = []
    for (state, bucket_date), candidates in sorted(window_candidates.items(), key=lambda item: (item[0][0], item[0][1])):
        winner = min(candidates, key=_winner_key)
        variance = winner.moment.population_variance()
        output_rows.append(
            (
                state,
                bucket_date.isoformat(),
                state_to_days[state],
                winner.size,
                winner.moment.count,
                "" if variance is None else variance,
            )
        )

    return output_rows

