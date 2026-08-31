# Independent full-data verification

Runtime: Python virtual environment with pandas 2.3.2 and pyarrow 21.0.0.
This is an independent semantic oracle, not a claim that Hadoop pseudo-distributed mode ran on Windows.

## Task 1-1

- Output rows: 3,696
- Independently recomputed rows: 3,696
- First five key/value columns identical: True
- First-five mismatch counts: {'state': 0, 'window_date': 0, 'window_days': 0, 'winning_size': 0, 'frequency': 0}
- Maximum absolute population-variance delta: 2.91038304567e-11
- Last output date: 2022-07-09

## Task 1-2

- Selected interpretation: global qualifying style.
- Global qualifying style count: 1,099
- Output state-month rows: 143
- Local-scope comparison rows: 128
- Groups whose median or qualifying-style count differs: 137
- Maharashtra 2022-04 global: median=3.0, styles=863
- Maharashtra 2022-04 local: median=4.0, styles=621

## Task 2-1

- Cancelled+Standard denominator rows: 6,906
- Output state-city groups: 1,442
- Qualifying rows: 0
- Maximum percentage: 0.000000%
- All statuses containing Cancelled: 18,332
- Cancelled rows with at least one promotion token: 295

## Task 2-2

- Valid rows: 128,975
- SKU-month groups: 16,486
- Maximum group rows: 426
- Groups above 1,000 rows: 0
- Output rows: 65,944
- Approximate/exact threshold differences: 0

## Comparison with instructor slide baselines

- Task 1-1: matches 3,696 rows and final date 2022-07-09.
- Task 1-2: the slide's local-scope quick check matches 128 rows and Maharashtra April median 4 over 647 styles; the selected global interpretation produces 143 rows and the global values shown above.
- Task 2-1: zero percent and 18,332 all-Cancelled rows match; this CSV produces 6,906 Cancelled+Standard rows and 1,442 state-city groups, versus slide baselines 6,909 and 1,435.
- Task 2-2: matches 16,486 groups, maximum 426 rows, and zero groups above 1,000.
