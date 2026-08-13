from __future__ import annotations

import argparse
from pathlib import Path
from typing import Tuple

try:
    from .common import load_rows, write_csv
    from .task11 import TASK11_HEADER, build_task11
    from .task12 import TASK12_HEADER, build_task12
    from .task21 import TASK21_HEADER, build_task21
    from .task22 import DEFAULT_ACCURACY, TASK22_HEADER, build_task22
except ImportError:  # pragma: no cover
    from common import load_rows, write_csv
    from task11 import TASK11_HEADER, build_task11
    from task12 import TASK12_HEADER, build_task12
    from task21 import TASK21_HEADER, build_task21
    from task22 import DEFAULT_ACCURACY, TASK22_HEADER, build_task22


def _default_input_path() -> Path:
    return Path(__file__).resolve().parents[2] / "Amazon Sale Report.csv"


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parent / "output"


def _resolve_input_path(path: Path) -> Path:
    if path.is_absolute() and path.exists():
        return path

    candidates = [
        Path.cwd() / path,
        Path(__file__).resolve().parent / path,
        Path(__file__).resolve().parent.parent / path,
        Path(__file__).resolve().parents[2] / path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return path


def _write_outputs(output_dir: Path, outputs: Tuple[Tuple[str, list], ...]) -> None:
    for file_name, header, rows in outputs:
        write_csv(output_dir / file_name, header, rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sinh 4 file CSV doi chieu cho Lab 3.")
    parser.add_argument("--input", type=Path, default=_default_input_path(), help="Duong dan CSV dau vao")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_default_output_dir(),
        help="Thu muc se chua 4 file CSV ket qua",
    )
    parser.add_argument(
        "--accuracy",
        type=int,
        default=DEFAULT_ACCURACY,
        help="Do chinh xac cho Task 2-2 phan approx",
    )
    args = parser.parse_args()

    input_path = _resolve_input_path(args.input)
    rows = load_rows(input_path)
    task11_rows = build_task11(rows)
    task12_rows = build_task12(rows)
    task21_rows = build_task21(rows)
    task22_rows = build_task22(rows, accuracy=args.accuracy)

    outputs = (
        ("Task_1-1.csv", TASK11_HEADER, task11_rows),
        ("Task_1-2.csv", TASK12_HEADER, task12_rows),
        ("Task_2-1.csv", TASK21_HEADER, task21_rows),
        ("Task_2-2.csv", TASK22_HEADER, task22_rows),
    )
    _write_outputs(args.output_dir, outputs)

    print(f"Da tao {len(outputs)} file CSV tai: {args.output_dir}")
    for file_name, _, _ in outputs:
        print(f"- {args.output_dir / file_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
