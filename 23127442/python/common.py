from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass
from datetime import date as Date
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

HEADER = [
    "index",
    "Order ID",
    "Date",
    "Status",
    "Fulfilment",
    "Sales Channel ",
    "ship-service-level",
    "Style",
    "SKU",
    "Category",
    "Size",
    "ASIN",
    "Courier Status",
    "Qty",
    "currency",
    "Amount",
    "ship-city",
    "ship-state",
    "ship-postal-code",
    "ship-country",
    "promotion-ids",
    "B2B",
    "fulfilled-by",
    "Unnamed: 22",
]

_WHITESPACE_RE = re.compile(r"\s+")
_NUMERIC_XL_RE = re.compile(r"^([2-9]|[1-9][0-9]+)XL$")
_REPEATED_XL_RE = re.compile(r"^X{2,}L$")


@dataclass(frozen=True)
class SaleRow:
    record_id: int
    order_id: str
    order_date: Date
    status: str
    fulfilment: str
    service_level: str
    style: Optional[str]
    sku: Optional[str]
    size: Optional[str]
    courier_status: Optional[str]
    qty: int
    amount: Optional[float]
    city: Optional[str]
    state: Optional[str]
    promotion_ids: Tuple[str, ...]

    @property
    def month(self) -> str:
        return f"{self.order_date.year:04d}-{self.order_date.month:02d}"


def normalize_dimension(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = _WHITESPACE_RE.sub(" ", value.strip()).upper()
    return cleaned or None


def is_bought(status: Optional[str], qty: int) -> bool:
    normalized = normalize_dimension(status)
    return qty != 0 and normalized is not None and "SHIPPED" in normalized


def is_at_least_xxl(size: Optional[str]) -> bool:
    normalized = normalize_dimension(size)
    if normalized is None:
        return False
    compact = normalized.replace(" ", "")
    return bool(_NUMERIC_XL_RE.fullmatch(compact) or _REPEATED_XL_RE.fullmatch(compact))


def parse_promotions(raw: Optional[str]) -> Tuple[str, ...]:
    if raw is None:
        return ()
    seen = set()
    values: List[str] = []
    for part in raw.split(","):
        token = part.strip()
        if token and token not in seen:
            seen.add(token)
            values.append(token)
    return tuple(values)


def parse_date(raw: str) -> Date:
    cleaned = raw.strip()
    if not re.fullmatch(r"\d{2}-\d{2}-\d{2}", cleaned):
        raise ValueError(f"Ngay khong hop le: {raw!r}")
    month_text, day_text, year_text = cleaned.split("-")
    return Date(2000 + int(year_text), int(month_text), int(day_text))


def _required_text(raw: Optional[str], field: str) -> str:
    if raw is None:
        raise ValueError(f"Thieu truong bat buoc: {field}")
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError(f"Thieu truong bat buoc: {field}")
    return cleaned


def _required_int(raw: Optional[str], field: str) -> int:
    text = _required_text(raw, field)
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"Gia tri khong hop le cho {field}: {text!r}") from exc


def _optional_float(raw: Optional[str], field: str) -> Optional[float]:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError as exc:
        raise ValueError(f"Gia tri khong hop le cho {field}: {text!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"Gia tri khong hop le cho {field}: {text!r}")
    return value


def _optional_dimension(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    cleaned = normalize_dimension(raw)
    return cleaned


def _parse_row(raw_row: Sequence[str], line_number: int) -> SaleRow:
    if len(raw_row) != len(HEADER):
        raise ValueError(f"Dong {line_number}: can {len(HEADER)} cot, nhan {len(raw_row)} cot")

    return SaleRow(
        record_id=_required_int(raw_row[0], "index"),
        order_id=_required_text(raw_row[1], "Order ID"),
        order_date=parse_date(_required_text(raw_row[2], "Date")),
        status=normalize_dimension(_required_text(raw_row[3], "Status")) or "",
        fulfilment=normalize_dimension(_required_text(raw_row[4], "Fulfilment")) or "",
        service_level=normalize_dimension(_required_text(raw_row[6], "ship-service-level")) or "",
        style=_optional_dimension(raw_row[7]),
        sku=_optional_dimension(raw_row[8]),
        size=_optional_dimension(raw_row[10]),
        courier_status=_optional_dimension(raw_row[12]),
        qty=_required_int(raw_row[13], "Qty"),
        amount=_optional_float(raw_row[15], "Amount"),
        city=_optional_dimension(raw_row[16]),
        state=_optional_dimension(raw_row[17]),
        promotion_ids=parse_promotions(raw_row[20]),
    )


def load_rows(path: Path) -> List[SaleRow]:
    if not path.is_file():
        raise FileNotFoundError(f"Khong tim thay CSV dau vao: {path}")

    rows: List[SaleRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise ValueError("CSV dau vao rong")
        if header != HEADER:
            raise ValueError("Header CSV khong khop schema 24 cot bat buoc")
        for line_number, raw_row in enumerate(reader, start=2):
            if not raw_row or all(not cell.strip() for cell in raw_row):
                continue
            rows.append(_parse_row(raw_row, line_number))
    return rows


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(list(header))
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])

