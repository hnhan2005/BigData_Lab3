# Hand-calculated fixture expectations

## `shared-sales.csv`

- 3 data rows and a header with exactly 24 columns.
- Row 0 is bought, has city `City, A`, amount 100, and two distinct promotions in input order: `Amazon Promo`, `Promo X`.
- Row 1 is not bought because its status is Cancelled and quantity is zero; Amount and promotions are empty.
- Row 2 is bought because Status contains `Shipped` and Qty is 2; `3XL` is at least XXL; Amount is empty.
- Dates map to month `2022-04`.

## `invalid-sales.csv`

- The data row is invalid for record ID, date, quantity, and finite amount.
- The parser returns the first typed validation error (`index`) deterministically.
