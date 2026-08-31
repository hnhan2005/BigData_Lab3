# Evidence Task 2-1

Thư mục này lưu các bằng chứng phục vụ việc kiểm tra Task 2-1 — tính tỷ lệ đơn hàng `Cancelled + Standard` theo `(state, city)` bằng Spark DataFrame API.

## Các file

### `extended-plan.txt`

Chứa execution plan mở rộng của Spark, tương đương kết quả của `explain(true)`. File này dùng để kiểm tra:

- các phép `Join` mà Spark lựa chọn;
- `BroadcastHashJoin` hoặc `SortMergeJoin`;
- số lượng `Exchange`/shuffle;
- các bước `Sort`, `Aggregate` và `Filter`;
- điều kiện `Status contains CANCELLED` trong logical/physical plan.

Không cần đọc toàn bộ file khi kiểm tra nhanh. Có thể tìm các từ khóa `BroadcastHashJoin`, `SortMergeJoin`, `Exchange` và `Sort`.

### `execution-summary.txt`

Là bản tóm tắt ngắn của execution plan và lần chạy Spark, gồm phiên bản Spark, master, join strategy, số `Exchange` và stage ID. File này giúp đưa số liệu vào Report mà không phải chép toàn bộ `extended-plan.txt`.

## Cách sử dụng trong Report

Report nên trình bày bảng so sánh join strategy, Exchange và Sort, sau đó dẫn nguồn tới hai file trong thư mục này. Không nên chép toàn bộ execution plan vào Report; chỉ cần trích các node và số liệu quan trọng.

## Cách xem

Xem toàn bộ plan:

```powershell
Get-Content -Encoding UTF8 docs/evidence/task21/extended-plan.txt
```

Tìm nhanh các node quan trọng:

```powershell
Select-String -Path docs/evidence/task21/extended-plan.txt `
  -Pattern "BroadcastHashJoin|SortMergeJoin|Exchange|Sort"
```

Xem bản tóm tắt:

```powershell
Get-Content -Encoding UTF8 docs/evidence/task21/execution-summary.txt
```
