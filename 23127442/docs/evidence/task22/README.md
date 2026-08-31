# Evidence Task 2-2

Thư mục này lưu các bằng chứng phục vụ việc kiểm tra Task 2-2 — so sánh percentile approximate và exact trên từng nhóm `(SKU, month)` bằng Spark.

## Các file

### `benchmark-samples.csv`

Chứa thời gian của từng lần chạy benchmark cho hai phương pháp `approx` và `exact`. Dùng để kiểm tra số lần chạy và làm dữ liệu đầu vào cho việc tính trung bình/độ lệch chuẩn.

### `benchmark-summary.csv`

Chứa kết quả tổng hợp benchmark: số lần chạy, mean và sample standard deviation của từng phương pháp. Đây là file phù hợp để lấy số liệu đưa vào bảng benchmark trong Report.

### `group-profile.txt`

Chứa thống kê kích thước các nhóm `(SKU, month)`, gồm số dòng hợp lệ, số group, group lớn nhất và số group vượt 1.000 dòng. File này dùng để giải thích quyết định repartition.

### `extended-plan.txt`

Chứa execution plan mở rộng của pipeline Task 2-2. Dùng để kiểm tra các bước `Window`, `Aggregate`, `Exchange`, `Sort` và các transformation Spark đã thực thi.

### `execution-summary.txt`

Là bản tóm tắt phiên bản Spark, master, accuracy, số lần benchmark, số `Exchange` và stage ID.

### `threshold-deltas/`

Chứa bảng so sánh threshold percentile approximate và exact theo từng `(SKU, month, percentile_level)`. File dữ liệu thật nằm ở `part-*.csv`.

### `set-difference-summary/`

Chứa số lượng record chỉ xuất hiện trong qualifying set của approximate hoặc exact. Dùng để đánh giá threshold khác nhau ảnh hưởng thế nào đến tập record được chọn.

### `set-difference-examples/`

Chứa một số record mẫu khác nhau giữa hai phương pháp. Đây là ví dụ giới hạn để minh họa, không phải toàn bộ danh sách khác biệt.

Trong các thư mục Spark nêu trên:

- `part-*.csv` là dữ liệu cần đọc;
- `_SUCCESS` chỉ báo Spark job hoàn tất;
- `.crc` là file checksum phụ, không cần phân tích.

## Cách sử dụng trong Report

Report nên dùng các file này để trình bày:

1. bảng benchmark approximate/exact;
2. số group và group lớn nhất;
3. chênh lệch threshold;
4. chênh lệch qualifying set;
5. kết luận về tốc độ và độ chính xác.

Không cần chép toàn bộ các file CSV vào Report; chỉ chọn các thống kê tổng hợp và một vài ví dụ đại diện.

## Cách xem

Xem benchmark:

```powershell
Import-Csv docs/evidence/task22/benchmark-summary.csv | Format-Table
Import-Csv docs/evidence/task22/benchmark-samples.csv | Format-Table
```

Xem group profile:

```powershell
Get-Content -Encoding UTF8 docs/evidence/task22/group-profile.txt
```

Xem các bảng Spark CSV:

```powershell
Import-Csv docs/evidence/task22/threshold-deltas/part-*.csv | Format-Table
Import-Csv docs/evidence/task22/set-difference-summary/part-*.csv | Format-Table
Import-Csv docs/evidence/task22/set-difference-examples/part-*.csv | Format-Table
```
