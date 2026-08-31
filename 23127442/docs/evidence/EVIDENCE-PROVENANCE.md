# Nguồn gốc evidence — lần chạy gần nhất

Ngày chạy: 2026-08-31  
Dataset: `Amazon Sale Report.csv`  
Phiên bản mã nguồn: sau khi áp dụng các quy tắc trong `rules.md`

## Môi trường

- Java: Temurin 8.0.502
- Scala: 2.11.12
- SBT: 1.5.8
- Hadoop API: 3.3.6
- Spark: 2.4.8
- Kiểm tra độc lập: Python virtual environment với pandas 2.3.2 và pyarrow 21.0.0

## Kết quả xác minh

- `independent-validation/full-data-comparison.md`: tổng hợp số liệu full-data và đối chiếu với các mốc trong slide.
- `independent-validation/task12-global-vs-local.csv`: so sánh hai cách hiểu của Task 1-2; output chính sử dụng scope toàn cục.
- `independent-validation/official-validator.txt`: kết quả `ValidationMain`; cả bốn output đạt kiểm tra schema, key và invariant.
- `independent-validation/output-manifest.txt`: kích thước và SHA-256 của bốn output hiện tại.
- `independent-validation/runtime-status.txt`: phiên bản runtime và trạng thái môi trường chạy.

## Bốn output được xác minh

- `outputs/Task_1-1.csv`: 3.696 dòng, ngày cuối `2022-07-09`.
- `outputs/Task_1-2.csv`: 143 dòng theo cách hiểu global đã chọn; evidence local có 128 dòng.
- `outputs/Task_2-1.parquet`: 1.442 nhóm state-city, percentage bằng `0%`.
- `outputs/Task_2-2.parquet`: 16.486 nhóm SKU-month, nhóm lớn nhất 426 dòng.

## Evidence kế hoạch và benchmark

- `task21/extended-plan.txt`: execution plan của Task 2-1 sau revision, bao gồm điều kiện `Status contains CANCELLED`.
- `task22/benchmark-samples.csv` và `task22/benchmark-summary.csv`: mẫu đo và thống kê benchmark Task 2-2.
- `task22/group-profile.txt`: thống kê kích thước group Task 2-2.
- `task22/threshold-deltas`, `task22/set-difference-summary`, `task22/set-difference-examples`: so sánh threshold và qualifying set giữa approximate/exact.
- Các file `part-*.csv` trong các thư mục trên là dữ liệu evidence; `_SUCCESS` và `.crc` chỉ là file điều khiển/checksum của Spark.

## Giới hạn của lần chạy

Máy Windows hiện tại không có WSL/Docker và Hadoop yêu cầu `winutils.exe`. Vì vậy Hadoop MapReduce pseudo-distributed và thao tác ghi Parquet bằng Spark không thể hoàn tất trực tiếp trên máy này. Các output hiện tại được tạo/đối soát bằng kiểm chứng độc lập, sau đó được đọc lại và xác nhận bằng `ValidationMain`.

Evidence trong file này chỉ mô tả lần chạy gần nhất ngày 2026-08-31. Không sử dụng các kết quả của những lần chạy trước để kết luận cuối cùng.
