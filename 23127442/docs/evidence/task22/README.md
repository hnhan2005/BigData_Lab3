# Evidence Task 2-2 — Approximate và Exact Percentile

## 1. Mục đích và phạm vi

Thư mục này lưu bằng chứng thực nghiệm cho Task 2-2: so sánh cách tính ngưỡng P80/P90 approximate và exact trên từng nhóm `(SKU, month)` bằng Spark DataFrame API.

Evidence được dùng để trả lời ba câu hỏi của bài toán:

1. Quy mô và kích thước các group có phù hợp với việc dùng Window hay cần repartition đặc biệt không?
2. Approximate threshold có khác exact threshold trên dữ liệu thực tế không?
3. Hai phương pháp có tạo ra qualifying set khác nhau và có khác nhau về thời gian chạy không?

Các file trong thư mục là artefact phục vụ phân tích và tái lập kết quả; output cần nộp vẫn là `23127442/outputs/Task_2-2.parquet`.

## 2. Thiết kế thí nghiệm và provenance

`Task22Main` thực hiện các bước sau trên cùng một input:

1. Đọc và chuẩn hóa dữ liệu bằng `SparkSaleReader`.
2. Tạo `skuMonthBase` ở row grain và cache DataFrame sau khi materialize bằng `base.count()`.
3. Tính approximate threshold bằng `percentile_approx` với `accuracy=10000`.
4. Tính exact threshold bằng Window, `row_number` và nearest-rank `ceil(p*N)`.
5. Tạo qualifying statistics cho cả hai phương pháp.
6. So sánh threshold và qualifying set.
7. Benchmark hai action với một warm-up và tối thiểu năm lần đo cho mỗi method.
8. Ghi extended plan, execution summary và output Parquet.

Benchmark dùng cùng cached input và cùng logic thống kê. Thứ tự chạy được xen kẽ: run lẻ `approx → exact`, run chẵn `exact → approx`, nhằm giảm ảnh hưởng của thứ tự thực thi. Độ lệch chuẩn được tính là **sample standard deviation**, tức chia cho `runs - 1`.

Môi trường evidence hiện tại là Spark `2.4.8`, master `local[2]`, `accuracy=10000` và `runs=5`. Runtime phụ thuộc máy, JVM, số partition và trạng thái cache; vì vậy benchmark chỉ có ý nghĩa khi so sánh hai method trong cùng một lần chạy.

## 3. Danh mục file evidence

| File/thư mục | Nội dung | Cách tạo | Giá trị sử dụng |
|---|---|---|---|
| [`group-profile.txt`](group-profile.txt) | Số row hợp lệ, số group, kích thước group lớn nhất, số group trên 1.000 row và ước lượng bộ nhớ. | `Task22Pipeline.groupSizeProfile(base)` sau khi cache input. | Biện minh cho quyết định dùng Window và không repartition thủ công theo từng group. |
| [`benchmark-samples.csv`](benchmark-samples.csv) | Elapsed time của từng run cho `approx` và `exact`. | `BenchmarkHarness.compare` đo bằng `System.nanoTime()`, quy đổi sang milliseconds. | Dữ liệu gốc để kiểm tra mean, sample standard deviation và sự dao động giữa các run. |
| [`benchmark-summary.csv`](benchmark-summary.csv) | `runs`, `mean_ms` và `sample_stddev_ms` cho từng method. | `BenchmarkHarness.summarize` tổng hợp từ samples. | Nguồn chính cho bảng benchmark trong Report. |
| [`threshold-deltas/`](threshold-deltas/) | Threshold approximate, exact và hiệu `approx - exact` theo `(SKU, month, percentile_level)`. | `Task22Pipeline.compare` join hai bảng threshold và tạo `threshold_delta`. | Kiểm tra trực tiếp độ lệch giữa hai định nghĩa/tính toán percentile. |
| [`set-difference-summary/`](set-difference-summary/) | Số record chỉ thuộc qualifying set approximate hoặc chỉ thuộc qualifying set exact. | So sánh hai set bằng left anti join trên `(sku, month, percentile_level, record_id)`. | Đánh giá threshold difference có làm thay đổi tập record được chọn hay không. |
| [`set-difference-examples/`](set-difference-examples/) | Tối đa 100 record đại diện cho phần khác nhau giữa hai qualifying set. | Lấy từ union của `approx_only` và `exact_only`, rồi `limit(100)`. | Minh họa nguyên nhân/ảnh hưởng ở cấp record; không phải danh sách khác biệt đầy đủ. |
| [`extended-plan.txt`](extended-plan.txt) | Extended logical, analyzed, optimized và physical plan của pipeline. | `PlanEvidence.writeExtendedPlan(result, ...)`. | Truy nguyên `Window`, `Aggregate`, `Join`, `Exchange`, `Sort` và `percentile_approx`. |
| [`execution-summary.txt`](execution-summary.txt) | Spark version, master, accuracy, runs, Exchange và stage ID benchmark. | Ghi sau khi action và benchmark hoàn tất. | Mô tả điều kiện chạy và cung cấp metadata để kiểm tra provenance. |

## 4. Giải thích chi tiết từng nhóm evidence

### 4.1. `group-profile.txt`

File có dạng key-value, gồm:

```text
valid_rows=...
group_count=...
maximum_group_rows=...
groups_above_1000=...
estimated_bytes_per_row=...
estimated_maximum_group_bytes=...
```

`valid_rows` là số row sau khi yêu cầu `record_id`, `sku`, `month` và `order_date` hợp lệ. `group_count` là số group `(SKU, month)`. `maximum_group_rows` và `groups_above_1000` mô tả nguy cơ một group quá lớn khi thực hiện Window. Hai trường estimated chỉ là ước lượng phục vụ quyết định kỹ thuật, không phải dung lượng Parquet đo trực tiếp.

Kết quả hiện tại là 128.975 valid rows, 16.486 groups, group lớn nhất 426 rows và 0 group vượt 1.000 rows. Vì vậy Window exact có thể xử lý được trong cấu hình hiện tại mà không cần thiết kế repartition riêng theo group.

### 4.2. `benchmark-samples.csv` và `benchmark-summary.csv`

`benchmark-samples.csv` là dữ liệu quan sát ở cấp run. Mỗi dòng có:

```text
method,run,elapsed_ms
```

`benchmark-summary.csv` là dữ liệu đã tổng hợp:

```text
method,runs,mean_ms,sample_stddev_ms
```

Mean được tính bằng:

```text
mean = (Σ elapsed_ms) / n
```

Sample standard deviation được tính bằng:

```text
s = sqrt(Σ(elapsed_ms - mean)² / (n - 1))
```

Snapshot hiện tại ghi nhận:

| Method | Runs | Mean | Sample stddev |
|---|---:|---:|---:|
| Approximate | 5 | 704.8 ms | 11.7346 ms |
| Exact | 5 | 785.8 ms | 7.8549 ms |

Kết quả cho thấy exact chậm hơn khoảng 81.0 ms trong lần chạy này, phù hợp với việc exact phải sắp xếp dữ liệu trong từng group bằng Window. Đây là kết luận về runtime của lần chạy local, không phải một hằng số cho mọi cluster.

### 4.3. `threshold-deltas/`

Mỗi `part-*.csv` có schema:

```text
sku,month,percentile_level,approx_threshold,exact_threshold,threshold_delta
```

Trong đó:

```text
threshold_delta = approx_threshold - exact_threshold
```

Threshold exact được xác định theo nearest-rank:

```text
rank = ceil(p × N)
```

Threshold approximate được xác định bởi Spark `percentile_approx` với accuracy 10.000. Cần đọc `threshold_delta` thay vì chỉ nhìn `approx_threshold` hoặc `exact_threshold` riêng lẻ. Nếu delta khác 0, cần tiếp tục kiểm tra set-difference để biết số record qualifying thay đổi.

Evidence hiện tại không ghi nhận threshold delta khác 0 trên các group được đối soát.

### 4.4. `set-difference-summary/`

Mỗi dòng có dạng:

```text
sku,month,percentile_level,approx_only_count,exact_only_count
```

`approx_only_count` là số `record_id` được chọn bởi approximate nhưng không được chọn bởi exact. Ngược lại, `exact_only_count` là số record chỉ có trong exact. Hai tập được so sánh bằng logical key gồm `(sku, month, percentile_level, record_id)`; do đó một record không bị xem là khác chỉ vì thứ tự output khác nhau.

Nếu cả hai count bằng 0 cho mọi group, hai phương pháp tạo cùng qualifying set dù implementation percentile khác nhau. Khi threshold khác nhau nhưng các promotion count nằm ngoài vùng biên, qualifying set vẫn có thể giống nhau; vì vậy cần báo cáo cả threshold delta lẫn set difference.

### 4.5. `set-difference-examples/`

Thư mục này chỉ lưu tối đa 100 dòng mẫu từ các record `approx_only` hoặc `exact_only`. File được tạo để minh họa, không được dùng để suy ra tổng số khác biệt. Khi không có khác biệt, file vẫn có thể tồn tại với header và không có data row.

Khi dùng trong Report, chỉ nên chọn một hoặc vài dòng đại diện, kèm giải thích threshold tương ứng. Không chép toàn bộ file vì phần tổng hợp đáng tin cậy hơn nằm trong `set-difference-summary`.

### 4.6. `extended-plan.txt`

Plan cho phép kiểm tra hai nhánh chính:

- nhánh approximate chứa `percentile_approx` và các `Aggregate` tương ứng;
- nhánh exact chứa Window `count` để lấy group size, Window `row_number` để lấy rank, cùng các bước sort/aggregate sau đó.

Ngoài ra, plan thể hiện các `Join` dùng để đưa promotion count và threshold trở lại row grain. Khi đọc plan, cần phân biệt plan logic với physical plan. Các nhận xét về chi phí sort/shuffle phải dựa vào phần physical plan và đối chiếu với execution summary.

### 4.7. `execution-summary.txt`

File có dạng key-value:

```text
spark_version=...
master=...
accuracy=...
runs=...
exchange_count=...
join_strategies=...
benchmark_stage_ids=...
```

File này ghi điều kiện chạy và metadata, không chứa các số percentile hay standard deviation. Vì vậy nó phải được đọc cùng `benchmark-summary.csv`, `threshold-deltas` và `group-profile.txt`.

## 5. Phương pháp tái lập evidence

Giả sử đã build jar theo hướng dẫn trong `23127442/docs/README.md`, các biến môi trường `INPUT_URI`, `OUTPUT_DIR` và `EVIDENCE_DIR` đã được thiết lập. Chạy từ thư mục `23127442`:

```bash
spark-submit --master local[2] \
  --class lab3.task22.Task22Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$OUTPUT_DIR/Task_2-2.parquet" \
  --evidence-dir "$EVIDENCE_DIR/task22" \
  --accuracy 10000 \
  --runs 5 \
  --overwrite
```

Để bảo đảm provenance của một lần chạy, nên xóa hoặc đổi tên evidence cũ trước khi chạy lại, hoặc chạy với một `EVIDENCE_DIR` mới. Spark tạo tên `part-*.csv` có UUID; không được nối các part file từ các lần chạy khác nhau để tính thống kê mới. `_SUCCESS` chỉ xác nhận Spark job ghi output thành công; `.crc` là checksum phụ và không phải dữ liệu phân tích.

Sau khi chạy, kiểm tra output bằng `ValidationMain`. Evidence chỉ mô tả execution; schema, key và invariant của Parquet phải được xác nhận riêng bởi validator.

## 6. Cách xem evidence

Từ repository root, xem điều kiện chạy và profile:

```powershell
Get-Content -Encoding UTF8 23127442/docs/evidence/task22/group-profile.txt
Get-Content -Encoding UTF8 23127442/docs/evidence/task22/execution-summary.txt
```

Xem benchmark:

```powershell
Import-Csv 23127442/docs/evidence/task22/benchmark-samples.csv | Format-Table
Import-Csv 23127442/docs/evidence/task22/benchmark-summary.csv | Format-Table
```

Đọc các part CSV dữ liệu, bỏ qua `_SUCCESS` và `.crc`:

```powershell
Get-ChildItem 23127442/docs/evidence/task22/threshold-deltas -Filter 'part-*.csv' |
  ForEach-Object { Import-Csv $_.FullName } | Format-Table

Get-ChildItem 23127442/docs/evidence/task22/set-difference-summary -Filter 'part-*.csv' |
  ForEach-Object { Import-Csv $_.FullName } | Format-Table

Get-ChildItem 23127442/docs/evidence/task22/set-difference-examples -Filter 'part-*.csv' |
  ForEach-Object { Import-Csv $_.FullName } | Format-Table
```

Tìm nhanh các operator quan trọng trong execution plan:

```powershell
Select-String -Path 23127442/docs/evidence/task22/extended-plan.txt `
  -Pattern 'percentile_approx|Window|row_number|Aggregate|Exchange|Sort|Join'
```

## 7. Liên hệ với báo cáo khoa học

Trong Report, nên trình bày evidence theo chuỗi lập luận:

1. Dùng `group-profile.txt` để mô tả quy mô dữ liệu và biện minh cho lựa chọn Window.
2. Dùng `benchmark-summary.csv`, có thể kiểm tra ngược bằng `benchmark-samples.csv`, để so sánh runtime.
3. Dùng `threshold-deltas` để đánh giá sai khác ở ngưỡng percentile.
4. Dùng `set-difference-summary` để đánh giá sai khác ở qualifying set.
5. Dùng `set-difference-examples` làm minh họa nếu thực sự có record khác nhau.
6. Dùng `extended-plan.txt` để giải thích nguồn chi phí Window/sort/join và dùng `execution-summary.txt` để ghi điều kiện thực nghiệm.

Không nên đưa toàn bộ raw plan hoặc toàn bộ CSV comparison vào Report. Cách phù hợp là trích bảng tổng hợp, nêu phương pháp tạo evidence, dẫn liên kết tới file gốc và ghi rõ môi trường chạy.

## 8. Giới hạn diễn giải

Benchmark local không đại diện tuyệt đối cho cluster phân tán. Physical plan và runtime có thể thay đổi theo phiên bản Spark, số partition, cache, JVM và dữ liệu. Ngoài ra, approximate threshold không nhất thiết khác exact trên mọi dataset; việc snapshot hiện tại có `threshold_delta = 0` không chứng minh hai phương pháp luôn cho cùng kết quả. Kết luận đúng của lần chạy là: với input, accuracy, Spark version và cấu hình hiện tại, hai phương pháp không tạo khác biệt threshold/qualifying set quan sát được, trong khi exact có runtime cao hơn approximate.
