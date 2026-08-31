# Evidence Task 2-1 — Physical Execution Plan

## 1. Mục đích và phạm vi

Thư mục này lưu bằng chứng thực nghiệm cho Task 2-1: tính tỷ lệ record `Cancelled + Standard` đạt điều kiện promotion và Amount theo khóa `(state, city)` bằng Spark DataFrame API.

Các file trong thư mục không thay thế output `Task_2-1.parquet`. Chúng mô tả **cách Spark thực thi pipeline** và cho phép kiểm tra độc lập các nhận xét về join strategy, shuffle, sort và stage trong báo cáo. Output dữ liệu được kiểm tra ở `23127442/outputs/Task_2-1.parquet`; evidence ở đây tập trung vào provenance của execution.

## 2. Thiết kế thí nghiệm

Pipeline được khởi tạo từ `SparkSaleReader` và `Task21Job.build`. Sau khi xây dựng DataFrame kết quả, `Task21Main` thực hiện hai nhánh phân tích:

1. **Cấu hình mặc định**: giữ `spark.sql.autoBroadcastJoinThreshold` mặc định để Spark tự chọn join strategy.
2. **Cấu hình tắt broadcast**: đặt `spark.sql.autoBroadcastJoinThreshold=-1` rồi xây dựng lại pipeline, buộc Spark không dùng broadcast join.

Ở mỗi nhánh, chương trình ghi extended plan và thực hiện action. `StageCollector` gắn với job group tương ứng để lưu các stage đã hoàn tất. Môi trường evidence hiện tại là Spark `2.4.8`, master `local[2]`. Vì vậy các stage ID và số node là bằng chứng của lần chạy local này; khi chạy trên môi trường Hadoop/Spark khác, stage ID có thể thay đổi.

## 3. Danh mục file evidence

| File | Nội dung | Cách tạo | Giá trị sử dụng trong báo cáo |
|---|---|---|---|
| [`extended-plan.txt`](extended-plan.txt) | Extended plan của cấu hình mặc định, gồm logical, analyzed, optimized và physical plan. | `Task21Main` gọi `PlanEvidence.writeExtendedPlan(result, ...)` trước action export. | Truy nguyên các phép `Join`, `Exchange`, `Sort`, `Aggregate`, `Filter` và điều kiện lọc của pipeline mặc định. |
| [`extended-plan-no-broadcast.txt`](extended-plan-no-broadcast.txt) | Extended plan khi `autoBroadcastJoinThreshold=-1`. | `Task21Main` đặt threshold bằng `-1`, xây dựng `noBroadcastResult`, sau đó ghi plan riêng. | Đối chiếu tác động của việc tắt broadcast lên SortMergeJoin, shuffle và sort. |
| [`execution-summary.txt`](execution-summary.txt) | Metadata ngắn gọn của hai lần chạy: Spark version, master, join strategy, số Exchange/Sort và stage ID. | Sau các action, `Task21Main` gọi `PlanEvidence.executedPlan`, đếm node và ghi summary. | Lấy số liệu tổng hợp để đưa vào bảng kết quả mà không phải chép toàn bộ plan. |

### 3.1. Ý nghĩa của `extended-plan.txt`

Đây là bằng chứng chi tiết nhất. Phần physical plan cho biết Spark đã chọn chiến lược nào sau khi tối ưu hóa. Khi đọc file, cần phân biệt:

- `BroadcastHashJoin`: một phía của phép join được broadcast dưới dạng hashed relation.
- `SortMergeJoin`: hai phía được phân vùng theo khóa và sort trước khi join.
- `Exchange`: biên shuffle/repartition giữa các stage.
- `Sort`: bước sắp xếp, thường xuất hiện trước SortMergeJoin hoặc trước `orderBy` output.
- `Aggregate`: các phép `groupBy`, `count`, `avg`, `countDistinct` và `stddev`.
- `Filter`: các predicate như `CANCELLED`, `STANDARD`, Amount khác null và span promotion.

Không nên kết luận chiến lược chỉ từ một dòng ở logical plan. Kết luận thực nghiệm phải dựa vào executed physical plan và được đối chiếu với `execution-summary.txt`.

### 3.2. Ý nghĩa của `extended-plan-no-broadcast.txt`

File này là đối chứng kiểm soát. Việc tắt broadcast không thay đổi công thức hay dữ liệu đầu vào; nó chỉ thay đổi một cấu hình tối ưu hóa. Vì vậy, nếu hai nhánh có cùng output logic nhưng no-broadcast có thêm `Exchange`/`Sort`, chênh lệch đó được quy cho execution strategy chứ không phải thay đổi nghiệp vụ.

### 3.3. Ý nghĩa của `execution-summary.txt`

Summary có hai phần:

- `default_*`: kết quả của cấu hình mặc định;
- `no_broadcast_*`: kết quả của cấu hình tắt broadcast.

Các trường `*_join_strategies` là tập tên chiến lược xuất hiện trong plan, không phải số lần xuất hiện của từng join. Trong snapshot hiện tại:

| Cấu hình | Join strategy quan sát được | Exchange | Sort |
|---|---|---:|---:|
| Mặc định | `BroadcastHashJoin`, `SortMergeJoin` | 8 | 3 |
| Tắt broadcast | `SortMergeJoin` | 9 | 7 |

Default plan có cả hai tên chiến lược vì các join nội bộ của pipeline không nhất thiết có cùng kích thước/phía build. Khi broadcast bị tắt, các join phù hợp phải chuyển sang SortMergeJoin, làm số Exchange tăng từ 8 lên 9 và Sort tăng từ 3 lên 7.

## 4. Phương pháp tái lập evidence

Giả sử đã build jar theo hướng dẫn trong `23127442/docs/README.md`, các biến môi trường `INPUT_URI`, `OUTPUT_DIR` và `EVIDENCE_DIR` đã được thiết lập. Chạy từ thư mục `23127442`:

```bash
spark-submit --master local[2] \
  --class lab3.task21.Task21Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$OUTPUT_DIR/Task_2-1.parquet" \
  --evidence-dir "$EVIDENCE_DIR/task21" \
  --overwrite
```

Một lần chạy lệnh trên tự tạo cả hai plan: cấu hình mặc định và cấu hình no-broadcast. Không cần chạy riêng một lệnh thứ hai cho `extended-plan-no-broadcast.txt`.

Về mặt logic, pipeline được tái lập theo thứ tự: đọc CSV explicit schema → chuẩn hóa dimension → explode/deduplicate promotion → lọc promotion có span tối thiểu → đếm promotion theo record → tính state average → left join vào denominator → group `(state, city)` → ghi Parquet. Evidence chỉ được xem là hợp lệ khi output action hoàn tất và summary được ghi đầy đủ.

## 5. Cách đọc nhanh

Từ repository root:

```powershell
Get-Content -Encoding UTF8 23127442/docs/evidence/task21/execution-summary.txt
```

Tìm các node quan trọng trong plan mặc định:

```powershell
Select-String -Path 23127442/docs/evidence/task21/extended-plan.txt `
  -Pattern 'BroadcastHashJoin|SortMergeJoin|Exchange|Sort|Aggregate|Filter'
```

So sánh hai plan:

```powershell
Select-String -Path 23127442/docs/evidence/task21/extended-plan.txt `
  -Pattern 'BroadcastHashJoin|SortMergeJoin|Exchange|Sort'
Select-String -Path 23127442/docs/evidence/task21/extended-plan-no-broadcast.txt `
  -Pattern 'BroadcastHashJoin|SortMergeJoin|Exchange|Sort'
```

Khi cần phân tích sâu, đọc phần physical plan trong hai file rồi kiểm tra lại số đếm trong `execution-summary.txt`. Không nên copy toàn bộ plan vào report; chỉ trích các node liên quan và dẫn link tới file evidence.

## 6. Diễn giải và giới hạn

Evidence này chứng minh lựa chọn execution strategy của Spark trong lần chạy cụ thể, không chứng minh rằng mọi phiên bản Spark hoặc mọi kích thước dữ liệu sẽ tạo đúng cùng plan. Physical plan phụ thuộc Spark version, statistics, cấu hình broadcast, số partition và master.

Các mốc plan trên slide (`3 BroadcastHashJoin / 4 Exchange / 0 Sort` và `3 SortMergeJoin / 7 Exchange / 6 Sort`) được xem là baseline tham khảo. Số liệu evidence hiện tại (`8/3` và `9/7`) được giữ riêng vì cách đếm node và plan thực tế bao gồm cả các join nội bộ, sort output và các stage của pipeline. Khi viết report, cần ghi rõ nguồn số liệu và không thay thế evidence hiện tại bằng baseline slide.

## 7. Liên hệ với báo cáo

Report nên sử dụng:

1. `execution-summary.txt` để lập bảng so sánh hai cấu hình;
2. `extended-plan.txt` để minh họa một đoạn physical plan có `BroadcastHashJoin`;
3. `extended-plan-no-broadcast.txt` để chứng minh sự chuyển sang `SortMergeJoin`;
4. output Parquet và validator để đánh giá kết quả dữ liệu, vì plan evidence không thay thế kiểm tra correctness của output.

Ảnh chụp trong report chỉ nên là ảnh cắt của các node quan trọng. Raw plan đầy đủ được giữ trong thư mục này để người chấm có thể kiểm tra lại.
