# Big Data Lab 3 — MapReduce và Spark

Tài liệu này hướng dẫn chạy bài hoàn toàn trên máy Lab 1/pseudo-distributed, không dùng Google Colab. Các lệnh bên dưới chạy trong terminal Linux tại thư mục `bigdata-lab3`.

## 1. Kiểm tra môi trường

```bash
java -version
scala -version
hadoop version
spark-submit --version
sbt --version
bash scripts/preflight.sh
```

Mục đích: xác nhận đúng Java 8, Scala 2.11.12, Hadoop 3.3.6 và Spark 2.4.8 build cho Scala 2.11. Script sẽ dừng và báo rõ thành phần bị thiếu/sai phiên bản. Không chạy JAR Scala 2.11 bằng Spark build cho Scala 2.12.

## 2. Khởi động Hadoop pseudo-distributed

```bash
start-dfs.sh
start-yarn.sh
jps
hdfs dfsadmin -report
yarn node -list
```

Mục đích: khởi động HDFS/YARN và kiểm tra NameNode, DataNode, ResourceManager, NodeManager đang hoạt động. Nếu các dịch vụ đã chạy thì không cần khởi động lần nữa.

## 3. Build và chạy test

```bash
sbt clean test assembly
jar tf target/scala-2.11/bigdata-lab3.jar | grep -E '^org/apache/(spark|hadoop)/' || true
```

Kết quả mong đợi: test thành công, tạo `target/scala-2.11/bigdata-lab3.jar`; lệnh `grep` không in class Spark/Hadoop vì hai runtime này do môi trường cung cấp.

## 4. Chuẩn bị input và thư mục kết quả

```bash
mkdir -p outputs docs/evidence
hdfs dfs -mkdir -p /user/$USER/lab3/input
hdfs dfs -put "../Amazon Sale Report.csv" /user/$USER/lab3/input/amazon-sales.csv
```

Mục đích: MapReduce đọc CSV từ HDFS. Dấu ngoặc kép là bắt buộc vì tên file có khoảng trắng. Nếu HDFS input đã tồn tại, kiểm tra trước rồi chỉ dùng `-put -f` khi thật sự muốn ghi đè.

```bash
hdfs dfs -ls /user/$USER/lab3/input
hdfs dfs -du -h /user/$USER/lab3/input/amazon-sales.csv
```

Kết quả mong đợi: thấy một input khoảng 68,9 MB.

## 5. Chạy Task 1-1 — cửa sổ trượt động

```bash
hadoop jar target/scala-2.11/bigdata-lab3.jar lab3.task11.Task11Main \
  --input /user/$USER/lab3/input/amazon-sales.csv \
  --work /user/$USER/lab3/work/task11 \
  --output-local "$PWD/outputs/Task_1-1.csv" \
  --reducers 2
```

Mục đích: chạy ba job MR: đếm bought row theo bang, map mỗi row vào các bucket tương lai, rồi chọn size thắng. Bang có tổng bought `> 10000` dùng 5 ngày; còn lại dùng 10 ngày. Bought nghĩa là Status chứa `SHIPPED` và `Qty != 0`.

```bash
head outputs/Task_1-1.csv
```

Header phải là `state,window_date,window_days,winning_size,frequency,population_variance`.

## 6. Chạy Task 1-2 — median variety

```bash
hadoop jar target/scala-2.11/bigdata-lab3.jar lab3.task12.Task12Main \
  --input /user/$USER/lab3/input/amazon-sales.csv \
  --work /user/$USER/lab3/work/task12 \
  --output-local "$PWD/outputs/Task_1-2.csv" \
  --reducers 2
```

Mục đích: chỉ lấy bought rows, đếm distinct SKU theo style/bang/tháng đối với style có ít nhất một size từ XXL trở lên, rồi tính exact median.

```bash
head outputs/Task_1-2.csv
```

Header phải là `state,month,median_variety,qualifying_style_count`.

## 7. Chạy Task 2-1 — tỷ lệ Cancelled Standard

```bash
INPUT_URI=$(readlink -f "../Amazon Sale Report.csv")
spark-submit --master local[2] \
  --class lab3.task21.Task21Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$PWD/outputs/Task_2-1.parquet" \
  --evidence-dir "$PWD/docs/evidence/task21"
```

Mục đích: dùng DataFrame API để tính tử số/mẫu số theo `(state,city)`, ghi một file Parquet vật lý và lưu extended plan, join strategy, số Exchange, stage IDs. Mẫu số là toàn bộ Cancelled + Standard rows; tử số thêm promo hợp lệ `>=3`, Amount dưới trung bình bang của Merchant + Courier Shipped.

## 8. Chạy Task 2-2 — approximate và exact percentile

```bash
spark-submit --master local[2] \
  --class lab3.task22.Task22Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$PWD/outputs/Task_2-2.parquet" \
  --evidence-dir "$PWD/docs/evidence/task22" \
  --accuracy 10000 \
  --runs 5
```

Mục đích: chạy built-in `percentile_approx` và exact nearest-rank P80/P90, so sánh threshold/tập qualifying, benchmark tối thiểu 5 lần mỗi phương pháp và ghi một file Parquet.

## 9. Kiểm tra độc lập bốn output

```bash
bash scripts/validate-outputs.sh \
  target/scala-2.11/bigdata-lab3.jar \
  "$PWD/outputs"
```

Kết quả mong đợi: `[OK] Bốn output đã qua schema, key và invariant validation`.

## 10. Chạy toàn bộ bằng một lệnh

```bash
bash scripts/run-all.sh \
  --jar "$PWD/target/scala-2.11/bigdata-lab3.jar" \
  --input-local "$(readlink -f '../Amazon Sale Report.csv')" \
  --hdfs-input /user/$USER/lab3/input/amazon-sales.csv \
  --hdfs-work /user/$USER/lab3/work \
  --output-dir "$PWD/outputs" \
  --evidence-dir "$PWD/docs/evidence"
```

Script dừng ngay khi một bước lỗi, không xóa đường dẫn HDFS rộng và tự chạy validator cuối. Chỉ thêm `--overwrite` khi muốn thay work/output cũ.

## 11. Chạy lại và xử lý lỗi thường gặp

- `Output đã tồn tại`: đổi tên output hoặc chạy lại với `--overwrite` sau khi đã sao lưu kết quả cần giữ.
- `Work path đã tồn tại`: kiểm tra bằng `hdfs dfs -ls`, sau đó dùng `--overwrite`; không xóa `/`, `/user` hay toàn bộ home HDFS.
- `No FileSystem for scheme`: kiểm tra `core-site.xml`, `HADOOP_CONF_DIR` và dùng path HDFS hợp lệ.
- `UnsupportedClassVersionError`: phải dùng Java 8 cho baseline này.
- `NoSuchMethodError`/Scala signature error khi Spark chạy: `spark-submit --version` không khớp Spark 2.4.x/Scala 2.11; không ép chạy bằng Spark 3/Scala 2.12.
- `Incompatible Jackson version`: dùng classpath của bản phân phối Spark tương thích; không thêm Hadoop/Jackson JAR vào `--jars` tùy tiện.
- Thiếu `winutils.exe`: đây là lỗi Windows Hadoop; chạy bài trên môi trường Linux Lab 1/pseudo-distributed như đề yêu cầu.

## 12. Các lưu ý bắt buộc về semantics

- Grain là một CSV row, dùng cột `index` làm `record_id`; không deduplicate theo Order ID.
- Promotion token được tách đúng CSV, trim, bỏ rỗng và distinct trong từng row; promotion có chữ Amazon vẫn được tính.
- Task 1-1 map ngày mua `t` vào `t+1..t+w`, nên output có thể có ngày sau ngày lớn nhất của input.
- Task 1-2 dùng bought rows; median chẵn là trung bình hai giá trị giữa.
- Task 2-2 exact percentile là observed value tại rank `ceil(p*N)`; Amount null không vào stddev nhưng row vẫn thuộc percentile population.
- `shapes.parquet(legacy)` không liên quan đề và không được đưa vào bài nộp.
