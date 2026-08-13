# Big Data Lab 3 - MapReduce và Spark

Tài liệu này là runbook trực tiếp cho WSL. Không cần thư mục `scripts/`; mọi lệnh được chạy thẳng trong terminal.

Trước khi bắt đầu, hãy bảo đảm máy có:
- WSL2 hoặc một môi trường Ubuntu có `sudo`.
- Java 8, Scala 2.11.12, Hadoop 3.3.6, Spark 2.4.8 và sbt 1.10.6.
- Các tiện ích `zip`, `unzip`, `curl`, `wget`, `tar`, `git`.

## 0. Quy ước đường dẫn

- `USER_NAME` là tên user WSL của bạn, ví dụ `<user_name>`.
- `WORKSPACE_ROOT` là thư mục chứa workspace.
- `LAB3_ROOT` là thư mục project `23127442`.
- `INPUT_CSV` là file `Amazon Sale Report.csv` đi kèm workspace.
- `src/Task_*` là source root trực tiếp cho từng task; mỗi task chỉ còn các file `.scala` nằm trực tiếp bên dưới thư mục đó, còn `src/common/source` vẫn giữ cho mã dùng chung.
- `python/` chứa các script Python đối chiếu, sinh 4 file CSV kết quả.

Ví dụ:

```bash
export USER_NAME="<user_name>"
export HOME_DIR="/home/${USER_NAME}"
export WORKSPACE_ROOT="${HOME_DIR}/BigData_Lab3"
export LAB3_ROOT="${WORKSPACE_ROOT}/23127442"
export INPUT_CSV="${WORKSPACE_ROOT}/Amazon Sale Report.csv"
```

Nếu bạn clone workspace ở thư mục khác, chỉ cần sửa `WORKSPACE_ROOT` cho đúng.

## 1. Cài đặt phụ thuộc trên WSL

### 1.1 Cài gói cơ bản

```bash
sudo apt update
sudo apt install -y openjdk-8-jdk curl wget tar unzip git zip
```

### 1.2 Cài Scala và sbt

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install scala 2.11.12
sdk install sbt 1.10.6
```

### 1.3 Giải nén Hadoop và Spark

Nếu bạn đã có sẵn bộ cài của Lab 1, chỉ cần trỏ lại các biến ở bước 2. Nếu chưa có, hãy giải nén các gói đúng version sau:

```bash
mkdir -p "$HOME/tools"
tar -xzf "$HOME/Downloads/hadoop-3.3.6.tar.gz" -C "$HOME/tools"
tar -xzf "$HOME/Downloads/spark-2.4.8-bin-hadoop2.7.tgz" -C "$HOME/tools"
```

## 2. Cấu hình môi trường

```bash
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export HADOOP_HOME="$HOME/tools/hadoop-3.3.6"
export SPARK_HOME="$HOME/tools/spark-2.4.8-bin-hadoop2.7"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HOME/.sdkman/candidates/scala/current/bin:$HOME/.sdkman/candidates/sbt/current/bin:$PATH"

# Nếu muốn giữ cấu hình lâu dài, hãy thêm các dòng export trên vào ~/.bashrc
```

## 3. Kiểm tra phiên bản

```bash
cd "$LAB3_ROOT"
java -version
scala -version
hadoop version
spark-submit --version
sbt --version
```

Mục tiêu: xác nhận Java 8, Scala 2.11.12, Hadoop 3.3.6, Spark 2.4.8 build cho Scala 2.11 và sbt đã sẵn sàng.

## 4. Khởi động Hadoop pseudo-distributed

```bash
cd "$LAB3_ROOT"
start-dfs.sh
start-yarn.sh
jps
hdfs dfsadmin -report
yarn node -list
```

Mục tiêu: NameNode, DataNode, ResourceManager và NodeManager đều đang chạy trước khi nạp input.

## 5. Build project

```bash
cd "$LAB3_ROOT"
sbt clean test assembly
jar tf target/scala-2.11/bigdata-lab3.jar | grep -E '^org/apache/(spark|hadoop)/' || true
```

Mục tiêu: tạo `target/scala-2.11/bigdata-lab3.jar` và bảo đảm Hadoop/Spark không bị đóng gói vào JAR.

## 6. Chuẩn bị input

```bash
cd "$LAB3_ROOT"
export OUTPUT_DIR="$LAB3_ROOT/outputs"
export EVIDENCE_DIR="$LAB3_ROOT/docs/evidence"
export HDFS_ROOT="/user/${USER_NAME}/lab3"

mkdir -p "$OUTPUT_DIR" "$EVIDENCE_DIR"
hdfs dfs -mkdir -p "$HDFS_ROOT/input"
hdfs dfs -put -f "$INPUT_CSV" "$HDFS_ROOT/input/amazon-sales.csv"
hdfs dfs -ls "$HDFS_ROOT/input"
hdfs dfs -du -h "$HDFS_ROOT/input/amazon-sales.csv"
```

Mục tiêu: đưa CSV vào HDFS để Task 1-1 và Task 1-2 có thể đọc lại nhiều lần.

## 7. Chạy Task 1-1

```bash
cd "$LAB3_ROOT"
hadoop jar target/scala-2.11/bigdata-lab3.jar lab3.task11.Task11Main \
  --input "$HDFS_ROOT/input/amazon-sales.csv" \
  --work "$HDFS_ROOT/work/task11" \
  --output-local "$OUTPUT_DIR/Task_1-1.csv" \
  --reducers 2
```

Nếu muốn chạy lại và ghi đè kết quả cũ, thêm `--overwrite` vào cuối lệnh.

Kiểm tra nhanh:

```bash
head "$OUTPUT_DIR/Task_1-1.csv"
```

## 8. Chạy Task 1-2

```bash
cd "$LAB3_ROOT"
hadoop jar target/scala-2.11/bigdata-lab3.jar lab3.task12.Task12Main \
  --input "$HDFS_ROOT/input/amazon-sales.csv" \
  --work "$HDFS_ROOT/work/task12" \
  --output-local "$OUTPUT_DIR/Task_1-2.csv" \
  --reducers 2
```

Nếu muốn chạy lại và ghi đè kết quả cũ, thêm `--overwrite` vào cuối lệnh.

Kiểm tra nhanh:

```bash
head "$OUTPUT_DIR/Task_1-2.csv"
```

## 9. Chạy Task 2-1

```bash
cd "$LAB3_ROOT"
export INPUT_URI="$(readlink -f "$INPUT_CSV")"

spark-submit --master local[2] \
  --class lab3.task21.Task21Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$OUTPUT_DIR/Task_2-1.parquet" \
  --evidence-dir "$EVIDENCE_DIR/task21"
```

Nếu muốn chạy lại và ghi đè kết quả cũ, thêm `--overwrite` vào cuối lệnh.

Mục tiêu: tạo một file Parquet và thu thập `extended-plan.txt`, `execution-summary.txt`.

## 10. Chạy Task 2-2

```bash
cd "$LAB3_ROOT"

spark-submit --master local[2] \
  --class lab3.task22.Task22Main \
  target/scala-2.11/bigdata-lab3.jar \
  --input "$INPUT_URI" \
  --output-local "$OUTPUT_DIR/Task_2-2.parquet" \
  --evidence-dir "$EVIDENCE_DIR/task22" \
  --accuracy 10000 \
  --runs 5
```

Nếu muốn chạy lại và ghi đè kết quả cũ, thêm `--overwrite` vào cuối lệnh.

Mục tiêu: chạy percentile approx/exact, benchmark và thu thập evidence cho Task 2-2.

## 11. Kiểm tra đầu ra

```bash
cd "$LAB3_ROOT"
spark-submit --master local[2] \
  --class lab3.io.ValidationMain \
  target/scala-2.11/bigdata-lab3.jar \
  --output-dir "$OUTPUT_DIR"
```

Lệnh này đọc lại 4 kết quả cuối cùng:

- `Task_1-1.csv`
- `Task_1-2.csv`
- `Task_2-1.parquet`
- `Task_2-2.parquet`

Nếu cần, bạn có thể kiểm tra thêm thư mục evidence:

```bash
find "$EVIDENCE_DIR" -maxdepth 2 -type f | sort
```

## 12. Đóng gói nộp bài

```bash
printf '%s\n' "<drive_folder_url>" > "$LAB3_ROOT/docs/drive_link.txt"

cd "$WORKSPACE_ROOT"
zip -r "23127442.zip" "23127442" \
  -x "23127442/.git/*" \
     "23127442/target/*" \
     "23127442/src/Task_1-1/.*" \
     "23127442/src/Task_1-2/.*" \
     "23127442/src/Task_2-1/.*" \
     "23127442/src/Task_2-2/.*"
```

Các thư mục ẩn `.lab3` và `.source` dưới task root là leftovers kỹ thuật; lệnh trên loại chúng khỏi ZIP để tree nộp bài chỉ còn các file `.scala` cần thiết.
Nếu giảng viên muốn kèm theo evidence, bạn có thể bỏ phần loại trừ `docs/evidence/*` nếu có thêm vào sau.
Nếu muốn đóng gói gọn hơn, bạn có thể xóa luôn `docs/evidence/` sau khi đã nộp evidence riêng.

## 13. Ghi chú quan trọng

- Không cần và không nên có thư mục `scripts/` trong bản nộp cuối cùng.
- `src/Task_*` là root trực tiếp cho code của từng task và chỉ nên chứa các file `.scala` nằm trực tiếp.
- Các thư mục ẩn `.lab3` và `.source` dưới task root chỉ là leftovers kỹ thuật của quá trình chuyển layout, không nên đóng gói.
- `src/common/source` vẫn được giữ lại cho code dùng chung.
- Nếu `spark-submit --version` khác Spark 2.4.8 / Scala 2.11, hãy cập nhật Design trước khi tiếp tục.

## 14. Xử lý lỗi môi trường

- Nếu shell báo không nhận ra `java`, `scala`, `sbt`, `hadoop` hoặc `spark-submit`, nghĩa là toolchain Lab 1 chưa được nạp vào môi trường hiện tại. Hãy mở đúng WSL/Ubuntu và kiểm tra lại `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME` và `PATH`.
- Nếu chưa có lệnh `sbt` nhưng đã có một JDK 8 chạy được, bạn vẫn có thể boot SBT bằng `java -jar sbt-launch.jar ...` để chạy `clean test assembly`.
- Nếu đang ở Windows PowerShell mà WSL chưa cài, các lệnh trong tài liệu này sẽ không chạy trực tiếp; tài liệu này giả định môi trường WSL như phần mở đầu đã nêu.

## 15. Chạy bộ Python đối chiếu

Nếu bạn chỉ cần sinh 4 file CSV để so sánh kết quả giữa các cách cài đặt, dùng bộ script trong `python/`.
Các script này chỉ làm đến bước tạo file kết quả, không benchmark, không thống kê và không sinh evidence phụ.

```bash
cd "$LAB3_ROOT"
python python/main.py --input "$INPUT_CSV" --output-dir "$LAB3_ROOT/python/output"
```

Nếu bạn không truyền `--input`, `main.py` sẽ tự tìm `Amazon Sale Report.csv` ở workspace gốc.
Nếu muốn đổi độ chính xác cho phần `approx` của Task 2-2, thêm `--accuracy <so_nguyen>`.

Kết quả sẽ nằm trong:

- `python/output/Task_1-1.csv`
- `python/output/Task_1-2.csv`
- `python/output/Task_2-1.csv`
- `python/output/Task_2-2.csv`

Lưu ý:

- `Task_2-1.csv` và `Task_2-2.csv` trong thư mục `python/` là bản xuất CSV để đối chiếu.
- Luồng Scala gốc vẫn giữ đầu ra Parquet cho các bài 2-1 và 2-2 nếu bạn chạy theo các lệnh ở mục trước.
