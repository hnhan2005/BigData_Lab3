# Big Data Lab 3 - MapReduce và Spark

Tài liệu này là runbook trực tiếp cho WSL. Không cần thư mục `scripts/`; mọi lệnh được chạy thẳng trong terminal.

Trước khi bắt đầu, hãy bảo đảm máy có:
- WSL2 hoặc một môi trường Ubuntu có `sudo`.
- Java 8, Scala 2.11.12, Hadoop 3.3.6, Spark 2.4.8 và sbt 1.5.8.
- Các tiện ích `zip`, `unzip`, `curl`, `wget`, `tar`, `git`.

## 0. Chuẩn bị user và quy ước đường dẫn

### 0.1. Tạo user Lab 3

Lab 3 được thực hiện bằng một user Linux riêng. Nếu user `khtn_<id>` chưa tồn tại, tạo user bằng:

```bash
sudo adduser khtn_<id>
```

Nếu cần thực hiện các thao tác quản trị hệ thống bằng user này, thêm user vào nhóm `sudo`:

```bash
sudo usermod -aG sudo khtn_<id>
```

Chuyển sang user dùng cho Lab 3:

```bash
su - khtn_<id>
```

> Từ bước này trở đi, các lệnh của Lab 3 được thực hiện dưới user `khtn_<id>`.
> Chỉ sử dụng `sudo` khi thao tác yêu cầu quyền quản trị hệ thống.

### 0.2. Quy ước đường dẫn

Các biến môi trường cơ bản được sử dụng trong runbook:

- `USER_NAME` là tên user Linux thực hiện Lab 3.
- `HOME_DIR` là thư mục home của user.
- `WORKSPACE_ROOT` là thư mục gốc chứa workspace Lab 3.
- `LAB3_ROOT` là thư mục project `<id>`.
- `INPUT_CSV` là file dữ liệu đầu vào `asr.csv`.
- `src/Task_*` là source root trực tiếp cho từng task; mỗi task chỉ còn các file `.scala` nằm trực tiếp bên dưới thư mục đó.
- `src/common/source` vẫn giữ các mã nguồn dùng chung.
- `python/` chứa các script Python dùng để đối chiếu kết quả và sinh 4 file CSV.

Thiết lập các biến môi trường và lưu vào `~/.bashrc` để tự động sử dụng
ở các terminal sau:

> **Lưu ý:** Thay `khtn_<id>` và `<id>` bằng username và MSSV thực tế.
> Ví dụ với MSSV `23127447`, sử dụng `khtn_23127447` và `23127447`.

```bash
cat >> ~/.bashrc <<'EOF'

# Big Data Lab 3
export USER_NAME="khtn_<id>"
export HOME_DIR="/home/${USER_NAME}"

export WORKSPACE_ROOT="${HOME_DIR}/BigData_Lab3"
export LAB3_ROOT="${WORKSPACE_ROOT}/<id>"
export INPUT_CSV="${WORKSPACE_ROOT}/asr.csv"
EOF
```

Nạp lại cấu hình:

```bash
source ~/.bashrc
```

### 0.3. Tạo cấu trúc workspace

Nếu workspace chưa tồn tại, tạo các thư mục cần thiết:

```bash
sudo mkdir -p "$WORKSPACE_ROOT"
sudo mkdir -p "$LAB3_ROOT"
sudo chown -R "$USER_NAME:$USER_NAME" "$WORKSPACE_ROOT"
```

Cấu trúc cơ bản:

```text
/home/khtn_<id>/
└── BigData_Lab3/
    ├── asr.csv
    └── <id>/
```
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
sdk install sbt 1.5.8
```

### 1.3. Tải và cài đặt Hadoop và Spark

Nếu chưa có sẵn bộ cài Hadoop và Spark, tải đúng phiên bản được yêu cầu:

- Hadoop 3.3.6
- Spark 2.4.8
- Spark 2.4.8 bản build cho Hadoop 2.7

Tạo thư mục chứa các phần mềm:

```bash
mkdir -p "$HOME/tools"
cd "$HOME/tools"
```

Tải Hadoop 3.3.6:

```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```

Tải Spark 2.4.8:

```bash
wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz
```

Giải nén Hadoop:

```bash
tar -xzf hadoop-3.3.6.tar.gz
```

Giải nén Spark:

```bash
tar -xzf spark-2.4.8-bin-hadoop2.7.tgz
```

Sau khi giải nén, hai thư mục phần mềm sẽ nằm tại:

```text
$HOME/tools/hadoop-3.3.6
$HOME/tools/spark-2.4.8-bin-hadoop2.7
```

Có thể kiểm tra:

```bash
ls -ld "$HOME/tools/hadoop-3.3.6"
ls -ld "$HOME/tools/spark-2.4.8-bin-hadoop2.7"
```


## 2. Cấu hình môi trường và Hadoop

### 2.1. Cấu hình biến môi trường

Thiết lập các biến môi trường cho Java, Hadoop, Spark, Scala và sbt. Đưa trực tiếp cấu hình vào `~/.bashrc` để hệ thống tự động nhận diện trong mọi phiên làm việc:

```bash
cat >> ~/.bashrc <<'EOF'

# Toolchain paths
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export HADOOP_HOME="$HOME/tools/hadoop-3.3.6"
export SPARK_HOME="$HOME/tools/spark-2.4.8-bin-hadoop2.7"

export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HOME/.sdkman/candidates/scala/current/bin:$HOME/.sdkman/candidates/sbt/current/bin:$PATH"
EOF
```

Nạp lại cấu hình để các biến có hiệu lực ngay lập tức:

```bash
source ~/.bashrc
```

### 2.2. Cấu hình Hadoop

Hadoop được cấu hình ở chế độ **pseudo-distributed**, trong đó NameNode,
DataNode, ResourceManager và NodeManager đều chạy trên cùng một máy WSL.

Cấu hình sử dụng `localhost`, do đó không cần tạo hostname `master`
hoặc chỉnh sửa `/etc/hosts`.

> **Lưu ý về username:** Trong các cấu hình bên dưới, `khtn_<id>` là username WSL được sử dụng để thực hiện Lab 3. Hãy thay `khtn_<id>` bằng username thực tế của bạn trước khi thực hiện.
>
> Ví dụ, nếu username là:
>
> ```text
> khtn_23127447
> ```
>
> thì đường dẫn:
>
> ```text
> /home/khtn_<id>
> ```
>
> phải được thay thành:
>
> ```text
> /home/khtn_23127447
> ```
>
> Các đường dẫn chứa `khtn_<id>` trong phần cấu hình Hadoop cần được thay tương ứng.

#### 2.2.1. Cấu hình Java cho Hadoop

Mở file:

```bash
nano "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
```

Thêm hoặc chỉnh sửa:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

#### 2.2.2. Cấu hình `core-site.xml`

Mở file:

```bash
nano "$HADOOP_HOME/etc/hadoop/core-site.xml"
```

Thay nội dung bằng:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>

  <property>
    <name>hadoop.tmp.dir</name>
    <value>/home/khtn_<id>/hadoop_tmp</value>
  </property>
</configuration>
```

Trong đó:

- `hdfs://localhost:9000` là địa chỉ NameNode.
- `hadoop.tmp.dir` là thư mục tạm được Hadoop sử dụng.

#### 2.2.3. Cấu hình `hdfs-site.xml`

Mở file:

```bash
nano "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"
```

Thay nội dung bằng:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/khtn_<id>/hadoop_data/namenode</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/khtn_<id>/hadoop_data/datanode</value>
  </property>
</configuration>
```

Tạo các thư mục dữ liệu:

```bash
mkdir -p "$HOME/hadoop_tmp"
mkdir -p "$HOME/hadoop_data/namenode"
mkdir -p "$HOME/hadoop_data/datanode"
```

#### 2.2.4. Cấu hình `yarn-site.xml`

Mở file:

```bash
nano "$HADOOP_HOME/etc/hadoop/yarn-site.xml"
```

Thay nội dung bằng:

```xml
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
```

#### 2.2.5. Cấu hình `mapred-site.xml`

Mở file:

```bash
nano "$HADOOP_HOME/etc/hadoop/mapred-site.xml"
```

Thay nội dung bằng:

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>

  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/home/khtn_<id>/tools/hadoop-3.3.6</value>
  </property>

  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/home/khtn_<id>/tools/hadoop-3.3.6</value>
  </property>

  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/home/khtn_<id>/tools/hadoop-3.3.6</value>
  </property>
</configuration>
```

### 2.3. Các thư mục sử dụng trong Hadoop

Sau khi hoàn tất cấu hình, cấu trúc thư mục chính của Hadoop trong
home của user sẽ có dạng:

```text
/home/khtn_<id>/
├── tools/
│   ├── hadoop-3.3.6/
│   └── spark-2.4.8-bin-hadoop2.7/
├── hadoop_tmp/
└── hadoop_data/
    ├── namenode/
    └── datanode/
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

Mục tiêu: xác nhận Java 8, Scala 2.11.12, Hadoop 3.3.6, Spark 2.4.8 build cho Scala 2.11 và sbt 1.5.8 đã sẵn sàng.

## 4. Khởi động Hadoop pseudo-distributed

### 4.1. Cấu hình SSH Key

Hadoop sử dụng SSH để khởi động các daemon trên `localhost`. Thực hiện
cấu hình SSH passwordless cho user hiện tại:

```bash
sudo apt install -y openssh-server openssh-client
sudo service ssh start

ssh-keygen -t rsa -P "" -f "$HOME/.ssh/id_rsa"

cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"

chmod 700 "$HOME/.ssh"
chmod 600 "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/id_rsa"
chmod 644 "$HOME/.ssh/id_rsa.pub"
```

Kiểm tra SSH vào chính máy hiện tại:

```bash
ssh localhost
```

Nếu được yêu cầu xác nhận lần đầu, nhập:

```text
yes
```

Sau khi đăng nhập thành công, thoát SSH:

```bash
exit
```

### 4.2. Format NameNode

**Chỉ thực hiện bước này khi thiết lập Hadoop lần đầu hoặc muốn khởi
tạo lại HDFS. Không thực hiện lại nếu đang có dữ liệu HDFS cần giữ.**

```bash
hdfs namenode -format
```

### 4.3. Khởi động HDFS và YARN

```bash
cd "$LAB3_ROOT"

start-dfs.sh
start-yarn.sh
```

Kiểm tra các daemon:

```bash
jps
hdfs dfsadmin -report
yarn node -list
```

Mục tiêu: NameNode, DataNode, ResourceManager và NodeManager đều đang chạy trước khi nạp input.

## 5. Build project

Copy toàn bộ nội dung bên trong folder MSSV từ máy local vào `$LAB3_ROOT`.

> **Ví dụ:** Nếu folder source code của bạn đang nằm tại `/tmp/<RepresentativeID>` (vd: `/tmp/23127447`), hãy chạy lệnh dưới đây. 
> *Lưu ý: Bắt buộc phải có dấu `/*` ở cuối đường dẫn nguồn để copy phần "ruột", tránh bị lỗi lồng thư mục (như `$LAB3_ROOT/23127447/src/...`).*

```bash
cp -r /tmp/<RepresentativeID>/* "$LAB3_ROOT/"
```

**Kiểm tra cấu trúc thư mục:**
Sau khi copy, bên trong `$LAB3_ROOT` (tức là `~/BigData_Lab3/<id>`) phải có cấu trúc các thư mục và file chính như sau:

```text
$LAB3_ROOT/
 ├── build.sbt      (File cấu hình chính để sbt nhận diện project)
 ├── docs/          (Thư mục chứa Report.pdf, README.md, thư mục evidence...)
 ├── project/       (Thư mục chứa cấu hình plugin và version của sbt)
 └── src/           (Thư mục chứa toàn bộ mã nguồn Scala)
     ├── Task_1-1/
     ├── Task_1-2/
     ├── Task_2-1/
     ├── Task_2-2/
     └── common/
```

```bash
cd "$LAB3_ROOT"
sbt clean test assembly
jar tf target/scala-2.11/bigdata-lab3.jar | grep -E '^org/apache/(spark|hadoop)/' || true
```

Mục tiêu: tạo `target/scala-2.11/bigdata-lab3.jar` và bảo đảm Hadoop/Spark không bị đóng gói vào JAR.

## 6. Chuẩn bị input

Trước tiên, copy file dữ liệu gốc `asr.csv` từ máy local vào thư mục workspace (`$WORKSPACE_ROOT`). 

> **Ví dụ:** Nếu file `asr.csv` đang nằm ở `/tmp/asr.csv` hoặc nằm bên ổ C của Windows (như `/mnt/c/Users/Name/Downloads/asr.csv`), sử dụng lệnh `cp` để đưa file vào đúng vị trí.

```bash
# Thay thế đường dẫn /tmp/asr.csv bằng đường dẫn thực tế chứa file
cp /tmp/asr.csv "$WORKSPACE_ROOT/"
```

Sau khi file đã nằm đúng vị trí (biến `$INPUT_CSV` đã được trỏ vào đây từ bước 0.2), tiến hành tạo các thư mục đầu ra và đẩy file CSV này lên hệ thống file phân tán HDFS:

Thêm các biến môi trường thư mục vào `~/.bashrc` để hệ thống tự động nhận diện trong các lần mở terminal sau, sau đó nạp lại cấu hình:

```bash
cat >> ~/.bashrc <<'EOF'
export OUTPUT_DIR="${LAB3_ROOT}/outputs"
export EVIDENCE_DIR="${LAB3_ROOT}/docs/evidence"
export HDFS_ROOT="/user/${USER_NAME}/lab3"
EOF

source ~/.bashrc
```

Sau khi file đã nằm đúng vị trí và các biến được nạp, tiến hành tạo các thư mục đầu ra cục bộ và đẩy file CSV lên hệ thống file phân tán HDFS:

```bash
cd "$LAB3_ROOT"

mkdir -p "$OUTPUT_DIR" "$EVIDENCE_DIR"
hdfs dfs -mkdir -p "$HDFS_ROOT/input"
hdfs dfs -put -f "$INPUT_CSV" "$HDFS_ROOT/input/amazon-sales.csv"
hdfs dfs -ls "$HDFS_ROOT/input"
hdfs dfs -du -h "$HDFS_ROOT/input/amazon-sales.csv"
```

Mục tiêu: Đảm bảo file dữ liệu đã sẵn sàng trên workspace cục bộ, đồng thời thiết lập các đường dẫn và đưa CSV vào HDFS để Task 1-1 và Task 1-2 (MapReduce) có thể đọc lại nhiều lần mà không bị lỗi thiếu đường dẫn.

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

## 12. Kiểm tra kết quả và đóng gói nộp bài

Sau khi chạy hoàn tất Task 1-1, Task 1-2, Task 2-1 và Task 2-2, hãy kiểm tra thư mục `outputs`:

```bash
cd "$LAB3_ROOT"
tree "$OUTPUT_DIR"
```

**Kết quả mong đợi:**
```text
outputs/
├── Task_1-1.csv
├── Task_1-2.csv
├── Task_2-1.parquet
└── Task_2-2.parquet
```

Bốn kết quả cuối cùng tương ứng với:
* **Task_1-1.csv**: Kết quả của bài toán MapReduce Task 1-1.
* **Task_1-2.csv**: Kết quả của bài toán MapReduce Task 1-2.
* **Task_2-1.parquet**: Kết quả xử lý bằng Spark cho Task 2-1.
* **Task_2-2.parquet**: Kết quả xử lý bằng Spark cho Task 2-2.

Tiếp theo, kiểm tra thư mục `evidence`:

```bash
tree "$EVIDENCE_DIR"
```

**Kết quả mong đợi (có evidence cho cả Task 2-1 và Task 2-2):**
```text
evidence/
├── task21/
│   ├── execution-summary.txt
│   └── extended-plan.txt
└── task22/
    ├── benchmark-samples.csv
    ├── benchmark-summary.csv
    ├── execution-summary.txt
    ├── extended-plan.txt
    ├── group-profile.txt
    ├── set-difference-examples/
    ├── set-difference-summary/
    └── threshold-deltas/
```

Có thể chạy lại bước validation để xác nhận toàn bộ 4 output:

```bash
spark-submit --master local[2] \
  --class lab3.io.ValidationMain \
  target/scala-2.11/bigdata-lab3.jar \
  --output-dir "$OUTPUT_DIR"
```

Nếu validation hoàn tất không có lỗi, có thể xem như bốn kết quả đầu ra đã được tạo đầy đủ.

## 13. Xử lý lỗi môi trường

- Nếu shell báo không nhận ra `java`, `scala`, `sbt`, `hadoop` hoặc `spark-submit`, nghĩa là toolchain Lab 1 chưa được nạp vào môi trường hiện tại. Hãy mở đúng WSL/Ubuntu và kiểm tra lại `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME` và `PATH`.
- Nếu chưa có lệnh `sbt` nhưng đã có một JDK 8 chạy được, bạn vẫn có thể boot SBT bằng `java -jar sbt-launch.jar ...` để chạy `clean test assembly`.
- Nếu đang ở Windows PowerShell mà WSL chưa cài, các lệnh trong tài liệu này sẽ không chạy trực tiếp; tài liệu này giả định môi trường WSL như phần mở đầu đã nêu.
