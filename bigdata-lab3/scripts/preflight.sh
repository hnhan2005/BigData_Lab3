#!/usr/bin/env bash
set -euo pipefail

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] Không tìm thấy lệnh '$1' trên PATH." >&2
    exit 2
  fi
}

for command_name in java scala hadoop hdfs yarn spark-submit sbt; do
  require_command "$command_name"
done

java_version="$(java -version 2>&1 | head -n 1)"
scala_version="$(scala -version 2>&1 | head -n 1)"
hadoop_version="$(hadoop version 2>&1 | head -n 1)"
spark_version="$(spark-submit --version 2>&1)"
sbt_version="$(sbt --version 2>&1 | head -n 1)"

case "$java_version" in
  *'1.8.'*) ;;
  *) echo "[ERROR] Cần Java 8, nhận được: $java_version" >&2; exit 2 ;;
esac

case "$scala_version" in
  *'2.11.12'*) ;;
  *) echo "[ERROR] Cần Scala 2.11.12, nhận được: $scala_version" >&2; exit 2 ;;
esac

case "$hadoop_version" in
  *'3.3.6'*) ;;
  *) echo "[ERROR] Cần Hadoop 3.3.6, nhận được: $hadoop_version" >&2; exit 2 ;;
esac

case "$spark_version" in
  *'version 2.4.8'*|*'version 2.4.8'*) ;;
  *) echo "[ERROR] Baseline cần Spark 2.4.8. Hãy cập nhật Detailed Design trước khi dùng version khác." >&2; exit 2 ;;
esac

case "$spark_version" in
  *'Scala version 2.11'*) ;;
  *) echo "[ERROR] Spark phải được build cho Scala 2.11." >&2; exit 2 ;;
esac

echo "[OK] $java_version"
echo "[OK] $scala_version"
echo "[OK] $hadoop_version"
echo "[OK] Spark 2.4.8 / Scala 2.11"
echo "[OK] $sbt_version"

echo "[INFO] Kiểm tra HDFS..."
hdfs dfs -ls / >/dev/null
echo "[OK] HDFS truy cập được."

echo "[INFO] Kiểm tra YARN..."
yarn node -list >/dev/null
echo "[OK] YARN truy cập được."
