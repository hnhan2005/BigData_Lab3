#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --jar JAR --input-local CSV --hdfs-input PATH --hdfs-work PATH --output-dir DIR --evidence-dir DIR [--overwrite]" >&2
}

OVERWRITE=false
declare -A OPTS
while [[ $# -gt 0 ]]; do
  case "$1" in
    --overwrite) OVERWRITE=true; shift ;;
    --jar|--input-local|--hdfs-input|--hdfs-work|--output-dir|--evidence-dir)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      OPTS[${1#--}]=$2; shift 2 ;;
    *) usage; exit 2 ;;
  esac
done

for key in jar input-local hdfs-input hdfs-work output-dir evidence-dir; do
  [[ -n "${OPTS[$key]:-}" ]] || { echo "Thiếu --$key" >&2; exit 2; }
done
[[ -f "${OPTS[jar]}" ]] || { echo "Không tìm thấy JAR" >&2; exit 2; }
[[ -f "${OPTS[input-local]}" ]] || { echo "Không tìm thấy CSV" >&2; exit 2; }

WORK=${OPTS[hdfs-work]%/}
[[ "$WORK" != "/" && "$WORK" == */*/* ]] || { echo "--hdfs-work quá rộng/không an toàn" >&2; exit 2; }
mkdir -p "${OPTS[output-dir]}" "${OPTS[evidence-dir]}"
"$(dirname "$0")/preflight.sh"

EXTRA=()
if $OVERWRITE; then EXTRA+=(--overwrite); fi
if hdfs dfs -test -e "${OPTS[hdfs-input]}" && ! $OVERWRITE; then
  echo "HDFS input đã tồn tại; dùng --overwrite" >&2; exit 2
fi
if hdfs dfs -test -e "$WORK"; then
  $OVERWRITE || { echo "HDFS work đã tồn tại; dùng --overwrite" >&2; exit 2; }
  hdfs dfs -rm -r -skipTrash "$WORK"
fi
hdfs dfs -mkdir -p "${OPTS[hdfs-input]%/*}"
HDFS_PUT=(-put)
if $OVERWRITE; then HDFS_PUT+=(-f); fi
hdfs dfs "${HDFS_PUT[@]}" "${OPTS[input-local]}" "${OPTS[hdfs-input]}"

hadoop jar "${OPTS[jar]}" lab3.task11.Task11Main --input "${OPTS[hdfs-input]}" --work "$WORK/task11" --output-local "${OPTS[output-dir]}/Task_1-1.csv" "${EXTRA[@]}"
hadoop jar "${OPTS[jar]}" lab3.task12.Task12Main --input "${OPTS[hdfs-input]}" --work "$WORK/task12" --output-local "${OPTS[output-dir]}/Task_1-2.csv" "${EXTRA[@]}"

INPUT_PATH=$(readlink -f "${OPTS[input-local]}")
spark-submit --master local[2] --class lab3.task21.Task21Main "${OPTS[jar]}" --input "$INPUT_PATH" --output-local "${OPTS[output-dir]}/Task_2-1.parquet" --evidence-dir "${OPTS[evidence-dir]}/task21" "${EXTRA[@]}"
spark-submit --master local[2] --class lab3.task22.Task22Main "${OPTS[jar]}" --input "$INPUT_PATH" --output-local "${OPTS[output-dir]}/Task_2-2.parquet" --evidence-dir "${OPTS[evidence-dir]}/task22" --accuracy 10000 --runs 5 "${EXTRA[@]}"

"$(dirname "$0")/validate-outputs.sh" "${OPTS[jar]}" "${OPTS[output-dir]}"
