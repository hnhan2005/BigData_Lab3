#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <bigdata-lab3.jar> <output-dir>" >&2
  exit 2
fi

JAR_PATH=$1
OUTPUT_DIR=$2
[[ -f "$JAR_PATH" ]] || { echo "Không tìm thấy JAR: $JAR_PATH" >&2; exit 2; }
[[ -d "$OUTPUT_DIR" ]] || { echo "Không tìm thấy output dir: $OUTPUT_DIR" >&2; exit 2; }

spark-submit --master local[2] --class lab3.io.ValidationMain "$JAR_PATH" --output-dir "$OUTPUT_DIR"
