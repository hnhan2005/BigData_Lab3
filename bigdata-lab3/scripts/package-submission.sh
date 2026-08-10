#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --representative-id ID --drive-url HTTPS_URL --outputs DIR --jar JAR [--overwrite]" >&2
}

OVERWRITE=false
declare -A OPTS
while [[ $# -gt 0 ]]; do
  case "$1" in
    --overwrite) OVERWRITE=true; shift ;;
    --representative-id|--drive-url|--outputs|--jar)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      OPTS[${1#--}]=$2; shift 2 ;;
    *) usage; exit 2 ;;
  esac
done

for key in representative-id drive-url outputs jar; do
  [[ -n "${OPTS[$key]:-}" ]] || { echo "Thiếu --$key" >&2; exit 2; }
done
ID=${OPTS[representative-id]}
URL=${OPTS[drive-url]}
[[ "$ID" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "Representative ID không an toàn" >&2; exit 2; }
[[ "$URL" =~ ^https://[^[:space:]]+$ && "$URL" != *PLACEHOLDER* ]] || { echo "Drive URL phải là một HTTPS URL thật" >&2; exit 2; }
for name in Task_1-1.csv Task_1-2.csv Task_2-1.parquet Task_2-2.parquet; do
  [[ -f "${OPTS[outputs]}/$name" ]] || { echo "Thiếu output Drive: $name" >&2; exit 2; }
done
[[ -f docs/Report.pdf && -f docs/README.md && -f "${OPTS[jar]}" ]] || { echo "Thiếu Report/README/JAR" >&2; exit 2; }

bash scripts/validate-outputs.sh "${OPTS[jar]}" "${OPTS[outputs]}"
ZIP="$PWD/$ID.zip"
if [[ -e "$ZIP" && "$OVERWRITE" != true ]]; then
  echo "$ZIP đã tồn tại; dùng --overwrite" >&2; exit 2
fi

STAGE=$(mktemp -d)
trap 'rm -rf -- "$STAGE"' EXIT
ROOT="$STAGE/$ID"
mkdir -p "$ROOT/project" "$ROOT/docs"
cp build.sbt "$ROOT/"
cp project/build.properties project/plugins.sbt "$ROOT/project/"
cp -R src "$ROOT/"
cp docs/README.md docs/Report.md docs/Report.pdf "$ROOT/docs/"
printf '%s\n' "$URL" > "$ROOT/docs/drive_link.txt"
if [[ -d docs/evidence ]]; then cp -R docs/evidence "$ROOT/docs/"; fi

rm -f -- "$ZIP"
(cd "$STAGE" && zip -qr "$ZIP" "$ID")
echo "[OK] Đã tạo $ZIP; Drive folder $ID phải chứa trực tiếp đúng bốn output đã validate."
