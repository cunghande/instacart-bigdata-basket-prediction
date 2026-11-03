#!/usr/bin/env bash
# Run on the Ubuntu Master after Spark, Hadoop/YARN and SSH are configured.
set -euo pipefail

MODE="${1:-demo}"
case "$MODE" in
  demo) SAMPLE_MOD=100; SHUFFLE_PARTITIONS=16 ;;
  full) SAMPLE_MOD=1; SHUFFLE_PARTITIONS=64 ;;
  *) echo "Usage: $0 [demo|full]" >&2; exit 2 ;;
esac

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HDFS_ROOT="${HDFS_ROOT:-/user/${USER}/instacart}"
RAW_DIR="${RAW_DIR:-${PROJECT_DIR}/data/raw}"
mkdir -p "${PROJECT_DIR}/outputs"

for command in hdfs spark-submit; do
  command -v "$command" >/dev/null || { echo "$command is not installed or not in PATH" >&2; exit 1; }
done
for file in aisles.csv departments.csv products.csv orders.csv order_products__prior.csv order_products__train.csv; do
  test -f "$RAW_DIR/$file" || { echo "Missing $RAW_DIR/$file" >&2; exit 1; }
done

hdfs dfs -mkdir -p "$HDFS_ROOT/raw"
for file in aisles.csv departments.csv products.csv orders.csv order_products__prior.csv order_products__train.csv; do
  if ! hdfs dfs -test -e "$HDFS_ROOT/raw/$file"; then
    hdfs dfs -Ddfs.replication=1 -put "$RAW_DIR/$file" "$HDFS_ROOT/raw/$file"
  fi
done

SPARK=(spark-submit --master yarn --deploy-mode client
  --driver-memory 1g --executor-memory 512m --executor-cores 1
  --num-executors 2 --conf spark.executor.memoryOverhead=384
  --conf spark.hadoop.dfs.replication=1
  --conf spark.driver.host=master --conf spark.driver.bindAddress=0.0.0.0
  --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS")
APP="$PROJECT_DIR/src/instacart_bigdata/pipeline.py"
STAGE="$HDFS_ROOT/stage-$MODE"
OUT="$HDFS_ROOT/out-$MODE"

"${SPARK[@]}" "$APP" prepare --raw "$HDFS_ROOT/raw" --stage "$STAGE" --sample-mod "$SAMPLE_MOD"
"${SPARK[@]}" "$APP" quality --stage "$STAGE" --output "$OUT"
"${SPARK[@]}" "$APP" analytics --stage "$STAGE" --output "$OUT/analytics"
"${SPARK[@]}" "$APP" train --stage "$STAGE" --model "$HDFS_ROOT/model-$MODE" \
  --metrics "$PROJECT_DIR/outputs/metrics.json"
"${SPARK[@]}" "$APP" score --stage "$STAGE" --model "$HDFS_ROOT/model-$MODE" \
  --output "$OUT/predictions"

echo "Done. HDFS output: $OUT"
echo "Model metrics: $PROJECT_DIR/outputs/metrics.json"
