#!/usr/bin/env bash
# Export Spark's small CSV result directories for the Windows dashboard.
set -euo pipefail
MODE="${1:-demo}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HDFS_ROOT="${HDFS_ROOT:-/user/${USER}/instacart}"
OUT="$HDFS_ROOT/out-$MODE"
DEST="$PROJECT_DIR/outputs"
mkdir -p "$DEST"
for name in analytics quality predictions; do
  if [ -e "$DEST/$name" ]; then
    echo "Export target exists: $DEST/$name. Move it before exporting again." >&2
    exit 1
  fi
  hdfs dfs -get "$OUT/$name" "$DEST/"
done
tar -czf "$PROJECT_DIR/instacart-results.tar.gz" -C "$PROJECT_DIR" outputs
echo "Created $PROJECT_DIR/instacart-results.tar.gz"
