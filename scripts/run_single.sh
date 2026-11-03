#!/usr/bin/env bash
# Run a complete local Spark demo on Linux/WSL without Hadoop services.
set -euo pipefail
MODE="${1:-fixture}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
case "$MODE" in
  fixture)
    python3 scripts/make_fixture.py --output data/fixture
    RAW=data/fixture; SAMPLE_MOD=1 ;;
  demo)
    python3 scripts/download_data.py --output data/raw
    RAW=data/raw; SAMPLE_MOD=100 ;;
  full)
    python3 scripts/download_data.py --output data/raw
    RAW=data/raw; SAMPLE_MOD=1 ;;
  *) echo "Usage: $0 [fixture|demo|full]" >&2; exit 2 ;;
esac
APP=src/instacart_bigdata/pipeline.py
STAGE="data/stage-$MODE"
OUT=outputs
SPARK=(spark-submit --master 'local[2]' --driver-memory 2g
  --conf spark.sql.shuffle.partitions=24)
"${SPARK[@]}" "$APP" prepare --raw "$RAW" --stage "$STAGE" --sample-mod "$SAMPLE_MOD"
"${SPARK[@]}" "$APP" quality --stage "$STAGE" --output "$OUT"
"${SPARK[@]}" "$APP" analytics --stage "$STAGE" --output "$OUT/analytics"
"${SPARK[@]}" "$APP" train --stage "$STAGE" --model "$OUT/model-$MODE" --metrics "$OUT/metrics.json"
"${SPARK[@]}" "$APP" score --stage "$STAGE" --model "$OUT/model-$MODE" --output "$OUT/predictions"
echo "Dashboard: streamlit run dashboard/app.py"
