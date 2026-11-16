#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BASE/conf/cluster.env"

if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    echo "Spark not found at: $SPARK_HOME"
    exit 1
fi

RUN_ID="demo-$(date -u +%Y%m%dT%H%M%SZ)-$$"

DEMO_ROOT="$BASE/demo-runs/$RUN_ID"
OUTPUT="file://${DEMO_ROOT}/export"
CHECKPOINT="file://${DEMO_ROOT}/checkpoints"

mkdir -p "$DEMO_ROOT" "$BASE/logs"

"$SPARK_HOME/bin/spark-submit" \
  --master "local[2]" \
  --deploy-mode client \
  "$BASE/jobs/build_search_export.py" \
  --demo \
  --output "$OUTPUT" \
  --checkpoint "$CHECKPOINT" \
  --iterations 10 \
  --damping 0.85 \
  --partitions 4 \
  2>&1 | tee "$BASE/logs/${RUN_ID}.log"

echo
echo "Demo completed successfully."
echo "Export directory: ${DEMO_ROOT}/export"
