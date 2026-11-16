#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BASE/conf/cluster.env"

MODE="${1:?Usage: run_cluster.sh inspect|build PHASE SNAPSHOT_ID}"
PHASE="${2:?Usage: run_cluster.sh inspect|build PHASE SNAPSHOT_ID}"
SNAPSHOT_ID="${3:?Usage: run_cluster.sh inspect|build PHASE SNAPSHOT_ID}"

if [[ "$MODE" != "inspect" && "$MODE" != "build" ]]; then
    echo "Mode must be inspect or build"
    exit 1
fi

if [[ ! "$PHASE" =~ ^[A-Za-z0-9_-]+$ ]]; then
    echo "Invalid phase name"
    exit 1
fi

: "${VM3_IP:?Fill VM3_IP in conf/cluster.env}"
: "${HDFS_URI:?Fill HDFS_URI in conf/cluster.env}"
: "${WAREHOUSE:?Fill WAREHOUSE in conf/cluster.env}"
: "${CATALOG:?Set CATALOG in conf/cluster.env}"
: "${DOCS_TABLE:?Set DOCS_TABLE in conf/cluster.env}"

MASTER_URL="spark://${VM3_IP}:7077"

COMMON_ARGS=(
  --master "$MASTER_URL"
  --deploy-mode client
  --conf "spark.driver.host=${VM3_IP}"
  --conf "spark.driver.bindAddress=${VM3_IP}"
  --conf "spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${CATALOG}.type=hadoop"
  --conf "spark.sql.catalog.${CATALOG}.warehouse=${WAREHOUSE}"
)

if [[ -n "$ICEBERG_PACKAGE" ]]; then
    COMMON_ARGS+=(--packages "$ICEBERG_PACKAGE")
fi

if [[ "$MODE" == "inspect" ]]; then
    "$SPARK_HOME/bin/spark-submit" \
      "${COMMON_ARGS[@]}" \
      "$BASE/jobs/inspect_input.py" \
      --table "$DOCS_TABLE" \
      --snapshot-id "$SNAPSHOT_ID"

    exit 0
fi

RUN_ID="${PHASE}-$(date -u +%Y%m%dT%H%M%SZ)-$$"

EXPORT_ROOT="${HDFS_URI%/}/search/exports"
CHECKPOINT_ROOT="${HDFS_URI%/}/search/checkpoints"

STAGING="${EXPORT_ROOT}/_staging/${RUN_ID}"
FINAL="${EXPORT_ROOT}/${RUN_ID}"
CHECKPOINT="${CHECKPOINT_ROOT}/${RUN_ID}"

mkdir -p "$BASE/logs"

hdfs dfs -mkdir -p "${EXPORT_ROOT}/_staging"
hdfs dfs -mkdir -p "$CHECKPOINT_ROOT"

"$SPARK_HOME/bin/spark-submit" \
  "${COMMON_ARGS[@]}" \
  "$BASE/jobs/build_search_export.py" \
  --table "$DOCS_TABLE" \
  --snapshot-id "$SNAPSHOT_ID" \
  --output "$STAGING" \
  --checkpoint "$CHECKPOINT" \
  --iterations 10 \
  --damping 0.85 \
  --partitions 8 \
  2>&1 | tee "$BASE/logs/${RUN_ID}.log"

hdfs dfs -test -e "${STAGING}/_READY"

# Publish only after successful Spark execution and validation.
# Both paths must remain on the same HDFS filesystem.
hdfs dfs -mv "$STAGING" "$FINAL"

echo
echo "PUBLISHED_EXPORT=$FINAL"
echo "RUN_ID=$RUN_ID"
