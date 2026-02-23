#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# run-spark-rtm.sh -- Submit a Spark Real-Time Mode pipeline for a given
#                     pattern number
#
# Locates the Python file under pipelines/NN-xxx/spark/ and submits it via
# spark-submit with the appropriate configuration for Real-Time Mode.
#
# Usage:
#   ./scripts/run-spark-rtm.sh <pattern-number>
#
# Examples:
#   ./scripts/run-spark-rtm.sh 01     # stateless passthrough
#   ./scripts/run-spark-rtm.sh 02     # tumbling window count
#   ./scripts/run-spark-rtm.sh 03     # sliding window average
#   ./scripts/run-spark-rtm.sh 04     # keyed state counter
#
# Environment variables:
#   SPARK_MASTER            Spark master URL          (default: spark://spark-master:7077)
#   KAFKA_BOOTSTRAP_SERVERS Kafka broker address      (default: kafka:9092)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SPARK_MASTER="${SPARK_MASTER:-spark://spark:7077}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

# ---------------------------------------------------------------------------
# Validate arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <pattern-number>  (01 | 02 | 03 | 04)"
    exit 1
fi

PATTERN_NUM="$1"

# ---------------------------------------------------------------------------
# Map pattern number to directory name and Python filename
# ---------------------------------------------------------------------------
declare -A PATTERN_DIRS=(
    ["01"]="01-stateless-passthrough"
    ["02"]="02-tumbling-window"
    ["03"]="03-sliding-window"
    ["04"]="04-keyed-state"
)

declare -A PY_FILES=(
    ["01"]="stateless_rtm.py"
    ["02"]="tumbling_count.py"
    ["03"]="sliding_avg.py"
    ["04"]="stateful_counter.py"
)

if [[ -z "${PATTERN_DIRS[$PATTERN_NUM]+_}" ]]; then
    echo "Error: Invalid pattern number '${PATTERN_NUM}'. Must be 01, 02, 03, or 04."
    exit 1
fi

PATTERN_DIR="${PATTERN_DIRS[$PATTERN_NUM]}"
PY_FILE="${PY_FILES[$PATTERN_NUM]}"

# ---------------------------------------------------------------------------
# Locate the Python pipeline script
# ---------------------------------------------------------------------------
PY_PATH="${REPO_ROOT}/pipelines/${PATTERN_DIR}/spark/${PY_FILE}"

if [[ ! -f "${PY_PATH}" ]]; then
    echo "Error: Python file not found at ${PY_PATH}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Submit the Spark Real-Time Mode pipeline
# ---------------------------------------------------------------------------
echo "==> Submitting Spark RTM pipeline for pattern ${PATTERN_NUM} (${PATTERN_DIR})"
echo "    Script:                  ${PY_PATH}"
echo "    SPARK_MASTER:            ${SPARK_MASTER}"
echo "    KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "    shuffle.partitions:      20"
echo ""

spark-submit \
    --master "${SPARK_MASTER}" \
    --conf "spark.sql.shuffle.partitions=20" \
    "${PY_PATH}"
