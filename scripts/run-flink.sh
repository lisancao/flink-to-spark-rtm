#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# run-flink.sh -- Submit a Flink job for a given pipeline pattern
#
# Locates the shaded JAR under pipelines/NN-xxx/flink/target/ and submits it
# to the Flink cluster via `flink run`.
#
# Usage:
#   ./scripts/run-flink.sh <pattern-number>
#
# Examples:
#   ./scripts/run-flink.sh 01     # stateless passthrough
#   ./scripts/run-flink.sh 02     # tumbling window count
#   ./scripts/run-flink.sh 03     # sliding window average
#   ./scripts/run-flink.sh 04     # keyed state counter
#
# Environment variables:
#   FLINK_HOME              Path to Flink installation (default: /opt/flink)
#   KAFKA_BOOTSTRAP_SERVERS Kafka broker address      (default: kafka:9092)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
FLINK_HOME="${FLINK_HOME:-/opt/flink}"
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
# Map pattern number to directory name and artifact ID
# ---------------------------------------------------------------------------
declare -A PATTERN_DIRS=(
    ["01"]="01-stateless-passthrough"
    ["02"]="02-tumbling-window"
    ["03"]="03-sliding-window"
    ["04"]="04-keyed-state"
)

declare -A ARTIFACT_IDS=(
    ["01"]="stateless-passthrough"
    ["02"]="tumbling-window-count"
    ["03"]="sliding-window-avg"
    ["04"]="keyed-state-counter"
)

if [[ -z "${PATTERN_DIRS[$PATTERN_NUM]+_}" ]]; then
    echo "Error: Invalid pattern number '${PATTERN_NUM}'. Must be 01, 02, 03, or 04."
    exit 1
fi

PATTERN_DIR="${PATTERN_DIRS[$PATTERN_NUM]}"
ARTIFACT_ID="${ARTIFACT_IDS[$PATTERN_NUM]}"
VERSION="1.0.0"

# ---------------------------------------------------------------------------
# Locate the shaded uber-JAR
# ---------------------------------------------------------------------------
JAR_PATH="${REPO_ROOT}/pipelines/${PATTERN_DIR}/flink/target/${ARTIFACT_ID}-${VERSION}.jar"

if [[ ! -f "${JAR_PATH}" ]]; then
    echo "Error: JAR not found at ${JAR_PATH}"
    echo "       Run scripts/setup.sh first to build the Flink JARs."
    exit 1
fi

# ---------------------------------------------------------------------------
# Submit the Flink job
# ---------------------------------------------------------------------------
echo "==> Submitting Flink job for pattern ${PATTERN_NUM} (${PATTERN_DIR})"
echo "    JAR:                     ${JAR_PATH}"
echo "    FLINK_HOME:              ${FLINK_HOME}"
echo "    KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}"
echo ""

"${FLINK_HOME}/bin/flink" run "${JAR_PATH}"
