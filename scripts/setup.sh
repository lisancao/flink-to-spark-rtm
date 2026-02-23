#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# setup.sh -- Bootstrap the benchmark environment
#
# Creates the three Kafka topics used by every pipeline pattern, then builds
# the Flink uber-JARs for all four patterns with Maven.
#
# Usage:
#   ./scripts/setup.sh [--bootstrap-server <host:port>]
#
# Options:
#   --bootstrap-server   Kafka bootstrap server (default: kafka:9092)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVER="kafka:9092"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bootstrap-server)
            BOOTSTRAP_SERVER="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--bootstrap-server <host:port>]"
            exit 1
            ;;
    esac
done

echo "==> Bootstrap server: ${BOOTSTRAP_SERVER}"

# ---------------------------------------------------------------------------
# 1. Create Kafka topics (16 partitions each)
# ---------------------------------------------------------------------------
TOPICS=("benchmark-input" "benchmark-output" "benchmark-latency")
PARTITIONS=16
REPLICATION_FACTOR=1

echo ""
echo "==> Creating Kafka topics with ${PARTITIONS} partitions ..."

for TOPIC in "${TOPICS[@]}"; do
    echo "    - ${TOPIC}"
    kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --if-not-exists
done

echo "==> Kafka topics ready."

# ---------------------------------------------------------------------------
# 2. Build Flink JARs for all four pipeline patterns
# ---------------------------------------------------------------------------
PATTERNS=(
    "01-stateless-passthrough"
    "02-tumbling-window"
    "03-sliding-window"
    "04-keyed-state"
)

echo ""
echo "==> Building Flink JARs for ${#PATTERNS[@]} pipeline patterns ..."

for PATTERN in "${PATTERNS[@]}"; do
    FLINK_DIR="${REPO_ROOT}/pipelines/${PATTERN}/flink"

    if [[ ! -f "${FLINK_DIR}/pom.xml" ]]; then
        echo "    [SKIP] No pom.xml found in ${FLINK_DIR}"
        continue
    fi

    echo "    [BUILD] ${PATTERN} ..."
    mvn -f "${FLINK_DIR}/pom.xml" clean package -q -DskipTests
    echo "    [OK]    ${PATTERN}"
done

echo ""
echo "==> Setup complete. All topics created and Flink JARs built."
