#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# run-benchmarks.sh -- Flink vs Spark RTM latency benchmark runner
#
# Runs all 4 pipeline patterns on both Flink and Spark, measuring end-to-end
# latency for each.  For every (engine, pattern) combination the script:
#
#   1. Starts the data generator (Kafka producer)
#   2. Starts the pipeline (Flink jar or Spark submit)
#   3. Runs the latency consumer for --duration seconds
#   4. Tears everything down and saves results
#
# Results are written to benchmarks/results/ as individual text files.
#
# Prerequisites:
#   - Kafka broker running and reachable
#   - Flink cluster running (flink CLI on PATH) with fat JARs already built
#   - Spark cluster running (spark-submit on PATH)
#   - Python 3 with kafka-python-ng installed
#   - Kafka topics "benchmark-input" and "benchmark-output" must exist
#
# Usage:
#   ./run-benchmarks.sh [OPTIONS]
#
# Options:
#   --bootstrap-servers HOST:PORT   Kafka bootstrap servers (default: localhost:9092)
#   --duration SECONDS              How long to measure each pipeline (default: 60)
#   --rate EVENTS_PER_SEC           Producer rate (default: 10000)
#   --keys NUM_KEYS                 Number of unique user keys (default: 100)
#   --patterns PATTERN_LIST         Comma-separated list of patterns to run,
#                                   e.g. "01,03" (default: 01,02,03,04)
#   --engines ENGINE_LIST           Comma-separated list of engines to test,
#                                   e.g. "spark" (default: flink,spark)
#   --help                          Show this help message
#
# Examples:
#   # Run all benchmarks with defaults
#   ./run-benchmarks.sh
#
#   # Only run pattern 01 on Spark, 30-second duration
#   ./run-benchmarks.sh --patterns 01 --engines spark --duration 30
#
#   # Custom Kafka broker and high producer rate
#   ./run-benchmarks.sh --bootstrap-servers kafka:9092 --rate 50000
# ============================================================================

# ── repo root (one level up from benchmarks/) ────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── defaults ─────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS="localhost:9092"
DURATION=60
RATE=10000
KEYS=100
PATTERNS="01,02,03,04"
ENGINES="flink,spark"
RESULTS_DIR="${SCRIPT_DIR}/results"

# ── pattern metadata ─────────────────────────────────────────────────────────
# Maps pattern number to directory name, Flink main class, and Spark script
declare -A PATTERN_DIRS=(
    [01]="01-stateless-passthrough"
    [02]="02-tumbling-window"
    [03]="03-sliding-window"
    [04]="04-keyed-state"
)

declare -A PATTERN_NAMES=(
    [01]="Stateless Passthrough"
    [02]="Tumbling Window Count"
    [03]="Sliding Window Average"
    [04]="Keyed State Counter"
)

declare -A FLINK_MAIN_CLASS=(
    [01]="com.example.StatelessPassthrough"
    [02]="com.example.TumblingWindowCount"
    [03]="com.example.SlidingWindowAvg"
    [04]="com.example.KeyedStateCounter"
)

declare -A SPARK_SCRIPTS=(
    [01]="spark/stateless_rtm.py"
    [02]="spark/tumbling_count.py"
    [03]="spark/sliding_avg.py"
    [04]="spark/stateful_counter.py"
)

# ── parse arguments ──────────────────────────────────────────────────────────
usage() {
    # Print the header comment block from this script as usage text
    sed -n '/^# Usage:/,/^# ====/p' "${BASH_SOURCE[0]}" | sed 's/^# //' | head -n -1
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bootstrap-servers)
            BOOTSTRAP_SERVERS="$2"; shift 2 ;;
        --duration)
            DURATION="$2"; shift 2 ;;
        --rate)
            RATE="$2"; shift 2 ;;
        --keys)
            KEYS="$2"; shift 2 ;;
        --patterns)
            PATTERNS="$2"; shift 2 ;;
        --engines)
            ENGINES="$2"; shift 2 ;;
        --help|-h)
            usage ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run with --help for usage." >&2
            exit 1 ;;
    esac
done

# Convert comma-separated lists to arrays
IFS=',' read -ra PATTERN_LIST <<< "$PATTERNS"
IFS=',' read -ra ENGINE_LIST <<< "$ENGINES"

# ── helpers ──────────────────────────────────────────────────────────────────

# Timestamp for log lines
ts() {
    date "+%H:%M:%S"
}

log() {
    echo "[$(ts)] $*"
}

# Track background PIDs so we can clean up on failure
PIDS_TO_KILL=()

cleanup() {
    log "Cleaning up background processes..."
    for pid in "${PIDS_TO_KILL[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    PIDS_TO_KILL=()
}

trap cleanup EXIT

# Start the data generator in the background.
# Sets PRODUCER_PID as a side effect.
start_producer() {
    log "  Starting producer: rate=${RATE}, keys=${KEYS}, topic=benchmark-input"
    python3 "${REPO_ROOT}/data_generator/producer.py" \
        --rate "$RATE" \
        --keys "$KEYS" \
        --topic benchmark-input \
        --bootstrap-servers "$BOOTSTRAP_SERVERS" \
        > /dev/null 2>&1 &
    PRODUCER_PID=$!
    PIDS_TO_KILL+=("$PRODUCER_PID")
    # Give the producer a moment to start sending
    sleep 2
}

# Stop the producer.
stop_producer() {
    if [[ -n "${PRODUCER_PID:-}" ]] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
        log "  Stopping producer (PID ${PRODUCER_PID})"
        kill "$PRODUCER_PID" 2>/dev/null || true
        wait "$PRODUCER_PID" 2>/dev/null || true
    fi
}

# Run the latency consumer for $DURATION seconds, saving output to a file.
# Args: $1 = output file path
measure_latency() {
    local outfile="$1"
    log "  Measuring latency for ${DURATION}s -> ${outfile}"

    python3 "${SCRIPT_DIR}/measure-latency.py" \
        --topic benchmark-output \
        --bootstrap-servers "$BOOTSTRAP_SERVERS" \
        --duration "$DURATION" \
        2>&1 | tee "$outfile"
}

# ── Flink pipeline runner ────────────────────────────────────────────────────
# Starts a Flink job, waits for latency measurement, then cancels the job.
run_flink_pipeline() {
    local pattern="$1"
    local pattern_dir="${PATTERN_DIRS[$pattern]}"
    local main_class="${FLINK_MAIN_CLASS[$pattern]}"
    local jar_path="${REPO_ROOT}/pipelines/${pattern_dir}/flink/target/${pattern_dir}-1.0.0.jar"

    if [[ ! -f "$jar_path" ]]; then
        log "  WARNING: Flink JAR not found at ${jar_path}"
        log "  Attempting to find shaded JAR..."
        # Try the shade plugin's output naming convention
        jar_path=$(find "${REPO_ROOT}/pipelines/${pattern_dir}/flink/target/" \
            -name "*.jar" -not -name "original-*" 2>/dev/null | head -1)
        if [[ -z "$jar_path" || ! -f "$jar_path" ]]; then
            log "  SKIP: No Flink JAR found. Build with: cd pipelines/${pattern_dir}/flink && mvn package"
            return 1
        fi
    fi

    log "  Starting Flink job: ${main_class}"
    export KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS"
    export INPUT_TOPIC="benchmark-input"
    export OUTPUT_TOPIC="benchmark-output"

    # Submit the Flink job (runs in the background on the Flink cluster)
    local flink_output
    flink_output=$(flink run -d -c "$main_class" "$jar_path" 2>&1) || {
        log "  SKIP: flink run failed: ${flink_output}"
        return 1
    }

    # Extract the Flink job ID so we can cancel it later
    local job_id
    job_id=$(echo "$flink_output" | grep -oP 'JobID \K[a-f0-9]+' || true)
    log "  Flink job submitted: ${job_id:-unknown}"

    # Measure latency
    local outfile="${RESULTS_DIR}/${pattern}-flink-latency.txt"
    measure_latency "$outfile"

    # Cancel the Flink job
    if [[ -n "$job_id" ]]; then
        log "  Cancelling Flink job ${job_id}"
        flink cancel "$job_id" 2>/dev/null || true
    fi
}

# ── Spark pipeline runner ────────────────────────────────────────────────────
# Starts a Spark Structured Streaming job, waits for latency measurement,
# then kills the spark-submit process.
run_spark_pipeline() {
    local pattern="$1"
    local pattern_dir="${PATTERN_DIRS[$pattern]}"
    local spark_script="${REPO_ROOT}/pipelines/${pattern_dir}/${SPARK_SCRIPTS[$pattern]}"

    if [[ ! -f "$spark_script" ]]; then
        log "  SKIP: Spark script not found at ${spark_script}"
        return 1
    fi

    log "  Starting Spark pipeline: ${spark_script}"
    export KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS"

    spark-submit "$spark_script" > /dev/null 2>&1 &
    local spark_pid=$!
    PIDS_TO_KILL+=("$spark_pid")

    # Give Spark time to initialize and connect to Kafka
    sleep 10

    # Measure latency
    local outfile="${RESULTS_DIR}/${pattern}-spark-latency.txt"
    measure_latency "$outfile"

    # Stop the Spark job
    if kill -0 "$spark_pid" 2>/dev/null; then
        log "  Stopping Spark job (PID ${spark_pid})"
        kill "$spark_pid" 2>/dev/null || true
        wait "$spark_pid" 2>/dev/null || true
    fi
}

# ── main ─────────────────────────────────────────────────────────────────────

echo "============================================================"
echo "  flink-to-spark-rtm benchmark suite"
echo "============================================================"
echo "  Bootstrap servers : ${BOOTSTRAP_SERVERS}"
echo "  Duration per run  : ${DURATION}s"
echo "  Producer rate     : ${RATE} events/sec"
echo "  Producer keys     : ${KEYS}"
echo "  Patterns          : ${PATTERNS}"
echo "  Engines           : ${ENGINES}"
echo "  Results dir       : ${RESULTS_DIR}"
echo "============================================================"
echo ""

mkdir -p "$RESULTS_DIR"

# Iterate: for each pattern, run each requested engine
for pattern in "${PATTERN_LIST[@]}"; do
    pattern_name="${PATTERN_NAMES[$pattern]:-Pattern $pattern}"
    echo ""
    echo "------------------------------------------------------------"
    log "Pattern ${pattern}: ${pattern_name}"
    echo "------------------------------------------------------------"

    for engine in "${ENGINE_LIST[@]}"; do
        echo ""
        log ">>> Running ${engine} for pattern ${pattern} (${pattern_name})"

        # Start the producer
        start_producer

        # Run the pipeline + latency measurement
        case "$engine" in
            flink)
                run_flink_pipeline "$pattern" || {
                    log "  Flink pipeline failed or skipped"
                    stop_producer
                    continue
                }
                ;;
            spark)
                run_spark_pipeline "$pattern" || {
                    log "  Spark pipeline failed or skipped"
                    stop_producer
                    continue
                }
                ;;
            *)
                log "  Unknown engine: ${engine}, skipping"
                stop_producer
                continue
                ;;
        esac

        # Stop the producer
        stop_producer
        log "<<< Finished ${engine} for pattern ${pattern}"
    done
done

echo ""
echo "============================================================"
log "All benchmarks complete. Results saved to: ${RESULTS_DIR}/"
echo "============================================================"

# List result files
echo ""
log "Result files:"
ls -1 "${RESULTS_DIR}"/*.txt 2>/dev/null | while read -r f; do
    echo "  $(basename "$f")"
done
