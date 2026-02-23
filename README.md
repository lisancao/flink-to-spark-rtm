# Flink to Spark RTM: Migration Guide

Side-by-side implementations of 4 streaming patterns in **Apache Flink 1.20** and
**Apache Spark 4.1 Real-Time Mode (RTM)**, with latency benchmarks and comprehensive
migration documentation.

## What's Inside

### Pipelines (4 patterns, 2 implementations each)

| # | Pattern | Flink (Java) | Spark RTM (Python) | Fair? |
|---|---------|-------------|-------------------|-------|
| 01 | Stateless passthrough | Kafka → filter → Kafka | readStream → filter → writeStream | Yes |
| 02 | Tumbling window count | keyBy → TumblingWindow(1min) → sum | groupBy + window(1min) → count | Yes |
| 03 | Sliding window avg | keyBy → SlidingWindow(5min,1min) → avg | groupBy + window(5min,1min) → avg | Yes |
| 04 | Keyed state counter | KeyedProcessFunction + ValueState | transformWithState + StatefulProcessor | Semantic diff |

### Documentation

| Document | Description |
|----------|-------------|
| [API Mapping](docs/api-mapping.md) | Flink API → Spark RTM equivalents with code samples |
| [Patterns](docs/patterns.md) | All 4 patterns explained side-by-side with behavioral differences |
| [Limitations](docs/limitations.md) | Honest accounting of what RTM cannot do (yet) |
| [Decision Framework](docs/decision-framework.md) | Flowchart: should you migrate from Flink to RTM? |

### Benchmarks

- `benchmarks/run-benchmarks.sh` — Runs all 4 patterns on both engines
- `benchmarks/measure-latency.py` — Kafka consumer that computes end-to-end latency

## Prerequisites

- Docker and Docker Compose
- Python 3.8+ with `kafka-python-ng` (for data generator and latency measurement)

```bash
pip install kafka-python-ng
```

## Quick Start (Docker)

```bash
# Start Kafka + Flink + Spark
cd docker && docker compose up -d

# Create benchmark topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic benchmark-input --partitions 16 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic benchmark-output --partitions 16 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic benchmark-latency --partitions 16 --if-not-exists

# Start the data generator (separate terminal)
python data_generator/producer.py --rate 10000 --keys 100

# Run a Spark RTM pipeline
docker exec spark spark-submit /opt/spark-data/pipelines/01-stateless-passthrough/spark/stateless_rtm.py

# Or run a Flink pipeline (build first)
cd pipelines/01-stateless-passthrough/flink && mvn package -q
docker exec flink flink run /opt/spark-data/pipelines/01-stateless-passthrough/flink/target/stateless-passthrough-1.0.jar
```

## Quick Start (Existing Infrastructure)

If you already have Spark 4.1, Flink 1.20, and Kafka running:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Setup topics
./scripts/setup.sh $KAFKA_BOOTSTRAP_SERVERS

# Start producer
python data_generator/producer.py --rate 10000 --bootstrap-servers $KAFKA_BOOTSTRAP_SERVERS

# Run Spark pipeline (pattern 01)
./scripts/run-spark-rtm.sh 01

# Run Flink pipeline (pattern 01)
./scripts/run-flink.sh 01
```

## Running Benchmarks

The benchmark script runs all 4 patterns on both engines and captures latency:

```bash
./benchmarks/run-benchmarks.sh --bootstrap-server localhost:9092
```

Or measure a single pipeline manually:

```bash
# Terminal 1: Producer
python data_generator/producer.py --rate 10000

# Terminal 2: Pipeline
docker exec spark spark-submit /opt/spark-data/pipelines/01-stateless-passthrough/spark/stateless_rtm.py

# Terminal 3: Latency measurement
python benchmarks/measure-latency.py --topic benchmark-output --duration 60
```

## Latency Methodology

- Producer embeds `produced_at` (epoch ms) in each JSON event
- External consumer reads from output topic, computes `consumer_wall_clock - produced_at`
- Each run collects 60 seconds of data, reports p50/p95/p99
- 3 runs per configuration, report median p99
- **Caveat**: Single machine, not production-representative, illustrative only

## Project Structure

```
flink-to-spark-rtm/
├── README.md
├── docs/
│   ├── api-mapping.md              # Flink → Spark API reference
│   ├── patterns.md                 # 4 patterns translated side-by-side
│   ├── limitations.md              # What RTM can't do (honest)
│   └── decision-framework.md       # Should you migrate? Flowchart
├── docker/
│   ├── docker-compose.yml          # Kafka + Flink 1.20 + Spark 4.1
│   ├── spark-conf/spark-defaults.conf
│   └── flink-conf/flink-conf.yaml
├── pipelines/
│   ├── rtm_trigger.py              # PySpark RTM trigger helper (Py4J bridge)
│   ├── 01-stateless-passthrough/
│   │   ├── flink/src/.../StatelessPassthrough.java
│   │   ├── flink/pom.xml
│   │   ├── spark/stateless_rtm.py
│   │   └── README.md
│   ├── 02-tumbling-window/
│   │   ├── flink/src/.../TumblingWindowCount.java
│   │   ├── flink/pom.xml
│   │   ├── spark/tumbling_count.py
│   │   └── README.md
│   ├── 03-sliding-window/
│   │   ├── flink/src/.../SlidingWindowAvg.java
│   │   ├── flink/pom.xml
│   │   ├── spark/sliding_avg.py
│   │   └── README.md
│   └── 04-keyed-state/
│       ├── flink/src/.../KeyedStateCounter.java
│       ├── flink/pom.xml
│       ├── spark/stateful_counter.py
│       └── README.md
├── data_generator/
│   └── producer.py                 # Shared Kafka producer (JSON events)
├── benchmarks/
│   ├── run-benchmarks.sh           # Automated benchmark runner
│   ├── measure-latency.py          # Kafka consumer latency measurement
│   └── results/
│       └── .gitkeep
└── scripts/
    ├── setup.sh                    # Create topics + build Flink JARs
    ├── run-flink.sh                # Submit Flink job by pattern number
    └── run-spark-rtm.sh            # Submit Spark job by pattern number
```

## Configuration

### Spark (RTM-tuned)

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `spark.sql.shuffle.partitions` | 20 | Reduce task slot requirements |
| `spark.sql.execution.sortBeforeRepartition` | false | Avoid unnecessary sort in RTM |
| `spark.sql.streaming.stateStore.providerClass` | `...RocksDBStateStoreProvider` | Required for transformWithState |
| `spark.sql.streaming.realTimeMode.allowlistCheck` | false | Bypass operator allowlist (OSS only) |

### Flink

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `taskmanager.numberOfTaskSlots` | 16 | Match Spark's parallelism |
| `taskmanager.memory.process.size` | 4096m | Match Spark's memory allocation |
| `parallelism.default` | 16 | Default parallelism for all operators |
| `state.backend` | rocksdb | Required for keyed state patterns |
| `execution.checkpointing.interval` | 5000 | 5s checkpoint interval |

### Fair Comparison Setup

Both engines are configured for parity:
- **16 task slots** (Flink: task slots, Spark: cores)
- **4GB memory** per TaskManager / Executor
- **Shared Kafka** with 16 partitions per topic
- **Same data generator**: 10K events/sec, 100 keys, JSON format
- **Same latency measurement**: external consumer, wall-clock comparison

## Key Findings from Testing

1. **OSS Spark 4.1 RTM only supports stateless operations by default** — stateful operators
   require setting `allowlistCheck=false`
2. **PySpark doesn't expose `trigger(realTime=...)`** — requires Py4J bridge helper
3. **`current_timestamp()` is stale in RTM** — evaluates once per checkpoint boundary
4. **Console sink doesn't flush in RTM** — use Kafka sink for output
5. **transformWithState requires RocksDB** — column families not supported by default state store

See [limitations.md](docs/limitations.md) for the full list.

## Links

- [Spark RTM Quickstart](../spark-rtm-quickstart/) — Simpler demo pipelines (RTM only)
- [Spark 4.1 Release Notes](https://spark.apache.org/releases/spark-release-4-1-0.html)
- [SPIP: Real-Time Mode (SPARK-52330)](https://issues.apache.org/jira/browse/SPARK-52330)
- [Flink 1.20 Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
