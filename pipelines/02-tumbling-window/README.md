# Pipeline 02 -- Tumbling Window Count

## Pattern

Count events per key inside non-overlapping (tumbling) 1-minute windows.
A watermark of 10 seconds allows for modest out-of-order data while still
closing windows promptly.

This is the canonical "group-by time window" pattern -- the streaming
equivalent of `SELECT key, COUNT(*) ... GROUP BY key, TUMBLE(ts, INTERVAL 1 MINUTE)`.

## Flink implementation

```
KafkaSource
  ──▶ keyBy(key)
  ──▶ TumblingEventTimeWindows.of(Time.minutes(1))
  ──▶ CountFunction
  ──▶ PrintSink
```

Flink assigns each event to exactly one window pane, accumulates state per
key-window pair, and fires results when the watermark passes the window end.

## Spark implementation

```
KafkaSource
  ──▶ withWatermark("produced_at", "10 seconds")
  ──▶ groupBy(key, window(produced_at, "1 minute"))
  ──▶ count()
  ──▶ console sink  (outputMode = update)
```

### Trigger mode

By default the pipeline uses **micro-batch** with a 10-second processing-time
trigger.  This aligns naturally with windowed aggregation: Spark accumulates
records across the micro-batch, advances the watermark, and emits results for
any windows that have closed.

Set `USE_RTM=1` to switch to the Real-Time Mode trigger (experimental for
stateful workloads in Spark 4.x).

### Key Spark APIs

| Concept | API |
|---------|-----|
| Watermark | `withWatermark("produced_at", "10 seconds")` |
| Tumbling window | `window(col, "1 minute")` |
| Aggregation | `groupBy(...).agg(count("*"))` |
| Output mode | `update` -- emit changed rows each trigger |

### Running

```bash
# Micro-batch (default)
spark-submit \
  --master spark://spark-master:7077 \
  pipelines/02-tumbling-window/spark/tumbling_count.py

# Real-Time Mode (experimental)
USE_RTM=1 spark-submit \
  --master spark://spark-master:7077 \
  pipelines/02-tumbling-window/spark/tumbling_count.py
```

### Flink vs. Spark comparison

| Dimension | Flink | Spark (micro-batch) |
|-----------|-------|---------------------|
| Window semantics | Per-record assignment | Watermark-driven batch close |
| Trigger granularity | Per-watermark advance | Per processing-time interval |
| Late data | Side output | Dropped after watermark |
| State backend | RocksDB / HashMapState | HDFS-backed or RocksDB (Spark 4.x) |
