# Pipeline 03 -- Sliding Window Average

## Pattern

Compute a rolling average of the `value` field per key over a **5-minute
sliding window** that advances every **1 minute**.  Each event contributes to
5 overlapping windows simultaneously.  A 10-second watermark handles
out-of-order arrivals.

This pattern is common for metrics dashboards and SLA monitoring where you need
a continuously-updated moving average rather than discrete bucket counts.

## Flink implementation

```
KafkaSource
  ──▶ keyBy(key)
  ──▶ SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))
  ──▶ AverageAggregateFunction
  ──▶ PrintSink
```

Flink internally maintains an incremental aggregate (sum, count) per
key-window pane.  When the watermark passes a window's end time, the final
`avg = sum / count` is emitted.

## Spark implementation

```
KafkaSource
  ──▶ withWatermark("produced_at", "10 seconds")
  ──▶ groupBy(key, window(produced_at, "5 minutes", "1 minute"))
  ──▶ agg(avg(value), count(*))
  ──▶ console sink  (outputMode = update)
```

### How the sliding window works in Spark

The `window()` function with two duration arguments creates overlapping
windows.  Internally Spark expands each row into multiple rows -- one per
window the event belongs to -- and then groups normally.  The watermark ensures
that state for expired windows is eventually cleaned up.

### Trigger mode

Defaults to **micro-batch** (10-second processing-time trigger).  Set
`USE_RTM=1` for the Real-Time Mode trigger (experimental for stateful
workloads).

### Key Spark APIs

| Concept | API |
|---------|-----|
| Watermark | `withWatermark("produced_at", "10 seconds")` |
| Sliding window | `window(col, "5 minutes", "1 minute")` |
| Aggregation | `avg("value")`, `count("*")` |
| Rounding | `pyspark.sql.functions.round` (aliased to `spark_round`) |
| Output mode | `update` -- emit changed rows each trigger |

### Running

```bash
# Micro-batch (default)
spark-submit \
  --master spark://spark-master:7077 \
  pipelines/03-sliding-window/spark/sliding_avg.py

# Real-Time Mode (experimental)
USE_RTM=1 spark-submit \
  --master spark://spark-master:7077 \
  pipelines/03-sliding-window/spark/sliding_avg.py
```

### Flink vs. Spark comparison

| Dimension | Flink | Spark (micro-batch) |
|-----------|-------|---------------------|
| Window overlap | 5 panes active per key | Same -- Spark expands rows |
| Incremental aggregation | Built-in `AggregateFunction` | Spark handles internally |
| Memory cost | Proportional to keys x panes | Same, managed by state store |
| Slide granularity | Arbitrary | Arbitrary (string duration) |
