# Pipeline 04 -- Keyed State Counter

## Pattern

Maintain a **per-key running counter** that increments by 1 for every event
received.  Each output row contains the key and its cumulative count.

This is the streaming equivalent of a `running_total` -- the simplest
demonstration of managed keyed state.  In practice this pattern generalizes to
any per-key accumulator: running sums, session builders, anomaly detectors, etc.

## Flink implementation

```
KafkaSource
  ──▶ keyBy(key)
  ──▶ KeyedProcessFunction {
         ValueState<Long> counter;
         processElement(event) {
           counter.update(counter.value() + 1);
           out.collect(key, counter.value());
         }
       }
  ──▶ PrintSink
```

Flink gives each key its own `ValueState<Long>`, persisted to the configured
state backend (RocksDB in production).  State is checkpointed for
exactly-once recovery.

## Spark implementation

```
KafkaSource
  ──▶ withWatermark("produced_at", "10 seconds")
  ──▶ groupBy("key")
  ──▶ transformWithState(KeyedCounterProcessor)
  ──▶ console sink  (outputMode = update)
```

### How transformWithState works

`transformWithState` (Spark 4.x) is the Structured Streaming equivalent of
Flink's `KeyedProcessFunction`.  You subclass `StatefulProcessor` and get
access to:

- **`getValueState(name, schema)`** -- a single value per key (like Flink's
  `ValueState`)
- **`getListState(name, schema)`** -- a list per key (like Flink's
  `ListState`)

Inside `handleInputRows(key, rows)` you read/write state and yield output rows.
Spark manages serialization, checkpointing, and state TTL.

### State store backend

The pipeline is configured to use **RocksDB** via:

```python
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)
```

RocksDB keeps state on local disk with an in-memory cache, making it suitable
for large state sizes that exceed executor memory.

### Trigger mode

Defaults to **micro-batch** (10-second processing-time trigger).  Set
`USE_RTM=1` for the Real-Time Mode trigger (experimental for stateful
workloads).

### Key Spark APIs

| Concept | API |
|---------|-----|
| Stateful processing | `transformWithState(StatefulProcessor, ...)` |
| Per-key value state | `handle.getValueState("count", IntegerType())` |
| State read/write | `state.get()`, `state.update(value)` |
| State backend | `RocksDBStateStoreProvider` |
| Output mode | `update` -- emit changed keys each trigger |

### Running

```bash
# Micro-batch (default)
spark-submit \
  --master spark://spark-master:7077 \
  pipelines/04-keyed-state/spark/stateful_counter.py

# Real-Time Mode (experimental)
USE_RTM=1 spark-submit \
  --master spark://spark-master:7077 \
  pipelines/04-keyed-state/spark/stateful_counter.py
```

### Flink vs. Spark comparison

| Dimension | Flink | Spark (transformWithState) |
|-----------|-------|---------------------------|
| State abstraction | `ValueState<T>` | `ValueState` via `getValueState()` |
| State backend | RocksDB / HashMapState | RocksDB / HDFSBackedState |
| Per-record processing | Native | Per-key batch within micro-batch |
| Checkpointing | Async barrier-based | Write-ahead log per micro-batch |
| State TTL | Built-in TTL config | `timeMode` param (Spark 4.x) |
| Timers | `registerEventTimeTimer()` | Timer support in `StatefulProcessor` |
