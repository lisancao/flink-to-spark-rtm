# Flink to Spark RTM: API Mapping Reference

This document maps Apache Flink DataStream API concepts to their Apache Spark
Structured Streaming Real-Time Mode (RTM) equivalents. RTM was introduced in
Spark 4.1.0 and delivers sub-300ms p99 latency with exactly-once semantics.

Where a direct equivalent exists, both code snippets are shown. Where the
mapping is partial or absent, the differences are called out explicitly.

---

## Quick-Reference Table

| Flink Concept | Spark RTM Equivalent | Notes |
|---|---|---|
| `StreamExecutionEnvironment` | `SparkSession` | Spark uses a unified session for batch and streaming |
| `env.addSource(new FlinkKafkaConsumer(...))` | `spark.readStream.format("kafka")` | Both use Kafka consumer groups under the hood |
| `dataStream.keyBy(...)` | `df.groupBy(...)` | Spark groupBy returns a `RelationalGroupedDataset` |
| `window(TumblingEventTimeWindows.of(...))` | `window(col("event_time"), "1 minute")` | Spark uses SQL-style `window()` function |
| `window(SlidingEventTimeWindows.of(...))` | `window(col("event_time"), "2 minutes", "30 seconds")` | Second arg is window size, third is slide interval |
| `.aggregate(new MyAggregateFunction())` | `.agg(count("*"), sum("value"), ...)` | Spark uses built-in aggregate functions; custom aggregates via UDAF |
| `ProcessFunction` + `ValueState` | `transformWithState` + `StatefulProcessor` | RTM invokes `handleInputRows` per row, not per key batch |
| `env.enableCheckpointing(5000)` | `trigger(realTime="5 minutes")` | RTM checkpoint interval; data flows continuously between checkpoints |
| `WatermarkStrategy.forBoundedOutOfOrderness(...)` | `.withWatermark("event_time", "10 seconds")` | Spark watermark is column-based, declared once |
| `.name("my-operator")` | `.appName("my-pipeline")` | Spark names are at the application level, not per operator |
| `SideOutput` / `OutputTag` | **Not supported** | No equivalent in Structured Streaming |
| `ProcessFunction` timers (event-time) | **Not supported in RTM** | Only processing-time timers available via `transformWithState` |
| `ProcessFunction` timers (processing-time) | `transformWithState` with `timeMode="ProcessingTime"` | Limited support; see [limitations](./limitations.md) |
| `DataStream.union(...)` | `df1.union(df2)` | Supported in RTM with restrictions |
| `AsyncDataStream.unorderedWait(...)` | **Not supported** | No async I/O operator in Structured Streaming |
| `.addSink(new FlinkKafkaProducer(...))` | `.writeStream.format("kafka")` | Both support exactly-once Kafka writes |
| `OutputMode` (Flink has none) | `.outputMode("update")` | RTM only supports Update mode |

---

## Detailed Mappings with Code

### 1. Kafka Source

**Flink (Java)**

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("input-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> stream = env.fromSource(
    source, WatermarkStrategy.noWatermarks(), "Kafka Source");
```

**Spark RTM (Python)**

```python
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "input-topic")
    .option("startingOffsets", "latest")
    .load())

# Kafka source returns key/value as binary; cast to string
parsed = raw.selectExpr(
    "CAST(key AS STRING)", "CAST(value AS STRING)")
```

**Differences:**
- Flink requires explicit deserialization schemas; Spark always reads raw
  bytes from Kafka and you cast/parse in the DataFrame layer.
- Flink consumer group ID is set on the source; Spark auto-generates one
  from the checkpoint location (you can override with `kafka.group.id`).
- Spark's Kafka source handles offset tracking via the checkpoint, not
  the Kafka consumer group offset.

---

### 2. Watermark Assignment

**Flink (Java)**

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

DataStream<Event> withWatermarks = stream.assignTimestampsAndWatermarks(strategy);
```

**Spark RTM (Python)**

```python
from pyspark.sql.functions import from_unixtime, col

parsed = (raw
    .select(from_json(col("json_str"), schema).alias("event"))
    .select("event.*")
    .withColumn("event_time",
        from_unixtime(col("produced_at") / 1000).cast("timestamp")))

watermarked = parsed.withWatermark("event_time", "10 seconds")
```

**Differences:**
- Flink assigns watermarks per-record via a `WatermarkStrategy` that can
  be customized (periodic, punctuated, custom). Spark declares a single
  watermark delay on a timestamp column.
- Flink's `forBoundedOutOfOrderness` and Spark's `withWatermark` are
  conceptually equivalent: both define how late data can arrive.
- Spark does not support punctuated watermarks or custom watermark generators.

---

### 3. Windowed Aggregation

**Flink (Java)**

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

DataStream<Result> result = stream
    .keyBy(event -> event.getKey())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new CountAggregate());
```

**Spark RTM (Python)**

```python
from pyspark.sql.functions import window, count, col

windowed = (watermarked
    .groupBy(
        col("key"),
        window(col("event_time"), "1 minute"),
    )
    .agg(count("*").alias("event_count")))
```

**Differences:**
- Flink's `keyBy` + `window` + `aggregate` maps to Spark's `groupBy` +
  `window()` function + `agg()`.
- Flink supports custom `AggregateFunction` implementations with
  accumulator logic. Spark uses built-in aggregate functions (`count`,
  `sum`, `avg`, `min`, `max`) or UDAFs.
- Window results in Spark include `window.start` and `window.end` columns.

---

### 4. Keyed State (ProcessFunction vs transformWithState)

**Flink (Java)**

```java
public class RunningCounter extends KeyedProcessFunction<String, Event, Result> {
    private ValueState<Integer> countState;

    @Override
    public void open(Configuration params) {
        ValueStateDescriptor<Integer> desc =
            new ValueStateDescriptor<>("count", Integer.class, 0);
        countState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Result> out)
            throws Exception {
        int current = countState.value();
        current++;
        countState.update(current);
        out.collect(new Result(event.getKey(), current, event.getEventType()));
    }
}

stream.keyBy(Event::getKey)
    .process(new RunningCounter());
```

**Spark RTM (Python)**

```python
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import TimerValues
from typing import Iterator

COUNT_STATE_SCHEMA = StructType([StructField("count", IntegerType())])
OUTPUT_SCHEMA = StructType([
    StructField("key", StringType()),
    StructField("running_count", IntegerType()),
    StructField("last_event_type", StringType()),
])

class RunningCountProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.count_state = handle.getValueState("running_count", COUNT_STATE_SCHEMA)

    def handleInputRows(self, key: tuple, rows: Iterator[Row],
                        timerValues: TimerValues) -> Iterator[Row]:
        current_count = self.count_state.get()[0] if self.count_state.exists() else 0
        last_event_type = None
        for row in rows:
            current_count += 1
            last_event_type = row.event_type
        self.count_state.update((current_count,))
        yield Row(key=key[0], running_count=current_count,
                  last_event_type=last_event_type)

    def close(self) -> None:
        pass

stateful = (parsed
    .groupBy("key")
    .transformWithState(
        statefulProcessor=RunningCountProcessor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="Update",
        timeMode="None",
    ))
```

**Differences:**
- Flink's `processElement` is called once per record. In Spark RTM,
  `handleInputRows` is also called per row (the iterator yields a single
  value). In micro-batch mode, the iterator may contain multiple rows per
  key.
- Flink state descriptors are created in `open()`; Spark state handles
  are created in `init()`.
- Flink supports `ValueState`, `ListState`, `MapState`, `ReducingState`,
  and `AggregatingState`. Spark `transformWithState` supports
  `getValueState`, `getListState`, and `getMapState`.
- Flink's `processElement` collects output via `Collector`; Spark's
  `handleInputRows` uses Python generators (`yield`).
- **Important:** `transformWithState` requires the RocksDB state store
  provider:
  ```python
  .config("spark.sql.streaming.stateStore.providerClass",
          "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
  ```

---

### 5. Checkpointing Configuration

**Flink (Java)**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);  // checkpoint every 5 seconds
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
env.getCheckpointConfig().setCheckpointTimeout(60000);
```

**Spark RTM (Python)**

```python
query = (output.writeStream
    .format("kafka")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/my-pipeline")
    .outputMode("update")
    .trigger(realTime="5 minutes")  # checkpoint interval
    .start())
```

**Differences:**
- Flink checkpointing is configured on the execution environment with
  multiple parameters (interval, mode, timeout, min pause). Spark RTM
  uses a single checkpoint interval specified in the trigger.
- In Flink, the checkpoint interval directly affects latency (shorter
  intervals = more overhead). In Spark RTM, the checkpoint interval is
  decoupled from latency -- data flows continuously regardless of
  checkpoint frequency.
- Spark checkpoints are written to a filesystem path (HDFS, S3, local).
  Flink checkpoints go to a configured state backend (RocksDB, filesystem,
  etc.).
- Both provide exactly-once semantics.

---

### 6. Kafka Sink

**Flink (Java)**

```java
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
            .setTopic("output-topic")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build();

stream.sinkTo(sink);
```

**Spark RTM (Python)**

```python
output = (parsed
    .select(
        col("key").alias("key"),
        to_json(struct("*")).alias("value"),
    ))

query = (output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "output-topic")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/pipeline")
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())
```

**Differences:**
- Flink requires explicit serialization schemas; Spark expects `key` and
  `value` columns as strings or binary.
- Flink's exactly-once Kafka sink uses Kafka transactions
  (`EXACTLY_ONCE` delivery guarantee). Spark's Kafka sink also uses
  transactions, managed automatically by the checkpoint mechanism.
- Spark requires a `checkpointLocation`; Flink manages checkpoint storage
  separately from the sink configuration.

---

### 7. Application Naming and Identification

**Flink (Java)**

```java
// Per-operator naming
stream
    .filter(e -> e.getType().equals("click"))
    .name("click-filter")
    .keyBy(Event::getKey)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new CountAgg())
    .name("click-counter");
```

**Spark RTM (Python)**

```python
# Application-level naming only
spark = (SparkSession.builder
    .appName("RTM-Click-Counter")
    .getOrCreate())
```

**Differences:**
- Flink allows `.name()` on each operator for fine-grained monitoring in
  the Flink Web UI. Spark names are at the application level only.
- Spark's Spark UI shows stages and tasks but does not support per-operator
  naming in the streaming query plan.

---

### 8. Concepts with No RTM Equivalent

| Flink Feature | Status in Spark RTM |
|---|---|
| `SideOutput` / `OutputTag` | Not supported. Workaround: write to multiple sinks from different filtered branches of the same source DataFrame. |
| Event-time timers (`ctx.timerService().registerEventTimeTimer(...)`) | Not supported in RTM. Only processing-time timers are available in `transformWithState`. |
| `AsyncDataStream` (async I/O) | Not supported. Workaround: use `forEach` with async calls inside the writer, or handle async logic outside Spark. |
| Session windows | Not supported in RTM or Structured Streaming's Update mode. Available in micro-batch Append mode only. |
| Stream-stream joins | Not supported in RTM. Available in micro-batch mode. |
| `CoProcessFunction` (connected streams) | No direct equivalent. Use `transformWithState` with unioned streams and a type discriminator column. |
| `BroadcastState` pattern | Partial: use stream-table joins with a broadcast hint for reference data. Not dynamically updatable in RTM. |
| Savepoints (portable snapshots) | Not supported. Spark checkpoints are tied to the query and cannot be used for migration or versioning. |

---

## PySpark RTM Trigger Note

As of Spark 4.1.0, PySpark does not expose `trigger(realTime=...)` as a
keyword argument. You need a Py4J bridge helper:

```python
def with_real_time_trigger(dsw, checkpoint_interval="5 minutes"):
    """Apply RealTimeTrigger to a DataStreamWriter via JVM gateway."""
    jvm = dsw._spark._jvm
    jTrigger = jvm.org.apache.spark.sql.execution.streaming.RealTimeTrigger.apply(
        checkpoint_interval
    )
    dsw._jwrite = dsw._jwrite.trigger(jTrigger)
    return dsw
```

Usage:

```python
writer = (output.writeStream
    .format("kafka")
    .option("topic", "output-topic")
    .option("checkpointLocation", "/tmp/checkpoint")
    .outputMode("update"))

query = with_real_time_trigger(writer, "5 minutes").start()
```

This is a temporary workaround. Future PySpark releases are expected to add
native `trigger(realTime=...)` support.
