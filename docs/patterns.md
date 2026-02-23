# Flink to Spark RTM: Pattern Translation Guide

This document translates four common streaming patterns from Flink (Java) to
Spark Real-Time Mode (Python), shown side-by-side. Each pattern includes
behavioral differences you should be aware of when migrating.

All Spark examples use PySpark 4.1+ with Real-Time Mode. Flink examples
target Flink 1.18+ with the DataStream API.

---

## Pattern 1: Stateless Passthrough

**Use case:** Read events from Kafka, filter and transform, write to another
Kafka topic. No state, no aggregation, no shuffle.

This is the simplest streaming pattern and where RTM delivers its best
latency: single-digit milliseconds p99.

### Flink (Java)

```java
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StatelessPassthrough {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("input-topic")
            .setGroupId("passthrough-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<String> filtered = stream
            .filter(json -> {
                JsonNode node = mapper.readTree(json);
                String type = node.get("event_type").asText();
                return type.equals("click") || type.equals("purchase");
            })
            .name("filter-clicks-purchases");

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("output-topic")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

        filtered.sinkTo(sink).name("Kafka Sink");
        env.execute("Stateless Passthrough");
    }
}
```

### Spark RTM (Python)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, struct, to_json
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("key", StringType()),
    StructField("value", IntegerType()),
    StructField("produced_at", LongType()),
])

spark = (SparkSession.builder
    .appName("RTM-Stateless-Passthrough")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate())

# Source: Kafka
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "input-topic")
    .option("startingOffsets", "latest")
    .load())

# Transform: parse JSON, filter, add processing timestamp
parsed = (raw
    .selectExpr("CAST(key AS STRING) AS kafka_key",
                "CAST(value AS STRING) AS json_str")
    .select(col("kafka_key"),
            from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
    .select("kafka_key", "event.*")
    .filter(col("event_type").isin("click", "purchase"))
    .withColumn("processed_at", current_timestamp()))

# Sink: Kafka
output = parsed.select(
    col("key").alias("key"),
    to_json(struct("*")).alias("value"))

query = (output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "output-topic")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/passthrough")
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())

query.awaitTermination()
```

### Behavioral Differences

| Aspect | Flink | Spark RTM |
|---|---|---|
| **Latency** | Sub-millisecond per record | 5-15ms p99 end-to-end |
| **Deserialization** | Explicit schema in source | Cast from bytes + `from_json` in DataFrame |
| **Filter execution** | Per-record Java lambda | Catalyst-optimized predicate pushdown |
| **Timestamp addition** | `System.currentTimeMillis()` per record | `current_timestamp()` evaluates once per checkpoint, not per row |
| **Operator naming** | `.name()` per operator | `.appName()` at session level only |
| **Exactly-once** | Kafka transactions via sink config | Kafka transactions via checkpoint mechanism |

**Migration note:** The `current_timestamp()` gotcha is significant. In Flink,
you naturally call `System.currentTimeMillis()` per record. In Spark RTM,
`current_timestamp()` evaluates once per checkpoint boundary. If you need
per-record wall-clock timestamps, embed them in your source data or use a
`forEach` writer.

---

## Pattern 2: Tumbling Window Count

**Use case:** Count events per key within fixed-size, non-overlapping time
windows. Common for metrics dashboards, rate limiting, and alerting.

### Flink (Java)

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;

public class TumblingWindowCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // ... Kafka source setup (same as Pattern 1) ...

        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, ts) -> event.getProducedAt());

        DataStream<Event> withWatermarks = stream
            .map(json -> Event.fromJson(json))
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Tuple3<String, Long, Long>> counts = withWatermarks
            .keyBy(Event::getKey)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new CountAggregate())
            .name("tumbling-1m-count");

        counts.print();
        env.execute("Tumbling Window Count");
    }
}

// CountAggregate implements AggregateFunction<Event, Long, Tuple3<String, Long, Long>>
// - createAccumulator() -> 0L
// - add(event, acc) -> acc + 1
// - getResult(acc) -> Tuple3(key, windowEnd, acc)
// - merge(a, b) -> a + b
```

### Spark RTM (Python)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, from_unixtime, window
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("key", StringType()),
    StructField("value", IntegerType()),
    StructField("produced_at", LongType()),
])

spark = (SparkSession.builder
    .appName("RTM-Tumbling-Window-Count")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate())

# Source: Kafka
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "input-topic")
    .option("startingOffsets", "latest")
    .load())

# Parse and derive event-time column
parsed = (raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
    .select("event.*")
    .withColumn("event_time",
        from_unixtime(col("produced_at") / 1000).cast("timestamp")))

# Watermark + tumbling window aggregation
windowed = (parsed
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        col("key"),
        window(col("event_time"), "1 minute"),
    )
    .agg(count("*").alias("event_count")))

# Output: key, window_start, window_end, event_count
output = windowed.select(
    col("key"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("event_count"))

query = (output.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/tumbling")
    .option("truncate", "false")
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())

query.awaitTermination()
```

### Behavioral Differences

| Aspect | Flink | Spark RTM |
|---|---|---|
| **Window assignment** | Explicit `TumblingEventTimeWindows.of(...)` | `window()` SQL function on a timestamp column |
| **Aggregation** | Custom `AggregateFunction` with accumulator | Built-in `count()`, `sum()`, etc. |
| **Window output timing** | Fires when watermark passes window end | Emits updates continuously in Update mode |
| **Late data** | Configurable allowed lateness | Controlled by watermark delay only |
| **Output semantics** | One result per window per key (on fire) | Multiple updates per window as data arrives |
| **Watermark strategy** | Per-record `TimestampAssigner` | Column-level `withWatermark()` declaration |

**Migration note:** The output semantics differ significantly. Flink fires
the window once when the watermark passes the window boundary (unless allowed
lateness is configured). Spark RTM in Update mode emits intermediate results
as each record arrives. Downstream consumers must handle receiving multiple
updates for the same window. If your downstream expects a single final result,
you need to handle deduplication or use watermark-based filtering downstream.

**OSS Spark note:** Windowed aggregation is a stateful operation. In OSS
Spark 4.1, RTM stateful operators are gated behind an allowlist. To run this
in RTM on OSS Spark, set:
```
spark.sql.streaming.realTimeMode.allowlistCheck=false
```
On Databricks Runtime, stateful RTM operators are fully supported.

---

## Pattern 3: Sliding Window Average

**Use case:** Compute a rolling average over overlapping windows. Common for
smoothing metrics, anomaly detection thresholds, and real-time SLA monitoring.

### Flink (Java)

```java
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SlidingWindowAverage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // ... Kafka source + watermark setup ...

        DataStream<Result> averages = withWatermarks
            .keyBy(Event::getKey)
            .window(SlidingEventTimeWindows.of(
                Time.minutes(2),   // window size
                Time.seconds(30))) // slide interval
            .aggregate(new AverageAggregate())
            .name("sliding-2m-avg");

        averages.print();
        env.execute("Sliding Window Average");
    }
}

// AverageAggregate implements AggregateFunction<Event, Tuple2<Long, Long>, Double>
// - createAccumulator() -> (0L, 0L)  // (sum, count)
// - add(event, acc) -> (acc.f0 + event.getValue(), acc.f1 + 1)
// - getResult(acc) -> (double) acc.f0 / acc.f1
// - merge(a, b) -> (a.f0 + b.f0, a.f1 + b.f1)
```

### Spark RTM (Python)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, from_unixtime, window
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("key", StringType()),
    StructField("value", IntegerType()),
    StructField("produced_at", LongType()),
])

spark = (SparkSession.builder
    .appName("RTM-Sliding-Window-Average")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate())

# Source: Kafka
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "input-topic")
    .option("startingOffsets", "latest")
    .load())

parsed = (raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
    .select("event.*")
    .withColumn("event_time",
        from_unixtime(col("produced_at") / 1000).cast("timestamp")))

# Sliding window: 2-minute windows sliding every 30 seconds
windowed = (parsed
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        col("key"),
        window(col("event_time"), "2 minutes", "30 seconds"),
    )
    .agg(
        avg("value").alias("avg_value"),
        count("*").alias("sample_count"),
    ))

output = windowed.select(
    col("key"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_value"),
    col("sample_count"))

query = (output.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/sliding")
    .option("truncate", "false")
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())

query.awaitTermination()
```

### Behavioral Differences

| Aspect | Flink | Spark RTM |
|---|---|---|
| **Sliding window API** | `SlidingEventTimeWindows.of(size, slide)` | `window(col, "2 minutes", "30 seconds")` |
| **Record per window** | Each record assigned to `size/slide` windows (here, 4) | Same: each record contributes to 4 overlapping windows |
| **State overhead** | Managed per-window in state backend | Managed per-window in state store |
| **Aggregation** | Custom `AverageAggregate` with sum/count accumulator | Built-in `avg()` function |
| **Output** | One result per window on fire | Continuous updates in Update mode |
| **Memory** | Proportional to `size/slide` ratio * keys | Same proportionality, stored in RocksDB or in-memory state store |

**Migration note:** Sliding windows generate more state than tumbling windows
because each record belongs to multiple windows. With a 2-minute window
sliding every 30 seconds, each record belongs to 4 windows. Monitor state
size carefully. The state management cost applies equally to both Flink and
Spark.

**Migration note:** Flink's custom `AverageAggregate` computes the average
incrementally via a sum/count accumulator. Spark's built-in `avg()` handles
this internally. If you need a custom aggregation not available as a built-in
function, you need a Scala or Python UDAF.

---

## Pattern 4: Keyed State Counter

**Use case:** Maintain a running count per key across the entire stream
lifetime, not bounded by windows. Useful for session tracking, feature
counters, or deduplication state.

### Flink (Java)

```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedStateCounter
        extends KeyedProcessFunction<String, Event, Result> {

    private transient ValueState<Integer> countState;

    @Override
    public void open(Configuration params) {
        ValueStateDescriptor<Integer> desc =
            new ValueStateDescriptor<>("event-count", Integer.class, 0);
        countState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event event, Context ctx,
            Collector<Result> out) throws Exception {
        int current = countState.value();
        current++;
        countState.update(current);
        out.collect(new Result(
            event.getKey(), current, event.getEventType()));
    }
}

// Usage:
stream.keyBy(Event::getKey)
    .process(new KeyedStateCounter())
    .name("keyed-counter");
```

### Spark RTM (Python)

```python
from typing import Iterator
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import TimerValues
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("key", StringType()),
    StructField("value", IntegerType()),
    StructField("produced_at", LongType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("key", StringType()),
    StructField("running_count", IntegerType()),
    StructField("last_event_type", StringType()),
])

COUNT_STATE_SCHEMA = StructType([StructField("count", IntegerType())])


class RunningCountProcessor(StatefulProcessor):
    """Per-key running count using transformWithState.

    In RTM: handleInputRows is called per row (iterator yields 1 value).
    In micro-batch: the iterator may contain multiple rows per key.
    The code handles both cases correctly by iterating.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.count_state = handle.getValueState(
            "running_count", COUNT_STATE_SCHEMA)

    def handleInputRows(self, key: tuple, rows: Iterator[Row],
                        timerValues: TimerValues) -> Iterator[Row]:
        current_count = (
            self.count_state.get()[0] if self.count_state.exists() else 0)

        last_event_type = None
        for row in rows:
            current_count += 1
            last_event_type = row.event_type

        self.count_state.update((current_count,))
        yield Row(
            key=key[0],
            running_count=current_count,
            last_event_type=last_event_type)

    def close(self) -> None:
        pass


spark = (SparkSession.builder
    .appName("RTM-Keyed-State-Counter")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state."
            "RocksDBStateStoreProvider")
    .getOrCreate())

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "input-topic")
    .option("startingOffsets", "latest")
    .load())

parsed = (raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
    .select("event.*"))

stateful = (parsed
    .groupBy("key")
    .transformWithState(
        statefulProcessor=RunningCountProcessor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="Update",
        timeMode="None",
    ))

query = (stateful.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/rtm-checkpoints/keyed-state")
    .option("truncate", "false")
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())

query.awaitTermination()
```

### Behavioral Differences

| Aspect | Flink | Spark RTM |
|---|---|---|
| **API** | `KeyedProcessFunction` + `ValueState` | `StatefulProcessor` + `getValueState` |
| **Invocation** | `processElement` called per record | `handleInputRows` called per row in RTM (iterator has 1 element) |
| **State initialization** | `open()` with `ValueStateDescriptor` | `init()` with `handle.getValueState()` |
| **State access** | `countState.value()` / `countState.update()` | `count_state.get()` / `count_state.update()` (tuple-based) |
| **Output** | `Collector.collect()` (imperative) | `yield Row(...)` (generator-based) |
| **State backend** | RocksDB, HashMapStateBackend, etc. | RocksDB (required for `transformWithState`) |
| **State expiration** | TTL via `StateTtlConfig` | TTL via `handle.getValueState(..., ttlDuration=...)` |
| **Event-time timers** | `ctx.timerService().registerEventTimeTimer(t)` | **Not available in RTM** |
| **Processing-time timers** | `ctx.timerService().registerProcessingTimeTimer(t)` | Available via `timeMode="ProcessingTime"` and `handle.registerTimer(...)` |

**Migration note:** The most significant difference is invocation granularity
in RTM. Flink calls `processElement` per record, and Spark RTM calls
`handleInputRows` per row (with an iterator that yields exactly one element).
The code should iterate over `rows` regardless, so it works correctly in both
RTM and micro-batch modes. Do not assume the iterator length.

**Migration note:** Flink state is strongly typed (`ValueState<Integer>`).
Spark state uses tuples conforming to a schema (`StructType`). Access is
tuple-index-based: `self.count_state.get()[0]` rather than a typed accessor.

**State backend requirement:** `transformWithState` requires the RocksDB
state store provider. Set this in your `SparkSession` config:
```python
.config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state."
        "RocksDBStateStoreProvider")
```

**OSS Spark note:** `transformWithState` is a stateful operator gated behind
the RTM allowlist in OSS Spark 4.1. To run in RTM on OSS Spark, set
`spark.sql.streaming.realTimeMode.allowlistCheck=false`. On Databricks
Runtime, this works without configuration changes.
