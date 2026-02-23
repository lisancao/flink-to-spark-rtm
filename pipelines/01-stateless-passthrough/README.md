# Pipeline 01 -- Stateless Passthrough

## Pattern

The simplest streaming pattern: read events from Kafka, filter to keep only
`click` and `purchase` event types, and write the surviving records back to a
different Kafka topic with no transformation.

Because no aggregation or state is involved, the pipeline is **purely
stateless**.

## Flink implementation

A single `FilterFunction` wired between a `KafkaSource` and a `KafkaSink`.
Flink processes each record independently with true per-record semantics.

```
KafkaSource ──▶ FilterFunction(click | purchase) ──▶ KafkaSink
```

## Spark implementation (Real-Time Mode)

Structured Streaming with the **Real-Time Mode (RTM)** trigger.  RTM is the
ideal fit here: because there is no aggregation, Spark does not need to
accumulate a micro-batch.  Each record can flow through the filter and land in
the output topic with single-digit-millisecond latency -- comparable to Flink's
per-record model.

```
KafkaSource ──▶ filter(event_type IN (click, purchase)) ──▶ KafkaSink
              └── Real-Time Trigger (no micro-batch) ──┘
```

### Key Spark APIs

| Concept | API |
|---------|-----|
| Read from Kafka | `spark.readStream.format("kafka")` |
| JSON parsing | `from_json()` with a `StructType` schema |
| Filtering | `DataFrame.filter(col(...).isin(...))` |
| Write to Kafka | `writeStream.format("kafka")` |
| RTM trigger | `with_real_time_trigger(writer)` (custom helper) |

### Running

```bash
spark-submit \
  --master spark://spark-master:7077 \
  pipelines/01-stateless-passthrough/spark/stateless_rtm.py
```

Set `KAFKA_BOOTSTRAP_SERVERS` if Kafka is not at `kafka:9092`.

### When to use RTM vs. micro-batch

| Criterion | RTM | Micro-batch |
|-----------|-----|-------------|
| Stateless transforms | Ideal | Adds unnecessary latency |
| Windowed aggregations | Not supported | Required |
| Stateful processing | Not supported | Required |
| Latency target | < 10 ms | 100 ms -- seconds |
