# Spark RTM Limitations: What You Cannot Do (Yet)

This document provides an honest accounting of what Spark Real-Time Mode
cannot do as of Spark 4.1.0 (December 2025). If you are evaluating a
migration from Flink, these limitations should be part of your decision.

Each limitation includes context on why it exists, what the workaround is
(if any), and whether it is likely to be addressed in future releases.

---

## 1. No Session Windows

**What it means:** RTM does not support session windows (windows that open
on the first event for a key and close after a configurable gap of inactivity).

**Why it matters:** Session windows are critical for user session analytics,
clickstream analysis, and any pattern where the window boundary is
data-dependent rather than clock-dependent.

**Flink equivalent:** `EventTimeSessionWindows.withGap(Time.minutes(5))`

**Workaround:** Use `transformWithState` to implement manual session logic:
track the last-seen timestamp per key in state, emit results when the gap
exceeds your threshold using processing-time timers. This is significantly
more code than Flink's built-in session window assigner.

**Outlook:** Session windows are not on the published RTM roadmap. Structured
Streaming supports session windows in micro-batch Append mode, but this
support has not been extended to RTM's Update mode.

---

## 2. No Stream-Stream Joins

**What it means:** RTM does not support joining two streaming DataFrames.
Stream-table joins (a stream joined with a static or slowly-changing table)
are supported via broadcast, but two unbounded streams cannot be joined.

**Why it matters:** Stream-stream joins are used for enrichment patterns
where both sides are event streams (e.g., joining ad impressions with ad
clicks within a time window).

**Flink equivalent:** `stream1.keyBy(...).connect(stream2.keyBy(...)).process(...)`
or `stream1.join(stream2).where(...).equalTo(...).window(...).apply(...)`

**Workaround:** Write both streams to a table (Iceberg, Delta) and perform
the join as a batch or micro-batch query. Alternatively, use
`transformWithState` on a unioned stream with a type discriminator column,
buffering one side in state. Both workarounds add latency and complexity.

**Outlook:** Stream-stream joins are available in micro-batch mode. Extending
them to RTM requires streaming shuffle support for the join operator, which
is non-trivial.

---

## 3. No Event-Time Timers in transformWithState

**What it means:** In RTM, `transformWithState` supports processing-time
timers only. Event-time timers (`timeMode="EventTime"`) are not available.

**Why it matters:** Event-time timers fire based on watermark progress, which
is essential for patterns like "emit an alert if no event arrives for key X
within 5 minutes of event time." Processing-time timers fire based on
wall-clock time, which is less deterministic and not replayable.

**Flink equivalent:**
```java
ctx.timerService().registerEventTimeTimer(event.getTimestamp() + 300_000);
```

**Workaround:** Use processing-time timers as an approximation. For many
use cases (session timeout, inactivity alerts), processing-time timers are
acceptable if you do not need deterministic replay.

**Outlook:** Event-time timers require watermark propagation through the
streaming shuffle, which is an active area of development.

---

## 4. Update Mode Only

**What it means:** RTM supports only `outputMode("update")`. Append mode
and Complete mode are not available.

**Why it matters:**
- **Append mode** is required by some sinks (e.g., writing to Iceberg/Delta
  tables that expect insert-only semantics). Append mode also guarantees that
  each row is emitted exactly once, which simplifies downstream processing.
- **Complete mode** emits the entire result table on each trigger, which is
  useful for dashboards that display the full aggregated state.

**Flink context:** Flink does not have an explicit output mode concept. Its
DataStream API emits records as they are produced, which is closest to
Spark's Update mode. However, Flink's Table API supports append, retract,
and upsert modes.

**Workaround:** For sinks that need append semantics, write to Kafka in
Update mode and consume downstream with upsert logic. For complete mode use
cases, maintain the full state in an external store updated via `forEach`.

**Outlook:** Append mode support for RTM is a common request and may be
addressed in future Spark releases.

---

## 5. Limited Source Connectors

**What it means:** RTM supports only:
- **Kafka** (including Amazon MSK, Azure EventHub via Kafka protocol,
  Confluent Cloud)
- **Amazon Kinesis** (Enhanced Fan-Out mode only)

The following sources are **not** supported in RTM:
- Google Pub/Sub
- Apache Pulsar
- File sources (S3, HDFS, local)
- Socket source
- Rate source (except for testing)

**Why it matters:** If your Flink pipelines consume from Pub/Sub, Pulsar,
or file-based sources, you cannot migrate those to RTM without also
migrating the source to Kafka or Kinesis.

**Flink context:** Flink supports a broad set of connectors through its
connector ecosystem. The DataStream API can read from essentially any source
via custom `SourceFunction` implementations.

**Workaround:** Use a bridge layer (e.g., Pub/Sub to Kafka via Pub/Sub Lite
Kafka compatibility, or Pulsar's Kafka-on-Pulsar protocol). This adds
operational complexity and latency.

**Outlook:** Source support is expected to expand. Kafka covers the majority
of production streaming use cases, but the connector gap is a real barrier
for some teams.

---

## 6. Per-Row State Invocation in RTM

**What it means:** In RTM, `transformWithState`'s `handleInputRows` is
invoked once per row. The `rows` iterator yields a single element. In
micro-batch mode, the same method receives all rows for a key in one call.

**Why it matters:** If your stateful logic assumes batched access to
multiple rows per key (e.g., computing an average across the batch before
updating state), it will still work correctly but will be called N times
for N rows, each time with a single-element iterator. This changes
performance characteristics:
- More state reads/writes per key per checkpoint interval
- More function call overhead
- Potentially different output ordering

**Flink context:** Flink's `processElement` is always called per record, so
this matches Flink's invocation model more closely than Spark micro-batch.

**Code guidance:** Always iterate over `rows` without assuming the iterator
length. This ensures your `StatefulProcessor` works correctly in both RTM
and micro-batch modes:

```python
def handleInputRows(self, key, rows, timerValues):
    current = self.state.get()[0] if self.state.exists() else 0
    for row in rows:       # 1 row in RTM, potentially many in micro-batch
        current += row.value
    self.state.update((current,))
    yield Row(key=key[0], total=current)
```

---

## 7. Python UDFs Not in OSS RTM Allowlist

**What it means:** In open-source Spark 4.1, RTM has an operator allowlist
that gates which operations can run in real-time mode. Python UDFs (including
pandas UDFs) are not on this allowlist in OSS Spark.

**Why it matters:** Many PySpark pipelines rely on Python UDFs for custom
business logic that cannot be expressed with built-in functions. If your
pipeline uses Python UDFs, you cannot run it in RTM on OSS Spark without
disabling the allowlist check.

**Workaround:** Set `spark.sql.streaming.realTimeMode.allowlistCheck=false`
to bypass the check. This disables safety validation and may expose your
pipeline to unsupported operator combinations. Test thoroughly.

On **Databricks Runtime**, Python UDFs are supported in RTM without this
workaround.

**Outlook:** The allowlist is expected to expand as more operators are
validated for RTM. Scala UDFs are supported in OSS Spark 4.1.

---

## 8. Stateful Operations Not in OSS RTM Allowlist

**What it means:** Stateful operations -- including windowed aggregations,
`transformWithState`, `dropDuplicates`, and stream-table joins -- are gated
behind the RTM allowlist in open-source Spark 4.1. Only stateless operations
(select, filter, map, project) are on the OSS allowlist by default.

**Why it matters:** This is the most significant limitation for OSS users.
The majority of non-trivial streaming pipelines require some form of state.
Without stateful support, RTM on OSS Spark is limited to simple
filter/map/project pipelines.

**Workaround:** Same as above: set
`spark.sql.streaming.realTimeMode.allowlistCheck=false`. This has been
tested and works for windowed aggregations and `transformWithState` in the
companion repository's pipelines.

On **Databricks Runtime**, stateful RTM operators are fully supported and
validated.

**Outlook:** Stateful operator support in OSS RTM is expected to expand.
The SPIP (SPARK-52330) explicitly lists stateful operations as part of the
RTM roadmap.

---

## 9. Console Sink Does Not Flush in RTM

**What it means:** When using `.format("console")` as a sink in RTM, output
may not appear in the terminal. The console sink buffers output and does not
flush reliably in RTM's continuous execution model.

**Why it matters:** This is primarily a development and debugging issue. The
console sink is commonly used during development to inspect pipeline output.
When it does not flush, it appears as if the pipeline is producing no results.

**Workaround:** Use a Kafka sink and consume with a separate consumer
(e.g., `kafka-console-consumer.sh` or a Python consumer). Alternatively,
use `forEach` with explicit print and flush:

```python
def process_row(row):
    print(row, flush=True)

query = (df.writeStream
    .foreach(process_row)
    .outputMode("update")
    .trigger(realTime="5 minutes")
    .start())
```

**Outlook:** This is a known issue. The console sink was designed for
micro-batch mode where output is flushed at batch boundaries.

---

## 10. current_timestamp() Evaluates Once Per Checkpoint

**What it means:** In RTM, `current_timestamp()` returns the timestamp of
the last checkpoint boundary, not the wall-clock time when each row is
processed. All rows between two checkpoints get the same timestamp.

**Why it matters:** If you use `current_timestamp()` to measure processing
latency (e.g., `current_timestamp() - event_time`), the measurement will be
inaccurate. It may show latencies of minutes (the checkpoint interval)
rather than the actual processing time of milliseconds.

**Flink context:** In Flink, `System.currentTimeMillis()` returns the actual
wall-clock time per record. There is no equivalent gotcha.

**Workaround:** Measure latency externally. Write output to Kafka with the
source-embedded timestamp, then measure end-to-end latency at the consumer:

```python
# In the consumer (not in Spark):
latency_ms = consumer_wall_clock_ms - event["produced_at"]
```

This is the approach used in the companion repository's latency measurement
pipeline (Pipeline 05).

**Outlook:** This is inherent to RTM's architecture. The checkpoint boundary
is when expressions like `current_timestamp()` are evaluated. Per-row
wall-clock timestamps would require a different evaluation model.

---

## Summary Table

| Limitation | Severity | Workaround Available | Flink Supports It |
|---|---|---|---|
| No session windows | High (if needed) | Manual via transformWithState | Yes |
| No stream-stream joins | High (if needed) | Write to table + batch join | Yes |
| No event-time timers | Medium | Processing-time approximation | Yes |
| Update mode only | Medium | Downstream upsert logic | N/A (different model) |
| Kafka/Kinesis sources only | Medium | Bridge layer to Kafka | Yes (broad connectors) |
| Per-row state invocation | Low | Write iterator-safe code | Same model (per-record) |
| Python UDFs not in OSS allowlist | Medium (OSS only) | Bypass allowlist flag | N/A |
| Stateful ops not in OSS allowlist | High (OSS only) | Bypass allowlist flag | N/A |
| Console sink no flush | Low | Use Kafka sink for dev | N/A |
| current_timestamp() per checkpoint | Medium | External latency measurement | No equivalent issue |

---

## Recommendations

1. **Evaluate your pipeline against this list before committing to a
   migration.** If your pipeline requires session windows or stream-stream
   joins, RTM is not a viable target today.

2. **If you are on Databricks**, limitations 7 and 8 (allowlist issues)
   do not apply. Databricks Runtime has validated and enabled stateful
   RTM operators.

3. **If you are on OSS Spark 4.1**, expect to set
   `spark.sql.streaming.realTimeMode.allowlistCheck=false` for any
   non-trivial pipeline. Test thoroughly, as you are outside the
   validated operator set.

4. **Design for Update mode.** If your downstream systems expect append
   semantics, add deduplication or upsert logic at the consumer.

5. **Measure latency externally.** Do not rely on `current_timestamp()`
   for latency measurement in RTM pipelines.
