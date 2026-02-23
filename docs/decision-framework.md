# Flink to Spark RTM: Decision Framework

This document provides a structured framework for deciding whether to migrate
a Flink pipeline to Spark Real-Time Mode (RTM). It is designed for
engineering leads and architects evaluating RTM as a replacement for Flink
in their streaming infrastructure.

The framework consists of three parts:
1. A **disqualification checklist** (hard blockers that mean RTM is not viable)
2. A **fit assessment** (signals that RTM is a good match)
3. A **capability comparison table** for detailed evaluation

---

## Part 1: Disqualification Checklist

Answer these questions first. If any answer is "Yes," RTM is not a viable
migration target for that pipeline today.

```
Does your pipeline require session windows?
  YES --> Stay on Flink. RTM does not support session windows.
          Structured Streaming supports them in micro-batch Append mode,
          but not in RTM.

Does your pipeline require stream-stream joins?
  YES --> Stay on Flink. RTM does not support joining two unbounded
          streams. Stream-table joins (broadcast) are supported.

Does your pipeline require sub-10ms p99 latency?
  YES --> Stay on Flink. RTM delivers 5-15ms p99 for stateless and
          100-300ms p99 for stateful. Flink can achieve sub-millisecond
          latency for simple pipelines.

Does your pipeline require Complex Event Processing (CEP)?
  YES --> Stay on Flink. Flink has a dedicated CEP library for pattern
          detection over event streams. Spark has no equivalent.

Does your pipeline consume from Pub/Sub, Pulsar, or file sources?
  YES --> Stay on Flink (or add a Kafka bridge). RTM supports Kafka
          and Kinesis EFO only.

Does your pipeline use event-time timers in ProcessFunction?
  YES --> Evaluate carefully. RTM supports processing-time timers only.
          If deterministic replay and event-time semantics are critical,
          stay on Flink.

Does your pipeline use Flink's SideOutput / OutputTag?
  YES --> No direct equivalent in RTM. You can branch the DataFrame into
          multiple filtered sinks, but the programming model is different.
          Evaluate whether the workaround is acceptable.
```

If none of the above apply, proceed to the fit assessment.

---

## Part 2: Fit Assessment

These signals indicate that RTM is a good migration target. The more that
apply, the stronger the case for migration.

### Strong Fit: SQL-First or Python-Heavy Team

RTM pipelines are written with the DataFrame API, which is essentially SQL
expressed in Python or Scala. If your team is more comfortable with SQL and
DataFrames than with Flink's Java DataStream API, RTM reduces the skill
barrier.

```
Team's primary language?
  Python      --> RTM is a natural fit. PySpark is first-class.
  SQL         --> RTM is a natural fit. Spark SQL works in streaming.
  Scala       --> RTM is a good fit. Scala Structured Streaming is mature.
  Java only   --> Weaker fit. PySpark or Scala are the primary RTM APIs.
```

**Caveat (OSS Spark):** Python UDFs are not on the OSS RTM allowlist.
If your pipeline relies on Python UDFs, you need Databricks Runtime or
must bypass the allowlist check. Built-in functions and SQL expressions
work without restriction.

### Strong Fit: Already Using Spark for Batch

If your organization already runs Spark for batch ETL, ML training, or
data warehousing, adding RTM for streaming **unifies your stack**:

- One compute engine instead of two (Spark + Flink)
- One API to learn (DataFrame / SQL)
- One set of monitoring, deployment, and debugging tools
- Shared cluster resources between batch and streaming
- Unified data formats (read and write from the same Iceberg/Delta tables)

This operational simplification is often the strongest argument for
migration, even if RTM's feature set is narrower than Flink's.

### Strong Fit: Simple Stateful Patterns

If your stateful logic fits one of these patterns, RTM can handle it:

- **Running counters/accumulators** per key
- **Tumbling or sliding window** aggregations (count, sum, avg, min, max)
- **Deduplication** via `dropDuplicates`
- **Simple state machines** with a small number of states per key

These are covered by `transformWithState` and built-in windowed aggregations.

**Caveat (OSS Spark):** Stateful operators require bypassing the allowlist
on OSS Spark 4.1, or running on Databricks Runtime.

### Moderate Fit: Latency Requirements Between 10ms and 1 Second

RTM occupies a latency tier between Flink (sub-ms) and Spark micro-batch
(seconds):

```
Latency requirement?
  < 1ms       --> Flink. RTM cannot compete at this tier.
  1-10ms      --> Flink preferred. RTM stateless can sometimes achieve
                  this, but not reliably.
  10-300ms    --> RTM sweet spot. Stateless: 5-15ms. Stateful: 100-300ms.
  300ms-2s    --> Either RTM or micro-batch with processingTime("0").
  > 2s        --> Micro-batch is sufficient. No need for RTM.
```

### Weak Fit: Complex Stateful Logic

If your Flink pipeline uses:
- Multiple interacting state variables per key
- Event-time timers for timeout logic
- `CoProcessFunction` for two-stream stateful processing
- Custom watermark strategies (punctuated, idle-source-aware)

Then the migration will require significant rearchitecting. RTM's
`transformWithState` is capable but less flexible than Flink's
`KeyedProcessFunction` with its full timer and state API.

---

## Part 3: Flowchart

```
                    Should you migrate from Flink to Spark RTM?
                    ============================================

                           Need session windows?
                          /                      \
                        YES                       NO
                         |                         |
                   STAY ON FLINK           Need stream-stream joins?
                                          /                          \
                                        YES                           NO
                                         |                             |
                                   STAY ON FLINK             Need sub-10ms p99?
                                                            /                  \
                                                          YES                   NO
                                                           |                     |
                                                     STAY ON FLINK         Need CEP patterns?
                                                                          /                  \
                                                                        YES                   NO
                                                                         |                     |
                                                                   STAY ON FLINK         Source is Kafka
                                                                                         or Kinesis?
                                                                                        /            \
                                                                                       NO            YES
                                                                                        |              |
                                                                               Can add Kafka      Already use Spark
                                                                                  bridge?          for batch?
                                                                              /          \        /            \
                                                                            NO           YES    YES             NO
                                                                             |             |      |               |
                                                                       STAY ON         (continue) |          Team is
                                                                        FLINK              |      |        Python/SQL?
                                                                                           v      v        /         \
                                                                                    +-----------------------+         NO
                                                                                    | EVALUATE RTM:         |          |
                                                                                    | - Prototype pipeline  |     STAY ON
                                                                                    | - Test with allowlist |      FLINK
                                                                                    |   bypass (OSS) or    |   (weak fit)
                                                                                    |   Databricks Runtime |
                                                                                    | - Measure latency    |
                                                                                    | - Validate output    |
                                                                                    |   mode compatibility |
                                                                                    +-----------------------+
                                                                                              |
                                                                                              v
                                                                                    Meets latency and
                                                                                    correctness needs?
                                                                                    /                \
                                                                                  YES                 NO
                                                                                   |                   |
                                                                              MIGRATE TO          STAY ON
                                                                              SPARK RTM            FLINK
```

---

## Part 4: Capability Comparison

| Capability | Flink | Spark RTM | Spark Micro-Batch |
|---|---|---|---|
| **Stateless transforms** | Yes | Yes | Yes |
| **Tumbling windows** | Yes | Yes | Yes |
| **Sliding windows** | Yes | Yes | Yes |
| **Session windows** | Yes | **No** | Yes (Append mode) |
| **Stream-stream joins** | Yes | **No** | Yes |
| **Stream-table joins** | Yes | Yes (broadcast) | Yes |
| **Custom stateful logic** | KeyedProcessFunction | transformWithState | transformWithState |
| **Event-time timers** | Yes | **No** | Yes |
| **Processing-time timers** | Yes | Yes | Yes |
| **Exactly-once** | Yes (Kafka transactions) | Yes (checkpoint-based) | Yes (checkpoint-based) |
| **At-least-once** | Yes | N/A (exactly-once only) | N/A |
| **Output modes** | Emit per record | Update only | Append, Update, Complete |
| **Latency (stateless)** | Sub-ms | 5-15ms p99 | 100ms+ |
| **Latency (stateful)** | Low ms | 100-300ms p99 | Seconds |
| **Kafka source** | Yes | Yes | Yes |
| **Kinesis source** | Yes | Yes (EFO only) | Yes |
| **Pub/Sub source** | Yes | **No** | Yes (via connector) |
| **Pulsar source** | Yes | **No** | Yes (via connector) |
| **File source** | Yes | **No** | Yes |
| **Python API** | PyFlink (limited) | PySpark (full DataFrame) | PySpark (full DataFrame) |
| **Scala API** | Yes | Yes | Yes |
| **Java API** | Yes (primary) | Limited | Limited |
| **SQL interface** | Flink SQL | Spark SQL | Spark SQL |
| **CEP (Complex Event Processing)** | Yes (FlinkCEP) | **No** | **No** |
| **Async I/O** | AsyncDataStream | **No** | **No** |
| **Side outputs** | OutputTag | **No** | **No** |
| **Savepoints (portable snapshots)** | Yes | **No** | **No** |
| **Per-operator naming** | Yes (.name()) | **No** | **No** |
| **Cluster management** | Standalone, YARN, K8s | Standalone, YARN, K8s, Databricks | Same |
| **Unified batch + stream** | DataStream + Table API | DataFrame API (same API) | DataFrame API |
| **Managed cloud service** | Confluent, AWS KDA, Ververica | Databricks, AWS EMR, Dataproc | Same |

---

## Part 5: Migration Cost Estimation

Use this table to estimate the effort required for each pipeline pattern:

| Pipeline Pattern | Migration Effort | Notes |
|---|---|---|
| Stateless filter/map/project | **Low** (hours) | Near-direct translation. Fewest behavioral differences. |
| Tumbling window aggregation | **Low-Medium** (days) | Direct API mapping. Test Update mode output semantics. |
| Sliding window aggregation | **Low-Medium** (days) | Same as tumbling. Monitor state size for large slide ratios. |
| Simple keyed state (counter, accumulator) | **Medium** (days) | Translate ProcessFunction to StatefulProcessor. Test per-row invocation. |
| Complex keyed state (multi-state, timers) | **High** (weeks) | Rearchitect timer logic. Processing-time only. May require design changes. |
| Stream-stream join | **Not possible** | Blocker. Stay on Flink or redesign as stream-table + batch join. |
| Session window | **Not possible** | Blocker. Stay on Flink or implement manually in transformWithState. |
| CEP pattern | **Not possible** | Blocker. No equivalent in Spark. |

---

## Part 6: Recommended Migration Strategy

If the assessment indicates RTM is viable, follow this phased approach:

### Phase 1: Stateless Pipelines (1-2 weeks)

Migrate the simplest pipelines first: Kafka-to-Kafka filter/map/project.
These have the fewest behavioral differences and the most dramatic latency
improvement (seconds to milliseconds).

**Success criteria:** Latency measurements confirm sub-20ms p99. Output
correctness validated against Flink pipeline running in parallel.

### Phase 2: Windowed Aggregations (2-4 weeks)

Migrate tumbling and sliding window pipelines. Focus testing on:
- Update mode output semantics (multiple updates per window)
- Watermark behavior and late data handling
- State size under production load

**Success criteria:** Aggregation results match Flink output (accounting
for Update mode multiple-emit behavior). Latency under 500ms p99.

### Phase 3: Stateful Pipelines (4-8 weeks)

Migrate `ProcessFunction` pipelines to `transformWithState`. This requires
the most code changes:
- Translate `ValueState`/`ListState`/`MapState` to Spark equivalents
- Replace event-time timers with processing-time timers (if acceptable)
- Test per-row invocation behavior in RTM

**Success criteria:** State correctness validated over extended run (24+
hours). Timer behavior acceptable for business requirements.

### Phase 4: Decommission Flink (ongoing)

Run Spark RTM pipelines in parallel with Flink for a validation period.
Compare outputs, latency, and resource utilization. Decommission Flink
pipelines one by one after validation.

---

## Summary Decision Matrix

| Your Situation | Recommendation |
|---|---|
| Simple stateless pipelines, Python team, already on Spark | **Migrate.** RTM is a clear win. |
| Windowed aggregations, Kafka source, Databricks customer | **Migrate.** Full stateful support on Databricks. |
| Complex stateful with event-time timers | **Wait.** Event-time timer support may come in future releases. |
| Stream-stream joins required | **Stay on Flink.** No workaround in RTM. |
| Session windows required | **Stay on Flink.** Manual implementation is fragile. |
| Sub-1ms latency required | **Stay on Flink.** RTM floor is ~5ms for stateless. |
| Pub/Sub or Pulsar source | **Stay on Flink** (or add Kafka bridge if feasible). |
| Java-only team, no Spark experience | **Stay on Flink.** Migration cost is too high for the benefit. |
| Mixed batch + streaming, want unified stack | **Migrate stateless now, stateful incrementally.** |
