"""
Pipeline 4 -- Keyed State Counter (transformWithState)

Flink equivalent: a KeyedProcessFunction that maintains a ValueState<Long>
per key, incrementing it with every incoming event and emitting the running
count.

Spark version: Structured Streaming with transformWithState (Spark 4.x).
A StatefulProcessor keeps a per-key ValueState[int] that is incremented on
each input row and emitted downstream.

Because this requires managed keyed state, the pipeline defaults to
micro-batch processing.  Set USE_RTM=1 to attempt the Real-Time Mode
trigger instead (requires Spark 4.x with RTM stateful support).

Input topic : benchmark-input  (JSON)
Output      : console (for inspection / benchmarking)

NOTE: Requires RocksDB state store provider for production use.
      Configure via spark.sql.streaming.stateStore.providerClass.
"""

import os
import sys

from pyspark.sql import SparkSession
from typing import Iterator

from pyspark.sql import Row
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import TimerValues
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
)

# ── optional RTM import ────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
USE_RTM = os.environ.get("USE_RTM", "0") == "1"
if USE_RTM:
    from rtm_trigger import with_real_time_trigger

# ── configuration ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC     = "benchmark-input"
CHECKPOINT_DIR  = "/tmp/spark-checkpoints/04-keyed-state"
WATERMARK       = "10 seconds"

# ── schema ──────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("event_type",  StringType()),
    StructField("key",         StringType()),
    StructField("value",       IntegerType()),
    StructField("produced_at", LongType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("key",           StringType(),  False),
    StructField("running_count", IntegerType(), False),
])


# ── StatefulProcessor: per-key running counter ─────────────────────
class KeyedCounterProcessor(StatefulProcessor):
    """
    Maintains an integer counter per grouping key.  Every incoming row
    increments the counter by 1 and yields (key, running_count).

    This mirrors Flink's ValueState<Long> inside a KeyedProcessFunction.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.count_state = handle.getValueState(
            "count", StructType([StructField("count", IntegerType())]))

    def handleInputRows(self, key: tuple, rows: Iterator[Row],
                        timerValues: TimerValues) -> Iterator[Row]:
        # Retrieve current count (0 if this is a new key)
        current = self.count_state.get()[0] if self.count_state.exists() else 0

        # Increment for each row in this batch
        for _ in rows:
            current += 1

        self.count_state.update((current,))

        # Emit the latest running count
        yield Row(key=key[0], running_count=current)

    def close(self) -> None:
        pass


# ── Spark session ──────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("04-keyed-state-counter")
    .config("spark.sql.shuffle.partitions", "20")
    .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    )
    .getOrCreate()
)

# ── read stream ────────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

# ── parse JSON ─────────────────────────────────────────────────────
events = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("evt"))
    .select("evt.*")
    .withColumn("event_time",
        from_unixtime(col("produced_at") / 1000).cast("timestamp"))
    .withWatermark("event_time", WATERMARK)
)

# ── stateful transformation ────────────────────────────────────────
counted = events.groupBy("key").transformWithState(
    KeyedCounterProcessor(),
    outputStructType=OUTPUT_SCHEMA,
    outputMode="Update",
    timeMode="None",
)

# ── write stream ───────────────────────────────────────────────────
writer = (
    counted.writeStream
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("update")
)

if USE_RTM:
    query = with_real_time_trigger(writer).start()
else:
    query = writer.trigger(processingTime="10 seconds").start()

query.awaitTermination()
