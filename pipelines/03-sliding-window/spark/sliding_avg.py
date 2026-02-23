"""
Pipeline 3 -- Sliding Window Average (Micro-Batch)

Flink equivalent: a SlidingEventTimeWindows(5 min, 1 min) with an
AverageAggregateFunction over the "value" field, keyed by "key".

Spark version: Structured Streaming with a 5-minute sliding window
(1-minute slide), 10-second watermark, computing AVG(value) per key
per window.

Because windowed aggregation is stateful, this pipeline defaults to
micro-batch processing.  Set USE_RTM=1 to attempt the Real-Time Mode
trigger instead (requires Spark 4.x with RTM stateful support).

Input topic : benchmark-input  (JSON)
Output      : console (for inspection / benchmarking)
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, from_unixtime, round as spark_round, window,
)
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
CHECKPOINT_DIR  = "/tmp/spark-checkpoints/03-sliding-window"
WATERMARK       = "10 seconds"
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION  = "1 minute"

# ── schema ──────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("event_type",  StringType()),
    StructField("key",         StringType()),
    StructField("value",       IntegerType()),
    StructField("produced_at", LongType()),
])

# ── Spark session ──────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("03-sliding-window-avg")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)

# ── read stream ────────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
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
)

# ── watermark + sliding window aggregation ─────────────────────────
windowed = (
    events
    .withWatermark("event_time", WATERMARK)
    .groupBy(
        col("key"),
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
    )
    .agg(
        spark_round(avg("value"), 2).alias("avg_value"),
        count("*").alias("event_count"),
    )
    .select(
        col("key"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_value"),
        col("event_count"),
    )
)

# ── write stream ───────────────────────────────────────────────────
writer = (
    windowed.writeStream
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
