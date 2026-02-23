"""
Pipeline 1 -- Stateless Passthrough (Real-Time Mode)

Flink equivalent: a simple FilterFunction that keeps only "click" and
"purchase" events and forwards them unchanged to a Kafka sink.

Spark version: Structured Streaming with the Real-Time Mode trigger.
Because the pipeline is purely stateless (filter + project), RTM can
process each record the instant it arrives -- no micro-batch boundary
is required.

Input topic : benchmark-input   (JSON)
Output topic: benchmark-output  (JSON)
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
)

# ── importable helper lives one directory up ────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from rtm_trigger import with_real_time_trigger

# ── configuration ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC     = "benchmark-input"
OUTPUT_TOPIC    = "benchmark-output"
CHECKPOINT_DIR  = "/tmp/spark-checkpoints/01-stateless-passthrough"

# ── schema of the JSON events produced by the data generator ────────
EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("event_type",  StringType()),
    StructField("key",         StringType()),
    StructField("value",       IntegerType()),
    StructField("produced_at", LongType()),
])

KEEP_TYPES = ("click", "purchase")

# ── Spark session ──────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("01-stateless-passthrough-rtm")
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

# ── parse JSON and filter ──────────────────────────────────────────
events = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("evt"))
    .select("evt.*")
    .filter(col("event_type").isin(*KEEP_TYPES))
)

# ── build the Kafka sink value column ──────────────────────────────
output = events.select(
    col("event_id").alias("key"),
    to_json(
        struct("event_id", "event_type", "key", "value", "produced_at")
    ).alias("value"),
)

# ── write stream with RTM trigger ─────────────────────────────────
writer = (
    output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("update")
)

query = with_real_time_trigger(writer).start()
query.awaitTermination()
