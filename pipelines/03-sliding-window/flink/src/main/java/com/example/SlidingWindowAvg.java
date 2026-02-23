package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * Pattern 3 -- Sliding Window Average
 *
 * Reads JSON events from Kafka topic "benchmark-input", extracts event time
 * from "produced_at", keys by "key", applies a sliding window (5-minute
 * window, 1-minute slide), and computes the average of the "value" field per
 * key per window. Results are printed to stdout.
 *
 * Expected JSON schema:
 *   { "event_id": "...", "event_type": "...", "key": "...",
 *     "value": 42.0, "produced_at": "2025-01-01T00:00:00Z" }
 */
public class SlidingWindowAvg {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        final String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        final String inputTopic = System.getenv().getOrDefault(
                "INPUT_TOPIC", "benchmark-input");
        final String groupId = System.getenv().getOrDefault(
                "GROUP_ID", "flink-sliding-window-avg");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ---- Kafka source with event-time watermarks ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy =
                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<String>) (json, ts) -> {
                                    try {
                                        JsonNode node = MAPPER.readTree(json);
                                        String producedAt = node.path("produced_at").asText();
                                        return Instant.parse(producedAt).toEpochMilli();
                                    } catch (Exception e) {
                                        return ts;
                                    }
                                });

        DataStream<String> events = env.fromSource(
                source, watermarkStrategy, "kafka-source");

        // ---- Parse to (key, value) tuples ----
        DataStream<Tuple2<String, Double>> parsed = events
                .map(json -> {
                    JsonNode node = MAPPER.readTree(json);
                    String key = node.path("key").asText("unknown");
                    double value = node.path("value").asDouble(0.0);
                    return Tuple2.of(key, value);
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE))
                .name("parse-key-value");

        // ---- Sliding window (5 min window, 1 min slide), compute average ----
        DataStream<String> averages = parsed
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new AvgAggregate(), new AvgWindowFunction())
                .name("sliding-window-avg");

        averages.print().name("stdout-sink");

        env.execute("Sliding Window Average");
    }

    // ----------------------------------------------------------------
    // Accumulator: (sum, count)
    // ----------------------------------------------------------------

    /**
     * Incremental aggregation: accumulates sum and count so the final
     * average can be computed when the window fires.
     */
    public static class AvgAggregate
            implements AggregateFunction<Tuple2<String, Double>,
                                         Tuple2<Double, Long>,
                                         Double> {

        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return Tuple2.of(0.0, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(Tuple2<String, Double> value,
                                         Tuple2<Double, Long> acc) {
            return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Long> acc) {
            return acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a,
                                            Tuple2<Double, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    /**
     * Attaches the window key and window boundaries to the aggregated
     * average and emits a human-readable string.
     */
    public static class AvgWindowFunction
            extends ProcessWindowFunction<Double, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            ProcessWindowFunction<Double, String, String, TimeWindow>.Context ctx,
                            Iterable<Double> elements,
                            Collector<String> out) {
            Double avg = elements.iterator().next();
            long windowStart = ctx.window().getStart();
            long windowEnd   = ctx.window().getEnd();
            out.collect(String.format(
                    "key=%s  window=[%s .. %s)  avg_value=%.4f",
                    key,
                    Instant.ofEpochMilli(windowStart),
                    Instant.ofEpochMilli(windowEnd),
                    avg));
        }
    }
}
