package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.Instant;

/**
 * Pattern 2 -- Tumbling Window Count
 *
 * Reads JSON events from Kafka topic "benchmark-input", extracts event time
 * from the "produced_at" field, keys by "key", applies a 1-minute tumbling
 * window, and counts the number of events per key per window. Results are
 * printed to stdout.
 *
 * Expected JSON schema:
 *   { "event_id": "...", "event_type": "...", "key": "...",
 *     "value": ..., "produced_at": "2025-01-01T00:00:00Z" }
 */
public class TumblingWindowCount {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        final String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        final String inputTopic = System.getenv().getOrDefault(
                "INPUT_TOPIC", "benchmark-input");
        final String groupId = System.getenv().getOrDefault(
                "GROUP_ID", "flink-tumbling-window-count");

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
                                        return ts; // fall back to ingestion time
                                    }
                                });

        DataStream<String> events = env.fromSource(
                source, watermarkStrategy, "kafka-source");

        // ---- Parse to (key, 1L) tuples ----
        DataStream<Tuple2<String, Long>> keyed = events
                .map((MapFunction<String, Tuple2<String, Long>>) json -> {
                    JsonNode node = MAPPER.readTree(json);
                    String key = node.path("key").asText("unknown");
                    return Tuple2.of(key, 1L);
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG))
                .name("parse-key");

        // ---- 1-minute tumbling window, sum counts ----
        DataStream<Tuple2<String, Long>> windowCounts = keyed
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(1)
                .name("tumbling-window-count");

        windowCounts.print().name("stdout-sink");

        env.execute("Tumbling Window Count");
    }
}
