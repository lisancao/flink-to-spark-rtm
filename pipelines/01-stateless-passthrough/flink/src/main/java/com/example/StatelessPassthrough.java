package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Pattern 1 -- Stateless Passthrough
 *
 * Reads JSON events from Kafka topic "benchmark-input", filters to keep only
 * "click" and "purchase" events (drops "view"), and writes the surviving
 * events unchanged to Kafka topic "benchmark-output".
 *
 * Expected JSON schema:
 *   { "event_id": "...", "event_type": "...", "key": "...",
 *     "value": ..., "produced_at": "..." }
 */
public class StatelessPassthrough {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        final String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        final String inputTopic = System.getenv().getOrDefault(
                "INPUT_TOPIC", "benchmark-input");
        final String outputTopic = System.getenv().getOrDefault(
                "OUTPUT_TOPIC", "benchmark-output");
        final String groupId = System.getenv().getOrDefault(
                "GROUP_ID", "flink-stateless-passthrough");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ---- Kafka source ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-source");

        // ---- Filter: keep click + purchase, drop view ----
        DataStream<String> filtered = events.filter(json -> {
            try {
                JsonNode node = MAPPER.readTree(json);
                String eventType = node.path("event_type").asText("");
                return "click".equals(eventType) || "purchase".equals(eventType);
            } catch (Exception e) {
                // Drop malformed records
                return false;
            }
        }).name("filter-click-purchase");

        // ---- Kafka sink ----
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        filtered.sinkTo(sink).name("kafka-sink");

        env.execute("Stateless Passthrough");
    }
}
