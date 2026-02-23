package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Pattern 4 -- Keyed State Counter
 *
 * Reads JSON events from Kafka topic "benchmark-input", keys by the "key"
 * field, and uses a {@link KeyedProcessFunction} backed by
 * {@link ValueState} to maintain a running count of events per key.
 * On every incoming event the updated count is emitted as
 * "key=<key>  count=<n>".
 *
 * Expected JSON schema:
 *   { "event_id": "...", "event_type": "...", "key": "...",
 *     "value": ..., "produced_at": "..." }
 */
public class KeyedStateCounter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        final String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        final String inputTopic = System.getenv().getOrDefault(
                "INPUT_TOPIC", "benchmark-input");
        final String groupId = System.getenv().getOrDefault(
                "GROUP_ID", "flink-keyed-state-counter");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ---- Kafka source (processing time, no event-time needed) ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-source");

        // ---- Extract the key string from each JSON event ----
        DataStream<String> keyed = events
                .map(json -> {
                    JsonNode node = MAPPER.readTree(json);
                    return node.path("key").asText("unknown");
                })
                .returns(Types.STRING)
                .name("extract-key");

        // ---- KeyedProcessFunction with ValueState running counter ----
        DataStream<String> counts = keyed
                .keyBy(k -> k)
                .process(new CounterFunction())
                .name("keyed-state-counter");

        counts.print().name("stdout-sink");

        env.execute("Keyed State Counter");
    }

    /**
     * Maintains a per-key running count using Flink managed keyed state.
     * Each incoming element increments the counter and the new total is
     * emitted downstream.
     */
    public static class CounterFunction
            extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("event-count", Long.class);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String key,
                                   KeyedProcessFunction<String, String, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            Long current = countState.value();
            long newCount = (current == null) ? 1L : current + 1;
            countState.update(newCount);
            out.collect(String.format("key=%s  count=%d", key, newCount));
        }
    }
}
