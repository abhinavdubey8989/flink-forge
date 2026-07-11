package com.flink_forge.windowed_aggregation.source;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.mapper.UserActivityDeserialization;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 *
 * - This class is responsible for reading UserActivity events from a Kafka topic & converting them into a Flink DataStream<UserActivity>
 * - It is the entry point of your Flink pipeline.
 *
 */
public class KafkaSourceConfig {

    /**
     *
     * - It accepts Flink's execution environment and returns a stream of UserActivity objects
     * - setStartingOffsets(...) : This tells Flink where to start reading
     * - setValueOnlyDeserializer(...) :
     *      - Kafka stores messages as raw bytes
     *      - custom deserializer converts byte[] into UserActivity
     *      - Without this deserializer, Flink would only see raw bytes
     */
    public static DataStream<UserActivity> create(
            StreamExecutionEnvironment env) {

        KafkaSource<UserActivity> source =
                KafkaSource.<UserActivity>builder()
                        .setBootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                        .setTopics(ConfigUtil.get("kafka.src-topic"))
                        .setGroupId(ConfigUtil.getDefaultOrJobSpecificConfig(
                                "kafka.group-id",
                                "job.windowed-aggregation.kafka.group-id"
                        ))
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new UserActivityDeserialization())
                        .build();

        /**
         * env.fromSource(...) :
         *   - This registers the source with the Flink execution environment.
         *   - Now Flink continuously polls Kafka for new messages.
         *   - Each consumed message becomes one element in the returned DataStream<UserActivity>
         *
         * name(...)
         *      - This assigns a readable name to the operator. You'll see this name in:
         *      - Flink Dashboard, Execution graph, Logs, Metrics instead of a generic operator name.
         *
         * uid(...)
         *      - The UID uniquely identifies this operator across job submissions.
         *      - This is especially important for stateful jobs because Flink uses UIDs to map operator state during upgrades or restarts.
         *      - Using stable UIDs helps preserve state compatibility when the job evolves.
         */
        return env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                ).name("Kafka Source")
                .uid("Kafka Source");
    }
}
