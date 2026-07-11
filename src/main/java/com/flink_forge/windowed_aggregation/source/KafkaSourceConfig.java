package com.flink_forge.windowed_aggregation.source;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.windowed_aggregation.dto.events.UserActivity;
import com.flink_forge.windowed_aggregation.mapper.UserActivityDeserialization;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaSourceConfig {

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

        return env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                ).name("Kafka Source")
                .uid("Kafka Source");
    }
}
