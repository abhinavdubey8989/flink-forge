package com.flink_forge.windowed_aggregation.sink;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import com.flink_forge.windowed_aggregation.mapper.UserActivitySummaryJsonSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.*;


public class UserActivitySummarySink {

    public static void create(DataStream<UserActivitySummary> stream) {

        KafkaSink<UserActivitySummary> sink =
                KafkaSink.<UserActivitySummary>builder()
                        .setBootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(ConfigUtil.getDefaultOrJobSpecificConfig(
                                                null,
                                                "job.windowed-aggregation.kafka.sink-topic.user-aggregation"
                                        ))
                                        .setValueSerializationSchema(new UserActivitySummaryJsonSerializer())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        stream.sinkTo(sink);
    }
}