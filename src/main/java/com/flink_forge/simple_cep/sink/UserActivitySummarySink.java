package com.flink_forge.simple_cep.sink;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import com.flink_forge.windowed_aggregation.mapper.UserActivitySummaryJsonSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * This class is responsible for writing/flushing the aggregated UserActivitySummary records to a Kafka topic
 *
 */
public class UserActivitySummarySink {

    /**
     *
     * @param stream - This method accepts a stream of aggregated user summaries
     * - The method creates a Kafka sink and attaches it to this stream
     * - setRecordSerializer(...) :
     *      - A Kafka message consists of Topic, Key (optional), Value.
     *      - This section tells Flink "How should a UserActivitySummary become a Kafka record?"
     *      - Kafka stores bytes, not Java objects. So Flink needs a serializer.
     *
     */
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