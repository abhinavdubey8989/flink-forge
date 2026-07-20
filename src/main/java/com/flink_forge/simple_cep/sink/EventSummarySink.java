package com.flink_forge.simple_cep.sink;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.windowed_aggregation.dto.internal.EventSummary;
import com.flink_forge.windowed_aggregation.mapper.EventSummaryJsonSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * This class is responsible for writing/flushing the aggregated EventSummary records to a Kafka topic
 *
 */
public class EventSummarySink {

    public static void create(DataStream<EventSummary> stream) {

        /**
         *
         * @param stream - This method accepts a stream of aggregated event summaries
         * - The method creates a Kafka sink and attaches it to this stream
         * - setRecordSerializer(...) :
         *      - A Kafka message consists of Topic, Key (optional), Value.
         *      - This section tells Flink "How should a EventSummary become a Kafka record?"
         *      - Kafka stores bytes, not Java objects. So Flink needs a serializer.
         *
         */
        KafkaSink<EventSummary> sink =
                KafkaSink.<EventSummary>builder()
                        .setBootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(ConfigUtil.getDefaultOrJobSpecificConfig(
                                                null,
                                                "job.windowed-aggregation.kafka.sink-topic.event-aggregation"
                                        ))
                                        .setValueSerializationSchema(new EventSummaryJsonSerializer())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        stream.sinkTo(sink);
    }
}