package com.flink_forge.basic_cep.sink;

import com.flink_forge.basic_cep.dto.PatternSummary;
import com.flink_forge.basic_cep.mapper.PatternSummaryJsonSerializer;
import com.flink_forge.common.config.ConfigUtil;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;


public class PatternSummarySink {

    // The method creates a Kafka sink and attaches the sink to the provided DataStream
    public static void create(DataStream<PatternSummary> stream) {

        KafkaSink<PatternSummary> sink =
                KafkaSink.<PatternSummary>builder()
                        .setBootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(ConfigUtil.getDefaultOrJobSpecificConfig(
                                                null,
                                                "job.basic-cep.kafka.sink-topic"
                                        ))
                                        .setValueSerializationSchema(new PatternSummaryJsonSerializer())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        stream.sinkTo(sink);
    }
}