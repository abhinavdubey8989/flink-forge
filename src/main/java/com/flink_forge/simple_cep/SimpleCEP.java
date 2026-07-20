package com.flink_forge.simple_cep;

import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.dto.internal.KafkaSourceDetails;
import com.flink_forge.simple_cep.dto.LoginCartInactiveSummary;
import com.flink_forge.simple_cep.pipeline.EventPatternPipeline;
import com.flink_forge.common.source.KafkaSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.flink_forge.common.env.FlinkEnvFactory;



@Slf4j
public class SimpleCEP {

    // Create Flink execution environment
    StreamExecutionEnvironment env = FlinkEnvFactory.create();

//    KafkaSourceDetails kafkaSourceDetails =
//            KafkaSourceDetails.builder()
//                    .bootstrapServers()
//                    .groupId()
//                    .srcTopic()
//                    .build();

    // Read events from Kafka
//    DataStream<UserActivity> events =
//            KafkaSourceConfig.create(env,kafkaSourceConfig );

    // Detect LOGIN -> ADD_TO_CART+ -> IN_ACTIVE pattern
//    DataStream<LoginCartInactiveSummary> summaries =
//            EventPatternPipeline.build(events);

    // For now, print detected matches
//        summaries.print();

    // Start Flink job
//        env.execute("Simple CEP");
}
