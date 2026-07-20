package com.flink_forge.windowed_aggregation;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.dto.internal.KafkaSourceDetails;
import com.flink_forge.common.env.FlinkEnvFactory;
import com.flink_forge.windowed_aggregation.config.KafkaSourceDetailsFactory;
import com.flink_forge.windowed_aggregation.dto.internal.EventSummary;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import com.flink_forge.windowed_aggregation.pipeline.EventAggregationPipeline;
import com.flink_forge.windowed_aggregation.pipeline.UserAggregationPipeline;
import com.flink_forge.windowed_aggregation.sink.EventSummarySink;
import com.flink_forge.windowed_aggregation.sink.UserActivitySummarySink;
import com.flink_forge.common.source.KafkaSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Slf4j
public class WindowedAggregation {


    public static void main(String[] args) throws Exception {
        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        StreamExecutionEnvironment env = FlinkEnvFactory.create();

        // Get the KafkaDetails bean (already populated from properties)
        KafkaSourceDetails kafkaSourceDetails = KafkaSourceDetailsFactory.fromConfig();

        // kafka-source
        DataStream<UserActivity> inputStream = KafkaSourceConfig.create(env, kafkaSourceDetails);

        // user-level aggregation
        DataStream<UserActivitySummary> userActivitySummaryStream = UserAggregationPipeline.build(inputStream);
        UserActivitySummarySink.create(userActivitySummaryStream);

        // event-level aggregation
        DataStream<EventSummary> eventSummaryStream = EventAggregationPipeline.build(inputStream);
        EventSummarySink.create(eventSummaryStream);


        env.execute(ConfigUtil.get("job.windowed-aggregation.name"));
    }
}
