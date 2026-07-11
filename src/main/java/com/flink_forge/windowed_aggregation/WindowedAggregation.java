package com.flink_forge.windowed_aggregation;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.windowed_aggregation.dto.events.UserActivity;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import com.flink_forge.windowed_aggregation.env.FlinkEnvFactory;
import com.flink_forge.windowed_aggregation.pipeline.UserAggregationPipeline;
import com.flink_forge.windowed_aggregation.sink.UserActivitySummarySink;
import com.flink_forge.windowed_aggregation.source.KafkaSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Slf4j
public class WindowedAggregation {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvFactory.create();

        // kafka-source
        DataStream<UserActivity> inputStream = KafkaSourceConfig.create(env);

        DataStream<UserActivitySummary> userActivitySummary = UserAggregationPipeline.build(inputStream);

        UserActivitySummarySink.create(userActivitySummary);


        env.execute(ConfigUtil.get("job.windowed-aggregation.name"));
    }
}
