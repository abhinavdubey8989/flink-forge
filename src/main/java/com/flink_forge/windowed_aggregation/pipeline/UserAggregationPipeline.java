package com.flink_forge.windowed_aggregation.pipeline;

import com.flink_forge.windowed_aggregation.aggregate.UserAggregationFunction;
import com.flink_forge.windowed_aggregation.aggregate.UserSummaryWindowFunction;
import com.flink_forge.windowed_aggregation.dto.events.UserActivity;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;


public class UserAggregationPipeline {

    private UserAggregationPipeline() {
    }

    public static DataStream<UserActivitySummary> build(
            DataStream<UserActivity> events) {

        /**
         * 1. [keyBy(UserActivity::getUserId)]
         * - Flink partitions the stream by userId
         * - Each key (i.e. user-id here) is processed independently i.e. 1 logical pipeline per user
         * - user1 -> Pipeline A, user2 -> Pipeline B, user3 -> Pipeline C. These pipelines can even run on different TaskManagers
         *
         *
         * 2. [window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))]
         * - Flink groups events into 10-second buckets (eg: 0-10 seconds, 10-20 seconds)
         * - Every user gets their own windows
         * - Tumbling Windows : windows never overlap
         *
         *
         * 3. [aggregate(new UserAggregationFunction(), new UserSummaryWindowFunction())]
         * - This has two components : UserAggregationFunction, UserSummaryWindowFunction
         * - UserAggregationFunction
         *      - This runs for every incoming event
         *      - as per the business logic Flink doesn't store every event, instead it only stores the accumulator/counts
         * - UserSummaryWindowFunction
         *      - When the window closes (after 10 seconds), Flink calls this once per unique key in the window
         *      - It receives : userId, window start & end, final accumulator from UserAggregationFunction
         *
         */
        return events
                .keyBy(UserActivity::getUserId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(
                        new UserAggregationFunction(),
                        new UserSummaryWindowFunction());
    }
}
