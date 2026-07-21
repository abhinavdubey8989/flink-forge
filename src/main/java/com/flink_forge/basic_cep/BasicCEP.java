package com.flink_forge.basic_cep;


import com.flink_forge.basic_cep.dto.PatternSummary;
import com.flink_forge.basic_cep.pattern.LoginCartInactivePattern;
import com.flink_forge.basic_cep.pattern_process.LoginCartInactiveProcessFunction;
import com.flink_forge.basic_cep.sink.PatternSummarySink;
import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.dto.internal.KafkaSourceDetails;
import com.flink_forge.common.source.KafkaSourceConfig;
import com.flink_forge.windowed_aggregation.config.KafkaSourceDetailsFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.flink_forge.common.env.FlinkEnvFactory;

import java.time.Duration;
import java.util.*;


@Slf4j
public class BasicCEP {

    public static void main(String[] args) throws Exception {

        // Create StreamExecutionEnvironment, it's the entry point for every Flink streaming application
        StreamExecutionEnvironment env = FlinkEnvFactory.create();

        // Get the KafkaSourceDetails (populated from properties)
        KafkaSourceDetails kafkaSourceDetails = KafkaSourceDetailsFactory.fromConfig();


        /**
         * - Create kafka-source, every Kafka message becomes one UserActivity object
         * - dataStream.assignTimestampsAndWatermarks(...)
         *      - CEP works on event time, not processing time
         *
         * - Without assignTimestampsAndWatermarks()
         *      - Events are processed in arrival order (processing time by default)
         *      - Late/out-of-order events may cause missed matches (MOST IMPORTANT)
         *      - Simpler, but less reliable for real-world streams
         *
         *
         * - With assignTimestampsAndWatermarks()
         *      - Events are processed using their event timestamps
         *      - Out-of-order events can still match within the allowed lateness
         *      - Recommended for Kafka streams that carry timestamps, makes the CEP engine robust against out-of-order
         *
         *
         * - [.<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(2))]
         *      - This tells Flink: Events may arrive as much as two seconds out of order
         *      - Notice this says nothing about where the timestamp comes from. It only defines how much disorder Flink should tolerate
         *      - Suppose the actual event timestamps are : [LOGIN @ ts=10 , CART @ ts = 11     , INACTIVE @ ts = 12]
         *         But Kafka delivers them as              : [LOGIN @ ts=10 , INACTIVE @ ts = 12 , CART @ ts = 12]
         *      - Because you've configured Duration.ofSeconds(2), Flink waits long enough to allow the
         *         CART event to arrive before deciding the event sequence is complete.
         *      - If CART arrived 5 seconds late, it would be considered too late.
         *
         * - [.withTimestampAssigner((event, previousTimestamp) -> event.getTs())]
         *      -  This tells Flink: Use UserActivity.ts as the event’s occurrence time
         *      - Flink cannot guess where the timestamp is
         *      - getTs() must return Unix epoch time in milliseconds
         *      - Without this, Flink may use no event timestamp or use the Kafka record timestamp
         *
         *
         * - [.withIdleness(Duration.ofSeconds(5))]
         *      - What you're telling Flink : If a Kafka partition doesn't receive any events for 5 seconds, consider it idle
         *      - This has nothing to do with late events & it solves a completely different problem
         *      - Imagine Kafka has two partitions.
         *             - Partition-0 : LOGIN, CART, LOGIN, CART
         *             - Partition-1 : (no events)
         *      - NOTE : Each partition generates its own watermark & The global watermark is always minimum(all partition watermarks)
         *         suppose
         *             - Partition-0 watermark = 100
         *             - Partition-1 watermark = 0
         *             - Thus, minimum watermark = 0
         *      - Without this, an inactive Kafka partition can block : CEP, Windows, Event-time timers, State cleanup
         *      - With this, Flink says: "Partition-1 appears inactive. I'll temporarily ignore its watermark."
         *      - Now only active partitions contribute.
         *             - Partition-0 watermark = 100
         *             - Partition-1 = IDLE
         *             - Global watermark becomes : 100
         *             - Everything continues normally
         */
        DataStream<UserActivity> originalDataStream = KafkaSourceConfig.create(env, kafkaSourceDetails);
        DataStream<UserActivity> dataStreamWithTimestampsAndWatermarks =
                originalDataStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, previousTimestamp) -> event.getTs())
                                .withIdleness(Duration.ofSeconds(5))
                );


        // sink-1 : print (only for debugging)
        // events.print();

        /**
         *
         * - Key the steam by user-id
         * - Without it, CEP would try matching patterns across every user, & give incorrect result
         * - The result is separate logical streams : Stream-for-user-1 : <....>, Stream-for-user-2 : <....> etc.
         */
        KeyedStream<UserActivity, String> keyedByUserIdStream =
                dataStreamWithTimestampsAndWatermarks.keyBy(UserActivity::getUserId);


        /**
         *
         * - LoginCartInactivePattern.create() :
         *     - creates the pattern
         *     - You're simply telling Flink-CEP : This is the sequence in keyedByUserIdStream I want to detect
         *
         * - CEP.pattern(...)
         *     - converts the normal stream into a PatternStream
         *     - i.e. DataStream<UserActivity> -> PatternStream<UserActivity>
         *     - A PatternStream continuously watches events and maintains partial matches in state
         *     - on LOGIN : Partial match created
         *     - on ADD_TO_CART : State becomes <LOGIN, ADD_TO_CART>
         *     - another ADD_TO_CART : State becomes <LOGIN, ADD_TO_CART, ADD_TO_CART> & still partial
         *     - Finally IN_ACTIVE : Pattern complete, now CEP emits a match
         *
         * - loginCartInactivePatternStream.process(new LoginCartInactiveProcessFunction())
         *     - Processes matched pattern
         *     - The process function receives all matched events
         *     - after this step : The output stream type changes
         *     - ie PatternStream<UserActivity> -> DataStream<LoginCartInactiveSummary>
         *
         *
         */
        PatternStream<UserActivity> loginCartInactivePatternStream =
                CEP.pattern(keyedByUserIdStream, LoginCartInactivePattern.create());

        DataStream<PatternSummary> patternSummaryDataStream =
                loginCartInactivePatternStream.process(new LoginCartInactiveProcessFunction());

        // Sink to kafka
        PatternSummarySink.create(patternSummaryDataStream);


        // Start Flink job
        env.execute(ConfigUtil.get("job.basic-cep.name"));
    }
}