package com.flink_forge.basic_cep;


import com.flink_forge.basic_cep.pattern.LoginCartInactivePattern;
import com.flink_forge.basic_cep.pattern_process.LoginCartInactiveProcessFunction;
import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.dto.internal.KafkaSourceDetails;
import com.flink_forge.basic_cep.dto.LoginCartInactiveSummary;
import com.flink_forge.common.enums.EventType;
import com.flink_forge.common.source.KafkaSourceConfig;
import com.flink_forge.windowed_aggregation.config.KafkaSourceDetailsFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.flink_forge.common.env.FlinkEnvFactory;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.*;


@Slf4j
public class BasicCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvFactory.create();

        // Get the KafkaSourceDetails (populated from properties)
        KafkaSourceDetails kafkaSourceDetails = KafkaSourceDetailsFactory.fromConfig();

        // Read events from Kafka
        DataStream<UserActivity> events =
                KafkaSourceConfig.create(env, kafkaSourceDetails)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                        .withTimestampAssigner(
                                                (event, previousTimestamp) -> event.getTs()
                                        )
                                        .withIdleness(Duration.ofSeconds(10))
                        );


        // sink-1 : print
        events.print();

        // Detect pattern : LOGIN -> ADD_TO_CART+ -> IN_ACTIVE
        DataStream<LoginCartInactiveSummary> summaries =
                CEP.pattern(events.keyBy(UserActivity::getUserId),
                                LoginCartInactivePattern.create())
                        .process(new LoginCartInactiveProcessFunction());

        // For now, print detected matches
        summaries.print();

        // Start Flink job
        env.execute(ConfigUtil.get("job.basic-cep.name"));
    }


    public static void test(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<UserActivity> events = env.fromData(
                new UserActivity("1", "u1", EventType.LOGIN, 1L),
                new UserActivity("2", "u1", EventType.ADD_TO_CART, 2L),
                new UserActivity("3", "u1", EventType.IN_ACTIVE, 3L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserActivity>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (event, previousTimestamp) -> event.getTs()
                        )
        );

        events.print();

        Pattern<UserActivity, ?> pattern =
                Pattern.<UserActivity>begin("a")
                        .where(new SimpleCondition<UserActivity>() {
                            @Override
                            public boolean filter(UserActivity value) {
                                return true;
                            }
                        })
                        .next("b")
                        .where(new SimpleCondition<UserActivity>() {
                            @Override
                            public boolean filter(UserActivity value) {
                                return true;
                            }
                        })
                        .within(Duration.ofSeconds(5));

        CEP.pattern(events.keyBy(UserActivity::getUserId),
                        pattern)
                .process(new PatternProcessFunction<UserActivity, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<UserActivity>> match,
                            Context ctx,
                            Collector<String> out) {
                        System.out.println("MATCH!");
                        out.collect("MATCH");
                    }
                })
                .print();

        env.execute("test");
    }

}