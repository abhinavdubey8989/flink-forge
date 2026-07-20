package com.flink_forge.simple_cep.pipeline;

import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.simple_cep.dto.LoginCartInactiveSummary;
import com.flink_forge.simple_cep.pattern.LoginCartInactivePattern;
import com.flink_forge.simple_cep.pattern_process.LoginCartInactiveProcessFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.streaming.api.datastream.DataStream;


public class EventPatternPipeline {

    public static DataStream<LoginCartInactiveSummary> build(
            DataStream<UserActivity> events) {

        return CEP.pattern(
                        events.keyBy(UserActivity::getUserId),
                        LoginCartInactivePattern.create())
                .process(new LoginCartInactiveProcessFunction());
    }
}
