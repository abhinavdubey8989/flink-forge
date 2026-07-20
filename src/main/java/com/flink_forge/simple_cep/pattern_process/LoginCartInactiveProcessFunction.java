package com.flink_forge.simple_cep.pattern_process;


import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.simple_cep.dto.LoginCartInactiveSummary;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class LoginCartInactiveProcessFunction
        extends PatternProcessFunction<UserActivity, LoginCartInactiveSummary> {

    @Override
    public void processMatch(
            Map<String, List<UserActivity>> match,
            Context context,
            Collector<LoginCartInactiveSummary> out) {

        UserActivity login = match.get("login").getFirst();
        List<UserActivity> addToCartEvents = match.get("addToCart");
        UserActivity inactive = match.get("inactive").getFirst();

        out.collect(
                LoginCartInactiveSummary.builder()
                        .userId(login.getUserId())
                        .cartItemCount(addToCartEvents.size())
                        .lastSeen(inactive.getTs())
                        .build()
        );
    }
}

