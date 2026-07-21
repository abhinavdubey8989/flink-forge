package com.flink_forge.basic_cep.pattern_process;


import com.flink_forge.basic_cep.dto.PatternSummary;
import com.flink_forge.common.dto.events.UserActivity;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;


@Slf4j
public class LoginCartInactiveProcessFunction
        extends PatternProcessFunction<UserActivity, PatternSummary> {

    @Override
    public void processMatch(
            Map<String, List<UserActivity>> match,
            Context context,
            Collector<PatternSummary> out) {


        UserActivity login = match.get("login").getFirst();
        List<UserActivity> addToCartEvents = match.get("addToCart");
        UserActivity inactive = match.get("inactive").getFirst();

        // add all itemValues
        int abandonedCartValue = addToCartEvents.stream()
                .map(UserActivity::getItemValue)
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .sum();

        // send to next operator
        out.collect(
                PatternSummary.builder()
                        .userId(login.getUserId())
                        .patternName("[LOGIN]__[ADD_TO_CART]__[INACTIVE]")
                        .cartItemCount(addToCartEvents.size())
                        .abandonedCartValue(abandonedCartValue)
                        .lastSeen(inactive.getTs())
                        .build()
        );
    }
}

