package com.flink_forge.basic_cep.pattern;

import com.flink_forge.common.dto.events.UserActivity;
import com.flink_forge.common.enums.EventType;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;


public class LoginCartInactivePattern {

    public static Pattern<UserActivity, ?> create() {

        return Pattern.<UserActivity>begin(
                        "login",
                        AfterMatchSkipStrategy.skipToNext())
                .where(
                        new SimpleCondition<UserActivity>() {
                            @Override
                            public boolean filter(UserActivity event) {
                                return event.getEventType() == EventType.LOGIN;
                            }
                        })
                .followedBy("addToCart")
                .where(
                        new SimpleCondition<UserActivity>() {
                            @Override
                            public boolean filter(UserActivity event) {
                                return event.getEventType() == EventType.ADD_TO_CART;
                            }
                        })
                .oneOrMore()
                .greedy()
                .followedBy("inactive")
                .where(
                        new SimpleCondition<UserActivity>() {
                            @Override
                            public boolean filter(UserActivity event) {
                                return event.getEventType() == EventType.IN_ACTIVE;
                            }
                        })
                .within(Duration.ofSeconds(5));


    }
}
