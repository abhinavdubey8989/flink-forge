package com.flink_forge.windowed_aggregation.aggregate.event_activity;

import com.flink_forge.common.dto.events.UserActivity;
import org.apache.flink.api.common.functions.AggregateFunction;


public class EventCountAggregationFunction
        implements AggregateFunction<
                UserActivity,
                Long,
                Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserActivity value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
