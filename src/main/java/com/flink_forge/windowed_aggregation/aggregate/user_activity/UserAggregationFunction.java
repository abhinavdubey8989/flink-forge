package com.flink_forge.windowed_aggregation.aggregate.user_activity;


import com.flink_forge.common.dto.events.UserActivity;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.*;


/**
 * - UserAggregationFunction tells Flink how to incrementally aggregate events.
 * - It implements: AggregateFunction<UserActivity, serActivityAccumulator, Map<String, Integer>>
 *
 */
public class UserAggregationFunction implements
        AggregateFunction<
                UserActivity, // IN - One incoming event schema
                UserActivityAccumulator, // ACC - Internal state maintained by Flink
                Map<String, Integer> // OUT : Final aggregation result
                > {


    /**
     * - Called by flink internally, once per unique keyBy (i.e. user-id here)
     * - This fn creates a fresh & empty accumulator
     *
     */
    @Override
    public UserActivityAccumulator createAccumulator() {
        return new UserActivityAccumulator();
    }


    /**
     *
     * @param value - the value/schema to add
     * @param accumulator - The accumulator to add the value to
     *
     * - This fn is called by flink, for every incoming event (ie UserActivity)
     *
     */
    @Override
    public UserActivityAccumulator add(UserActivity value,
                                       UserActivityAccumulator accumulator) {
        accumulator.increment(value.getEventType());
        return accumulator;
    }


    /**
     *
     * @param accumulator
     *
     * - When the window finishes, Flink asks: "What is the final aggregated value?"
     * - i.e. At the end of the window, Flink calls getResult fn, for each unique key in the window
     * - this fn does not create a UserActivitySummary. It only returns the aggregated map.
     *
     */
    @Override
    public Map<String, Integer> getResult(UserActivityAccumulator accumulator) {
        return accumulator.getEventCounts();
    }



    /**
     *
     * @param a - An accumulator to merge
     * @param b - Another accumulator to merge
     *
     * - This combines two accumulators
     * - But, for tumbling processing-time window, this method will normally never be invoked because tumbling-windows do not merge
     * - this fn exists because the AggregateFunction interface also supports merging window types such as session windows
     *
     */
    @Override
    public UserActivityAccumulator merge(UserActivityAccumulator a,
                                         UserActivityAccumulator b) {

        b.getEventCounts().forEach(
                (eventType, count) ->
                        a.getEventCounts().merge(eventType, count, Integer::sum));
        return a;
    }
}