package com.flink_forge.windowed_aggregation.aggregate;

import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;


public class UserSummaryWindowFunction extends
        ProcessWindowFunction<
                Map<String, Integer>, // IN - input value, in this case, result of UserAggregationFunction
                UserActivitySummary, // OUT - expected result schema as result of this process fn
                String, // KEY - key from keyBy, here user-id
                TimeWindow // W - type of window
                > {

    /**
     *
     * @param userId :  from keyBy
     * @param context :
     *    - The context contains information about the current window
     *    - We can use context.window().getStart(), context.window().getEnd() in UserActivitySummary if needed
     * @param elements
     *  - You might expect Map<String,Integer> here, since that is the result of UserAggregationFunction
     *  - But we get Iterable<Map<String,Integer>>, bcz ProcessWindowFunction is designed to work even when multiple elements are available
     *  - However, when it is combined with an AggregateFunction, the aggregation has already reduced all events into one accumulator
     *  - So, elements always contains exactly one item
     *
     * @param out :
     *  - This is how you emit data downstream
     *  - Think of it as return statement in simple java program, except a Flink operator can emit zero, one, or many records
     *
     *
     * [Explaination]
     * - process() is called once for every key (ie per user-id here) in the window when its window closes
     * - When a time window closes, Flink will receive a Map<String,Integer> for one user & it'll emit one UserActivitySummary
     * - This class is responsible for creating the final output object when a window closes
     * - It does not perform the aggregation itself, that has already been done by UserAggregationFunction
     *
     *
     *
     */
    @Override
    public void process(
            String userId,
            Context context,
            Iterable<Map<String, Integer>> elements,
            Collector<UserActivitySummary> out) {

        // since elements will have only 1 item, iterator().next() gets that element
        Map<String, Integer> eventCounts = elements.iterator().next();

        // Why are we creating new HashMap<>(...)
        // - This is done to make a defensive copy
        // - The accumulator map belongs to Flink's managed state. By copying it into a new HashMap, your UserActivitySummary owns its own independent map
        // - preventing accidental modification if the original map is reused or mutated internally
        out.collect(
                new UserActivitySummary(
                        userId,
                        new HashMap<>(eventCounts)));
    }
}
