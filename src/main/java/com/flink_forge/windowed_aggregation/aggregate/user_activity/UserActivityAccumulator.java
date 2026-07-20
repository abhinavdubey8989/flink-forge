package com.flink_forge.windowed_aggregation.aggregate.user_activity;

import com.flink_forge.common.enums.EventType;

import java.util.HashMap;
import java.util.Map;


/**
 * - This class is simply a container that holds the running aggregation state for a user within a window
 * - Precisely, it is a mutable state-object that Flink keeps for each (user + window) level
 * - It is not responsible for reading Kafka, managing windows, or emitting results.
 * - Its only job is to keep track of event counts as new events arrive.
 */
public class UserActivityAccumulator {

    private final Map<EventType, Integer> eventCounts = new HashMap<>();

    /**
     * - This method increments the count for a particular event type
     *   in the accumulator of a given key (i.e. user-id)
     *
     */
    public void increment(EventType eventType) {
        eventCounts.merge(eventType, 1, Integer::sum);
    }


    /**
     * - When the window ends, Flink calls UserAggregationFunction.getResult
     * - The above flow internally calls this : getEventCounts fn
     *
     */
    public Map<EventType, Integer> getEventCounts() {
        return eventCounts;
    }
}
