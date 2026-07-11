package com.flink_forge.windowed_aggregation.aggregate;

import java.util.HashMap;
import java.util.Map;


public class UserActivityAccumulator {

    private final Map<String, Integer> eventCounts = new HashMap<>();

    public void increment(String eventType) {
        eventCounts.merge(eventType, 1, Integer::sum);
    }

    public Map<String, Integer> getEventCounts() {
        return eventCounts;
    }
}
