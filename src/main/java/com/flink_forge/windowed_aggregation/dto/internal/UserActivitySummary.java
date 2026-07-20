package com.flink_forge.windowed_aggregation.dto.internal;

import com.flink_forge.common.enums.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;


/**
 * - Represents the aggregated activity summary for a single user within a processing-time window.
 * - Produced by the Flink windowed-aggregation pipeline for each event type for a user during the window.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActivitySummary {
    private String userId;

    /**
     * Mapping of event type to the number of times it occurred within the
     * aggregation window.
     *
     * {
     *     "LOGIN": 2,
     *     "VIEW": 5,
     *     "ADD_TO_CART": 1
     * }
     *
     */
    private Map<EventType, Integer> eventCounts;
}
