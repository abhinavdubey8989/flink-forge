package com.flink_forge.windowed_aggregation.dto.internal;

import com.flink_forge.common.enums.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * - Represents the aggregated count for a specific event type across all users within a processing-time window.
 * - e.g. if users collectively generate 125 LOGIN events during a 10-second window, the summary would be:
 *
 * {
 * eventName = "LOGIN"
 * count = 125
 * }
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventSummary {

    private String eventType;

    /** Total number of occurrences of the event within the aggregation window. */
    private long count;
}
