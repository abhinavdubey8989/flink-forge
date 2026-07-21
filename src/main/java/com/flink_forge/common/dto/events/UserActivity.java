package com.flink_forge.common.dto.events;

import com.flink_forge.common.enums.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * - Represents a single user-activity event pushed in Kafka topic
 * - Each instance corresponds to one user action (eg: LOGIN, VIEW etc.)
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActivity {

    private String eventId;
    private String userId;
    private EventType eventType;
    private Integer itemValue; // Optional
    private long ts; // short for timestamp (epoch in milliseconds)
}
