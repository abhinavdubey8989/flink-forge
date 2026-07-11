package com.flink_forge.windowed_aggregation.dto.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActivity {

    private String eventId;
    private String userId;
    private String eventType;
    private long ts; // short for timestamp (epoch in milliseconds)
}
