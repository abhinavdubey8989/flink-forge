package com.flink_forge.windowed_aggregation.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActivitySummary {
    private String userId;
    private Map<String, Integer> eventCounts;
}
