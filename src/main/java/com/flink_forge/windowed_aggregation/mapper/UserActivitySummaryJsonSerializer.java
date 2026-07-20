package com.flink_forge.windowed_aggregation.mapper;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink_forge.windowed_aggregation.dto.internal.UserActivitySummary;
import org.apache.flink.api.common.serialization.SerializationSchema;



public class UserActivitySummaryJsonSerializer
        implements SerializationSchema<UserActivitySummary> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(UserActivitySummary summary) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(summary);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize UserActivitySummary", e);
        }
    }
}