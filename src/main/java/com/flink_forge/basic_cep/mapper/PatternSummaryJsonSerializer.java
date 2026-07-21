package com.flink_forge.basic_cep.mapper;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink_forge.basic_cep.dto.PatternSummary;
import org.apache.flink.api.common.serialization.SerializationSchema;


public class PatternSummaryJsonSerializer
        implements SerializationSchema<PatternSummary> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(PatternSummary summary) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(summary);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize EventSummary", e);
        }
    }
}