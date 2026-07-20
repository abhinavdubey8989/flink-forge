package com.flink_forge.common.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import java.util.Map;


public class MapToJson implements SerializationSchema<Map<String, Object>> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(Map<String, Object> element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}