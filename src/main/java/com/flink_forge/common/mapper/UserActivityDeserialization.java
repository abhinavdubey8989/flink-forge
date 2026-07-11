package com.flink_forge.common.mapper;

import com.flink_forge.common.dto.events.UserActivity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;


public class UserActivityDeserialization implements DeserializationSchema<UserActivity> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public UserActivity deserialize(byte[] message) throws IOException {
        try {
            return objectMapper.readValue(message, UserActivity.class);
        } catch (Exception e) {
            // TODO: handle bad messages
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(UserActivity nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserActivity> getProducedType() {
        return TypeInformation.of(UserActivity.class);
    }
}