package com.flink_forge.common.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
public class KafkaSourceDetails {
    private String bootstrapServers;
    private String srcTopic;
    private String groupId;
}
