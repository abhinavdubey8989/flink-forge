package com.flink_forge.common.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaDetails {
    private String bootstrapServers;
    private String srcTopic;
    private String sinkTopic;
    private String groupId;
}
