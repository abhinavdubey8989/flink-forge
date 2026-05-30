package com.flink_forge.common.config;

import com.flink_forge.common.dto.internal.KafkaDetails;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaDetailsFactory {

    public static KafkaDetails fromConfig() {
        return KafkaDetails.builder()
                .bootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                .srcTopic(ConfigUtil.get("kafka.src-topic"))
                .sinkTopic(ConfigUtil.get("kafka.sink-topic"))
                .groupId(ConfigUtil.get("kafka.group-id"))
                .build();
    }

}
