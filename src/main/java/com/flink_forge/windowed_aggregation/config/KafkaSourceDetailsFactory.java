package com.flink_forge.windowed_aggregation.config;

import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.dto.internal.KafkaSourceDetails;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaSourceDetailsFactory {

    public static KafkaSourceDetails fromConfig() {
        return KafkaSourceDetails.builder()
                .bootstrapServers(ConfigUtil.get("kafka.bootstrap-servers"))
                .srcTopic(ConfigUtil.get("kafka.src-topic"))
                .groupId(ConfigUtil.getDefaultOrJobSpecificConfig(
                        "kafka.group-id",
                        "job.windowed-aggregation.kafka.group-id"
                ))
                .build();
    }
}
