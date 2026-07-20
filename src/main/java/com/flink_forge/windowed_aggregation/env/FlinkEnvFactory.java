package com.flink_forge.windowed_aggregation.env;

import com.flink_forge.common.config.ConfigUtil;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkEnvFactory {


    /**
     * Configures checkpoint storage and checkpoint behaviour.
     */
    private static void configureCheckpointing(
            StreamExecutionEnvironment env) {

        // ------------------------------------------------------------
        // Checkpoint storage
        // ------------------------------------------------------------
        Configuration configuration = new Configuration();

        configuration.set(
                CheckpointingOptions.CHECKPOINT_STORAGE,
                "filesystem");

        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                ConfigUtil.get("app.flink.checkpoint.dir"));

        configuration.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        env.configure(configuration);

        // ------------------------------------------------------------
        // Checkpoint behaviour
        // ------------------------------------------------------------
        env.enableCheckpointing(
                Long.parseLong(
                        ConfigUtil.get("app.flink.checkpoint.interval")));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointingConsistencyMode(
                CheckpointingMode.EXACTLY_ONCE);

        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        checkpointConfig.setCheckpointTimeout(
                Long.parseLong(
                        ConfigUtil.get("app.flink.checkpoint.timeout")));

        checkpointConfig.setMinPauseBetweenCheckpoints(
                Long.parseLong(
                        ConfigUtil.get("app.flink.checkpoint.min.pause")));

        checkpointConfig.setTolerableCheckpointFailureNumber(
                Integer.parseInt(
                        ConfigUtil.get(
                                "app.flink.checkpoint.tolerable.failure")));
    }


    public static StreamExecutionEnvironment create() {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        configureCheckpointing(env);

        // Prevent the UI from chaining all operators together
        env.disableOperatorChaining();

        // Default job-level parallelism
        env.setParallelism(
                Integer.parseInt(
                        ConfigUtil.getDefaultOrJobSpecificConfig(
                                "flink.default-parallelism",
                                null)));
        return env;
    }


}
