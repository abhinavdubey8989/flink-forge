package com.flink_forge.simple_flink_pipeline;


import com.flink_forge.FlinkForgeApplication;
import com.flink_forge.common.config.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;


@Slf4j
public class SimpleFlinkPipelineMain {

    // private static final String JOB_NAME = ConfigUtil.get("flink.job-name");
    // private static final String JOB_NAME = "flnk-1";
    // private static final String BOOTSTRAP_SERVERS = "kafka:9092"; //ConfigUtil.get("kafka.bootstrap-servers");
    private static final String KAFKA_SRC_TOPIC = "flink_src_topic_1" ;// ConfigUtil.get("kafka.src-topic");
    private static final String KAFKA_SINK_TOPIC = "flink_sink_topic_1" ; // ConfigUtil.get("kafka.sink-topic");
    private static final String GROUP_ID = "grp.flink-forge"; //ConfigUtil.get("kafka.group-id");


    private static KafkaSource<String> createKafkaSource() {
        /**
         * - This method only builds a Kafka source configuration object
         * - No Kafka connection, polling, or threads start during .build()
         * - KafkaSource<String> source = createKafkaSource(); only stores the config in a variable
         * - Actual Kafka consumption starts only at env.execute(JOB_NAME);
         */
        String BOOTSTRAP_SERVERS = ConfigUtil.get("kafka.bootstrap-servers");
        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_SRC_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }


    private static DataStream<String> createInputStream(
            StreamExecutionEnvironment env,
            KafkaSource<String> source) {
        /**
         *
         * - This is the moment where KafkaSource becomes part of the Flink execution graph
         * - Before this KafkaSource = just a config object
         * - After this : It becomes a Source Operator in the DAG
         * - This fn returns a DataStream<String> : This is Flink’s core abstraction for streaming data
         * - DataStream (here stream of strings) is logical stream of records flowing through operators
         * - `env.fromSource` registers a Source Operator in Flink
         * - At runtime, inside TaskManager, this becomes : KafkaSourceReader -> KafkaConsumer (internal) -> poll() loop
         */
        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source: " + KAFKA_SRC_TOPIC
        );
    }


    private static DataStream<String> processStream(DataStream<String> inputStream) {
        /**
         * - This is a dummy function, returns a hardcoded value, only for testing & simplicity purposes
         * - Using a map operator here, ie. for each input in kafka topic, there will a hardcoded value in output datastream
         *
         */
        return inputStream.map(value -> {
            log.info("Processing message: " + value);
            return "{"
                    + "\"timestamp\": \"" + System.currentTimeMillis() + "\", "
                    + "\"source\": \"flink-job\", "
                    + "\"input\": \"" + value + "\""
                    + "}";
        });
    }


    private static KafkaSink<String> createKafkaSink() {
        /**
         *
         * - This attaches a Kafka sink-operator to the processed stream
         * - `sinkTo` in main() is a terminal operation
         * - This branch is now closed, no further operation can be applied to it (Data leaves flink)
         * - The current fn builds a KafkaSink object (config only), it still does NOT run anything in Flink
         * - Nothing happens until : env.execute()
         *
         */
        String BOOTSTRAP_SERVERS = ConfigUtil.get("kafka.bootstrap-servers");

        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(KAFKA_SINK_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }


    public static void main(String[] args) throws Exception {
        // Step-1 : Start Spring Boot
        // Before this: no Spring, no beans, no Environment (as needed in ConfigUtil)
        // After this: Spring fully initialized
        // FlinkForgeApplication.class : This tells Spring to start scanning from this class/package
        // We STILL need it even when submitting the JAR to Flink,
        // Note : The returned value is not used, because your ConfigUtil depends on Spring Environment.
        ConfigurableApplicationContext context =
                SpringApplication.run(FlinkForgeApplication.class, args);

        // Step-2 : Setup flink env
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // set default parallelism for flink operator
        // env.setParallelism(Integer.parseInt(ConfigUtil.get("flink.default-parallelism")));
        env.setParallelism(1);
        // prevents the UI showing only 1 block when parallelism=1
        env.disableOperatorChaining();

        // Step-3 : Read from kafka topic as source & convert to data-stream
        KafkaSource<String> source = createKafkaSource();
        DataStream<String> inputStream = createInputStream(env, source);

        // Step-4 : Process the data-stream
        DataStream<String> processedStream = processStream(inputStream);

        // Step-5 : Push to kafka sink
        processedStream.sinkTo(createKafkaSink());

        // Step-6 : start flink job
        //env.execute(JOB_NAME);
        env.execute(ConfigUtil.get("flink.job-name"));
    }

}
