package com.flink_forge.simple_flink_pipeline;


import com.flink_forge.FlinkForgeApplication;
import com.flink_forge.common.config.ConfigUtil;
import com.flink_forge.common.config.KafkaDetailsFactory;
import com.flink_forge.common.dto.internal.KafkaDetails;
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
public class SimpleFlinkPipeline {


    private static KafkaSource<String> createKafkaSource(KafkaDetails kafkaDetails) {
        /**
         * - This method only builds a Kafka source configuration object
         * - No Kafka connection, polling, or threads start during .build()
         * - KafkaSource<String> source = createKafkaSource(); only stores the config in a variable
         * - Actual Kafka consumption starts only at env.execute(JOB_NAME);
         */
        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaDetails.getBootstrapServers())
                .setTopics(kafkaDetails.getSrcTopic())
                .setGroupId(kafkaDetails.getGroupId())
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
                        "Kafka Source")
                .name("Kafka Source");
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
                })
                .name("Process elements");
    }


    private static KafkaSink<String> createKafkaSink(KafkaDetails kafkaDetails) {
        /**
         *
         * - This attaches a Kafka sink-operator to the processed stream
         * - `sinkTo` in main() is a terminal operation
         * - This branch is now closed, no further operation can be applied to it (Data leaves flink)
         * - The current fn builds a KafkaSink object (config only), it still does NOT run anything in Flink
         * - Nothing happens until : env.execute()
         *
         */

        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaDetails.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaDetails.getSinkTopic())
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
        // We STILL need it even when submitting the JAR to Flink
        // [Why app.setRegisterShutdownHook(false) is done ?]
        // Register a JVM shutdown hook (A shutdown hook is simply a thread that the JVM executes just before the process terminates)
        // Spring Boot automatically registers one
        // We were getting classLoader error when running "flink run -d ..." (ie detached mode), so now we are not registering shutdown hook
        SpringApplication app = new SpringApplication(FlinkForgeApplication.class);
        app.setRegisterShutdownHook(false);
        ConfigurableApplicationContext context = app.run(args);


        // Get the KafkaDetails bean (already populated from properties)
        KafkaDetails kafkaDetails = KafkaDetailsFactory.fromConfig();

        // Step-2 : Setup flink env
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // set default parallelism for flink operator
        env.setParallelism(Integer.parseInt(ConfigUtil.get("flink.default-parallelism")));
        // prevents the UI showing only 1 block when parallelism=1
        env.disableOperatorChaining();

        // Step-3 : Read from kafka topic as source & convert to data-stream
        KafkaSource<String> source = createKafkaSource(kafkaDetails);
        DataStream<String> inputStream = createInputStream(env, source);

        // Step-4 : Process the data-stream
        DataStream<String> processedStream = processStream(inputStream);

        // Step-5 : Push to kafka sink
        processedStream
                .sinkTo(createKafkaSink(kafkaDetails))
                .name("Kafka Sink");

        // Step-6 : start flink job
        env.execute(ConfigUtil.get("job.simple-pipeline.name"));


        // Clean up Spring context after job completes
        // This is needed bcz : you've disabled the automatic cleanup, you become responsible for closing the context.
        // & Spring will never clean itself up automatically when the shutdown hook is disabled
        context.close();
    }

}
