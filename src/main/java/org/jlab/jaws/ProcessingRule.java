package org.jlab.jaws;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.jlab.jaws.entity.IntermediateMonolog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public abstract class ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(ProcessingRule.class);

    KafkaStreams streams;
    Properties props;
    Topology top;
    String inputTopic;
    String outputTopic;

    public ProcessingRule(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public Properties constructProperties() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");
        registry = (registry == null) ? "http://localhost:8081" : registry;

        String tmpDir = System.getProperty("java.io.tmpdir");
        String stateDir = System.getenv("STATE_DIR");
        stateDir = (stateDir == null) ? tmpDir + File.separator + "kafka-streams" : stateDir;


        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        return props;
    }

    abstract Topology constructTopology(Properties props);

    public void start() {
        props = constructProperties();
        top = constructTopology(props);

        streams = new KafkaStreams(top, props);

        streams.start();
    }

    public void close() {
        streams.close();
    }

    public static void populateHeaders(Record<? extends Object, ? extends Object> record) {
        String host = "unknown";

        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.debug("Unable to obtain host name");
        }

        record.headers().add("user", System.getProperty("user.name").getBytes(StandardCharsets.UTF_8));
        record.headers().add("producer", "registrations2epics".getBytes(StandardCharsets.UTF_8));
        record.headers().add("host", host.getBytes(StandardCharsets.UTF_8));
    }

    public final class MonologAddHeadersFactory implements ProcessorSupplier<String, IntermediateMonolog, String, IntermediateMonolog> {

        /**
         * Return a new {@link Processor} instance.
         *
         * @return a new {@link Processor} instance
         */
        @Override
        public Processor<String, IntermediateMonolog, String, IntermediateMonolog> get() {
            return new Processor<>() {
                private ProcessorContext<String, IntermediateMonolog> context;

                @Override
                public void init(ProcessorContext<String, IntermediateMonolog> context) {
                    this.context = context;
                }

                @Override
                public void process(Record<String, IntermediateMonolog> input) {
                    log.debug("Handling message: {}={}", input.key(), input.value());

                    long timestamp = System.currentTimeMillis();

                    Record<String, IntermediateMonolog> output = new Record<>(input.key(), input.value(), timestamp);

                    populateHeaders(output);

                    context.forward(output);
                }

                @Override
                public void close() {
                    // Nothing to do
                }
            };
        }
    }
}
