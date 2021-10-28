package org.jlab.jaws;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jlab.jaws.entity.IntermediateMonolog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

    void setHeaders(ProcessorContext context) {
        Headers headers = context.headers();

        if (headers != null) {
            log.debug("adding headers");

            String host = "unknown";

            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                log.debug("Unable to obtain host name");
            }

            headers.add("user", System.getProperty("user.name").getBytes(StandardCharsets.UTF_8));
            headers.add("producer", "jaws-effective-processor".getBytes(StandardCharsets.UTF_8));
            headers.add("host", host.getBytes(StandardCharsets.UTF_8));
        } else {
            log.debug("Headers are unavailable");
        }
    }

    public final class MonologAddHeadersFactory implements TransformerSupplier<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> {

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> get() {
            return new Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, IntermediateMonolog> transform(String key, IntermediateMonolog value) {
                    log.debug("Handling message: {}={}", key, value);

                    setHeaders(context);

                    return new KeyValue<>(key, value);
                }

                @Override
                public void close() {
                    // Nothing to do
                }
            };
        }
    }
}
