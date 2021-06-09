package org.jlab.jaws;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public abstract class AutoOverrideRule {
    KafkaStreams streams;
    Properties props;
    Topology top;

    public Properties constructProperties() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");
        registry = (registry == null) ? "http://localhost:8081" : registry;

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-auto-override-processor");
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
}
