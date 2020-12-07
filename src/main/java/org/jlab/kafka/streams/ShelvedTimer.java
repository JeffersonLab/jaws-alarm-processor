package org.jlab.kafka.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.jlab.kafka.alarms.ShelvedAlarm;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ShelvedTimer {
    private static final Logger LOGGER = Logger.getLogger("org.jlab.kafka.streams.ShelvedTimer");

    public static final String INPUT_TOPIC = "shelved-alarms";
    public static final String OUTPUT_TOPIC = INPUT_TOPIC;

    public static final Serde<String> INPUT_KEY_SERDE = Serdes.String();
    public static final SpecificAvroSerde<ShelvedAlarm> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final Serde<String> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
    public static final SpecificAvroSerde<ShelvedAlarm> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

    public static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * Enumerations of all channels with expiration timers, mapped to the cancellable Executor handle.
     */
    public static Map<String, ScheduledFuture> channelHandleMap = new ConcurrentHashMap<>();

    static Properties getStreamsConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");

        registry = (registry == null) ? "http://localhost:8081" : registry;

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shelved-timer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Create the Kafka Streams Domain Specific Language (DSL) Topology.
     *
     * @param props The streams configuration
     * @return The Topology
     */
    static Topology createTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        INPUT_VALUE_SERDE.configure(config, false);

        final KStream<String, ShelvedAlarm> input = builder.stream(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        input.foreach((key, value) -> {
            ScheduledFuture handle = channelHandleMap.get(key);

            // Clear expiration timer
            if(value == null || value.getExpiration() == null) {
                if(handle != null) {
                    handle.cancel(false);
                }
                channelHandleMap.remove(key);
            } else { // Possibly set timer
                if(handle != null) {
                    // already running, do nothing!
                } else {
                    Instant ts = Instant.ofEpochMilli(value.getExpiration());
                    Instant now = Instant.now();
                    long delayInSeconds = Duration.between(now, ts).getSeconds();
                    if(now.isAfter(ts)) {
                        delayInSeconds = 0; // If expiration is in the past then expire immediately
                    }
                    handle = scheduler.schedule(() -> {
                        sendTombstone(key);
                        channelHandleMap.remove(key);
                    },delayInSeconds, TimeUnit.SECONDS);
                    channelHandleMap.put(key, handle);
                }
            }
        });

        return builder.build();
    }

    static void sendTombstone(String channel) {

    }

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {
        final Properties props = getStreamsConfig();
        final Topology top = createTopology(props);
        final KafkaStreams streams = new KafkaStreams(top, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                scheduler.shutdown();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
