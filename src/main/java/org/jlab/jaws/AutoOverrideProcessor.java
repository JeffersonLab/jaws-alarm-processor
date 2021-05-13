package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.jlab.jaws.entity.ShelvedAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AutoOverrideProcessor {
    private static final Logger log = LoggerFactory.getLogger(AutoOverrideProcessor.class);

    public static final String INPUT_TOPIC = "shelved-alarms";
    public static final String OUTPUT_TOPIC = INPUT_TOPIC;

    public static final Serde<String> INPUT_KEY_SERDE = Serdes.String();
    public static final SpecificAvroSerde<ShelvedAlarm> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final Serde<String> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
    public static final SpecificAvroSerde<ShelvedAlarm> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

    /**
     * Enumerations of all channels with expiration timers, mapped to the cancellable Executor handle.
     */
    public static Map<String, Cancellable> channelHandleMap = new ConcurrentHashMap<>();

    static Properties getStreamsConfig() {

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

        final KStream<String, ShelvedAlarm> output = input.transform(new MsgTransformerFactory());

        output.to(OUTPUT_TOPIC, Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    /**
     * Factory to create Kafka Streams Transformer instances; references a stateStore to maintain previous
     * RegisteredAlarms.
     */
    private static final class MsgTransformerFactory implements TransformerSupplier<String, ShelvedAlarm, KeyValue<String, ShelvedAlarm>> {

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<String, ShelvedAlarm, KeyValue<String, ShelvedAlarm>> get() {
            return new Transformer<String, ShelvedAlarm, KeyValue<String, ShelvedAlarm>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, ShelvedAlarm> transform(String key, ShelvedAlarm value) {
                    KeyValue<String, ShelvedAlarm> result = null; // null returned to mean no record

                    log.debug("Handling message: {}={}", key, value);

                    // Get (and remove) timer handle (if exists)
                    Cancellable handle = channelHandleMap.remove(key);

                    // If exists, we always cancel timers
                    if (handle != null) {
                        log.debug("Timer Cancelled");
                        handle.cancel();
                    } else {
                        log.debug("No Timer exists");
                    }


                    if (value != null && value.getExpiration() > 0) { // Set new timer
                        Instant ts = Instant.ofEpochMilli(value.getExpiration());
                        Instant now = Instant.now();
                        long delayInSeconds = Duration.between(now, ts).getSeconds();
                        if (now.isAfter(ts)) {
                            delayInSeconds = 0; // If expiration is in the past then expire immediately
                        }
                        log.debug("Scheduling {} for delay of: {} seconds ", key, delayInSeconds);

                        Cancellable newHandle = context.schedule(Duration.ofSeconds(delayInSeconds), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                            log.debug("Punctuation triggered for: {}", key);

                            // Attempt to cancel timer immediately so only run once; can fail if schedule doesn't return fast enough before timer triggered!
                            Cancellable h = channelHandleMap.remove(key);
                            if(h != null) {
                                h.cancel();
                            }

                            context.forward(key, null);
                        });

                        Cancellable oldHandle = channelHandleMap.put(key, newHandle);

                        // This is to ensure we cancel every timer before losing it's handle otherwise it'll run forever (they repeat until cancelled)
                        if(oldHandle != null) { // This should only happen if timer callback is unable to cancel future runs (because handle assignment in map too slow)
                            oldHandle.cancel();
                        }
                    } else {
                        log.debug("Either null value or null expiration so no timer set!");
                    }

                    return result; // We never return anything but null here because records are produced async
                }

                @Override
                public void close() {
                    // Nothing to do
                }
            };
        }
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
