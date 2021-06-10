package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.jlab.jaws.entity.OverriddenAlarmKey;
import org.jlab.jaws.entity.OverriddenAlarmType;
import org.jlab.jaws.entity.OverriddenAlarmValue;
import org.jlab.jaws.entity.ShelvedAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Expires Shelved overrides by setting timers.
 */
public class ShelveExpirationRule extends AutoOverrideRule {

    private static final Logger log = LoggerFactory.getLogger(ShelveExpirationRule.class);

    public static final String INPUT_TOPIC = "overridden-alarms";
    public static final String OUTPUT_TOPIC = INPUT_TOPIC;

    public static final SpecificAvroSerde<OverriddenAlarmKey> INPUT_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmKey> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
    public static final SpecificAvroSerde<OverriddenAlarmValue> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

    /**
     * Enumerations of all channels with expiration timers, mapped to the cancellable Executor handle.
     */
    public static Map<String, Cancellable> channelHandleMap = new ConcurrentHashMap<>();

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-auto-override-processor-shelve-expiration");

        return props;
    }

    /**
     * Create the Kafka Streams Domain Specific Language (DSL) Topology.
     *
     * @return The Topology
     */
    public Topology constructTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, String> config = new HashMap<>();

        String value =  props.getProperty(SCHEMA_REGISTRY_URL_CONFIG);

        config.put(SCHEMA_REGISTRY_URL_CONFIG, value);
        INPUT_KEY_SERDE.configure(config, true);
        INPUT_VALUE_SERDE.configure(config, false);

        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> input = builder.stream(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> shelvedOnly = input.filter(new Predicate<OverriddenAlarmKey, OverriddenAlarmValue>() {
            @Override
            public boolean test(OverriddenAlarmKey key, OverriddenAlarmValue value) {
                return key.getType() == OverriddenAlarmType.Shelved;
            }
        });

        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> output = shelvedOnly.transform(new MsgTransformerFactory());

        output.to(OUTPUT_TOPIC, Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    /**
     * Factory to create Kafka Streams Transformer instances; references a stateStore to maintain previous
     * RegisteredAlarms.
     */
    private static final class MsgTransformerFactory implements TransformerSupplier<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> {

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> get() {
            return new Transformer<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> transform(OverriddenAlarmKey key, OverriddenAlarmValue value) {
                    KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = null; // null returned to mean no record

                    log.debug("Handling message: {}={}", key, value);

                    // Get (and remove) timer handle (if exists)
                    Cancellable handle = channelHandleMap.remove(key.getName());

                    // If exists, we always cancel timers
                    if (handle != null) {
                        log.debug("Timer Cancelled");
                        handle.cancel();
                    } else {
                        log.debug("No Timer exists");
                    }


                    ShelvedAlarm sa = null;

                    if(value != null && value.getMsg() instanceof ShelvedAlarm) {
                        sa = (ShelvedAlarm) value.getMsg();
                    }

                    if (sa != null && sa.getExpiration() > 0) { // Set new timer
                        Instant ts = Instant.ofEpochMilli(sa.getExpiration());
                        Instant now = Instant.now();
                        long delayInSeconds = Duration.between(now, ts).getSeconds();
                        if (now.isAfter(ts)) {
                            delayInSeconds = 0; // If expiration is in the past then expire immediately
                        }
                        log.debug("Scheduling {} for delay of: {} seconds ", key, delayInSeconds);

                        Cancellable newHandle = context.schedule(Duration.ofSeconds(delayInSeconds), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                            log.debug("Punctuation triggered for: {}", key);

                            // Attempt to cancel timer immediately so only run once; can fail if schedule doesn't return fast enough before timer triggered!
                            Cancellable h = channelHandleMap.remove(key.getName());
                            if(h != null) {
                                h.cancel();
                            }

                            Headers headers = context.headers();

                            if(headers != null) {
                                log.debug("adding headers");

                                String host = "unknown";

                                try {
                                    host = InetAddress.getLocalHost().getHostName();
                                } catch (UnknownHostException e) {
                                    log.debug("Unable to obtain host name");
                                }

                                headers.add("user", System.getProperty("user.name").getBytes(StandardCharsets.UTF_8));
                                headers.add("producer", "jaws-auto-override-processor".getBytes(StandardCharsets.UTF_8));
                                headers.add("host", host.getBytes(StandardCharsets.UTF_8));
                            } else{
                                log.debug("Headers are unavailable");
                            }

                            context.forward(key, null);
                        });

                        Cancellable oldHandle = channelHandleMap.put(key.getName(), newHandle);

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
}
