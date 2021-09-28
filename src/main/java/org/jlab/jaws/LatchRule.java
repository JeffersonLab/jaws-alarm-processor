package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Adds a Latched override for alarms registered as latching that become active.
 */
public class LatchRule extends AutoOverrideRule {

    private static final Logger log = LoggerFactory.getLogger(LatchRule.class);

    public static final String OUTPUT_TOPIC_PASSTHROUGH = "latch-processed-monolog";
    public static final String OUTPUT_TOPIC_OVERRIDE = "overridden-alarms";

    public static final String INPUT_TOPIC = "monolog";

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<MonologValue> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde LATCH_STORE_KEY_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde LATCH_STORE_VALUE_SERDE = new Serdes.StringSerde();

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-auto-override-processor-latch");

        return props;
    }

    @Override
    public Topology constructTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        MONOLOG_VALUE_SERDE.configure(config, false);

        OVERRIDE_KEY_SERDE.configure(config, true);
        OVERRIDE_VALUE_SERDE.configure(config, false);

        final KTable<String, MonologValue> monologTable = builder.table(INPUT_TOPIC,
                Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        final KStream<String, MonologValue> monologStream = monologTable.toStream();

        KStream<String, MonologValue> latchOverrideMonolog = monologStream.filter(new Predicate<String, MonologValue>() {
            @Override
            public boolean test(String key, MonologValue value) {
                //System.err.println("Filtering: " + key + ", value: " + value);
                return value.getEffectiveRegistered() != null && value.getEffectiveRegistered().getLatching() && value.getTransitions().getTransitionToActive();
            }
        });

        KStream<OverriddenAlarmKey, OverriddenAlarmValue> latchOverrides = latchOverrideMonolog.map(new KeyValueMapper<String, MonologValue, KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>>() {
            @Override
            public KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> apply(String key, MonologValue value) {
                return new KeyValue<>(new OverriddenAlarmKey(key, OverriddenAlarmType.Latched), new OverriddenAlarmValue(new LatchedAlarm()));
            }
        });

        latchOverrides.to(OUTPUT_TOPIC_OVERRIDE, Produced.as("Latch-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("LatchStateStore"),
                LATCH_STORE_KEY_SERDE,
                LATCH_STORE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<String, MonologValue> passthrough = monologStream.transform(
                new MsgTransformerFactory(storeBuilder.name()),
                Named.as("LatchTransitionProcessor"),
                storeBuilder.name());

        passthrough.to(OUTPUT_TOPIC_PASSTHROUGH, Produced.as("Latch-Passthrough")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        Topology top = builder.build();

        return top;
    }

    private static final class MsgTransformerFactory implements TransformerSupplier<String, MonologValue, KeyValue<String, MonologValue>> {

        private final String storeName;

        /**
         * Create a new MsgTransformerFactory.
         *
         * @param storeName The state store name
         */
        public MsgTransformerFactory(String storeName) {
            this.storeName = storeName;
        }

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<String, MonologValue, KeyValue<String, MonologValue>> get() {
            return new Transformer<String, MonologValue, KeyValue<String, MonologValue>>() {
                private KeyValueStore<String, String> store;
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.store = (KeyValueStore<String, String>) context.getStateStore(storeName);
                }

                @Override
                public KeyValue<String, MonologValue> transform(String key, MonologValue value) {
                    KeyValue<String, MonologValue> result = new KeyValue<>(key, value);

                    //System.err.println("Processing key = " + key + ", value = " + value);

                    // Skip the filter unless latching is registered
                    if(value.getEffectiveRegistered() != null && value.getEffectiveRegistered().getLatching()) {

                        // Check if already latching in-progress
                        boolean latching = store.get(key) != null;

                        // Check if latched
                        boolean latched = value.getOverrides().getLatched() != null;

                        // Check if we need to latch
                        boolean needToLatch = value.getTransitions().getTransitionToActive();

                        if (latched) {
                            latching = false;
                        } else if (needToLatch) {
                            latching = true;
                        }

                        if (latching) {
                            result = null; // filter out messages until latched!
                        }

                        //System.err.println("latched: " + latched);
                        //System.err.println("needToLatch: " + needToLatch);
                        //System.err.println("latching: " + latching);

                        store.put(key, latching ? "y" : null);
                    }

                    return result;
                }

                @Override
                public void close() {
                    // Nothing to do
                }
            };
        }
    }
}
