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
public class LatchRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(LatchRule.class);

    String overridesOutputTopic;

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<AlarmOverrideUnion> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde LATCH_STORE_KEY_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde LATCH_STORE_VALUE_SERDE = new Serdes.StringSerde();

    public LatchRule(String inputTopic, String outputTopic, String overridesOutputTopic) {
        super(inputTopic, outputTopic);
        this.overridesOutputTopic = overridesOutputTopic;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-alarm-processor-latch");

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

        final KTable<String, IntermediateMonolog> monologTable = builder.table(inputTopic,
                Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        final KStream<String, IntermediateMonolog> monologStream = monologTable.toStream();

        KStream<String, IntermediateMonolog> latchOverrideMonolog = monologStream.filter(new Predicate<String, IntermediateMonolog>() {
            @Override
            public boolean test(String key, IntermediateMonolog value) {
                log.debug("Filtering: " + key + ", value: " + value);
                return value.getRegistration().getCalculated() != null && Boolean.TRUE.equals(value.getRegistration().getCalculated().getLatching()) && value.getTransitions().getTransitionToActive();
            }
        });

        KStream<OverriddenAlarmKey, AlarmOverrideUnion> latchOverrides = latchOverrideMonolog.map(new KeyValueMapper<String, IntermediateMonolog, KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>>() {
            @Override
            public KeyValue<OverriddenAlarmKey, AlarmOverrideUnion> apply(String key, IntermediateMonolog value) {
                return new KeyValue<>(new OverriddenAlarmKey(key, OverriddenAlarmType.Latched), new AlarmOverrideUnion(new LatchedOverride()));
            }
        });

        latchOverrides.to(overridesOutputTopic, Produced.as("Latch-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("LatchStateStore"),
                LATCH_STORE_KEY_SERDE,
                LATCH_STORE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<String, IntermediateMonolog> passthrough = monologStream.transform(
                new MsgTransformerFactory(storeBuilder.name()),
                Named.as("LatchTransitionProcessor"),
                storeBuilder.name());

        passthrough.to(outputTopic, Produced.as("Latch-Passthrough")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        Topology top = builder.build();

        return top;
    }

    private static final class MsgTransformerFactory implements TransformerSupplier<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> {

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
        public Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> get() {
            return new Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>>() {
                private KeyValueStore<String, String> store;
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.store = (KeyValueStore<String, String>) context.getStateStore(storeName);
                }

                @Override
                public KeyValue<String, IntermediateMonolog> transform(String key, IntermediateMonolog value) {
                    log.debug("Processing key = {}, value = \n\tReg: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}", key, value.getRegistration().getCalculated(),value.getActivation().getActual(),value.getActivation().getOverrides(),value.getTransitions());

                    // Skip the filter unless latching is registered
                    if(value.getRegistration().getCalculated() != null && Boolean.TRUE.equals(value.getRegistration().getCalculated().getLatching())) {

                        // Check if already latching in-progress
                        boolean latching = store.get(key) != null;

                        // Check if latched
                        boolean latched = value.getActivation().getOverrides().getLatched() != null;

                        // Check if we need to latch
                        boolean needToLatch = value.getTransitions().getTransitionToActive();

                        if (latched) {
                            latching = false;
                        } else if (needToLatch) {
                            latching = true;
                        }

                        if (latching) { // Update transition state
                            value.getTransitions().setLatching(true);
                        }

                        log.debug("latched: " + latched);
                        log.debug("needToLatch: " + needToLatch);
                        log.debug("latching: " + latching);

                        store.put(key, latching ? "y" : null);
                    }

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
