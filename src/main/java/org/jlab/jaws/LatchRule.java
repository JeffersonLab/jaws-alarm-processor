package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
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
 * Adds a Latched override for alarms registered as latchable that become active.
 */
public class LatchRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(LatchRule.class);

    String overridesOutputTopic;

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<AlarmOverrideKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
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

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-latch");

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
                return value.getRegistration().getClass$() != null && Boolean.TRUE.equals(value.getRegistration().getClass$().getLatchable()) && value.getTransitions().getTransitionToActive();
            }
        });

        KStream<AlarmOverrideKey, AlarmOverrideUnion> latchOverrides = latchOverrideMonolog.map(new KeyValueMapper<String, IntermediateMonolog, KeyValue<AlarmOverrideKey, AlarmOverrideUnion>>() {
            @Override
            public KeyValue<AlarmOverrideKey, AlarmOverrideUnion> apply(String key, IntermediateMonolog value) {
                return new KeyValue<>(new AlarmOverrideKey(key, OverriddenAlarmType.Latched), new AlarmOverrideUnion(new LatchedOverride()));
            }
        });

        latchOverrides.to(overridesOutputTopic, Produced.as("Latch-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("LatchStateStore"),
                LATCH_STORE_KEY_SERDE,
                LATCH_STORE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<String, IntermediateMonolog> passthrough = monologStream.process(
                new MyProcessorSupplier(storeBuilder.name()),
                Named.as("LatchTransitionProcessor"),
                storeBuilder.name());

        passthrough.to(outputTopic, Produced.as("Latch-Passthrough")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        Topology top = builder.build();

        return top;
    }

    private static final class MyProcessorSupplier implements ProcessorSupplier<String, IntermediateMonolog, String, IntermediateMonolog> {

        private final String storeName;

        /**
         * Create a new ProcessorSupplier.
         *
         * @param storeName The state store name
         */
        public MyProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        /**
         * Return a new {@link Processor} instance.
         *
         * @return a new {@link Processor} instance
         */
        @Override
        public Processor<String, IntermediateMonolog, String, IntermediateMonolog> get() {
            return new Processor<>() {
                private KeyValueStore<String, String> store;
                private ProcessorContext<String, IntermediateMonolog> context;

                @Override
                public void init(ProcessorContext<String, IntermediateMonolog> context) {
                    this.context = context;
                    this.store = context.getStateStore(storeName);
                }

                @Override
                public void process(Record<String, IntermediateMonolog> input) {
                    log.debug("Processing key = {}, value = \n\tInstance: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}",
                            input.key(),
                            input.value().getRegistration().getInstance(),
                            input.value().getNotification().getActivation(),
                            input.value().getNotification().getOverrides(),
                            input.value().getTransitions());

                    long timestamp = System.currentTimeMillis();

                    Record<String, IntermediateMonolog> output = new Record<>(input.key(), input.value(), timestamp);

                    // Skip the filter unless latchable is registered
                    if(output.value().getRegistration().getClass$() != null
                            && Boolean.TRUE.equals(output.value().getRegistration().getClass$().getLatchable())) {

                        // Check if already latching in-progress
                        boolean latching = store.get(output.key()) != null;

                        // Check if latched
                        boolean latched = output.value().getNotification().getOverrides().getLatched() != null;

                        // Check if we need to latch
                        boolean needToLatch = output.value().getTransitions().getTransitionToActive();

                        if (latched) {
                            latching = false;
                        } else if (needToLatch) {
                            latching = true;
                        }

                        if (latching) { // Update transition state
                            output.value().getTransitions().setLatching(true);
                        }

                        log.debug("latched: " + latched);
                        log.debug("needToLatch: " + needToLatch);
                        log.debug("latching: " + latching);

                        store.put(output.key(), latching ? "y" : null);
                    }

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
