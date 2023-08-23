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
 * Adds a Masked override to an alarm with an active parent alarm and removes the Masked override when the parent
 * alarm is no longer active.
 */
public class MaskRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(MaskRule.class);

    String overridesOutputTopic;

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<AlarmOverrideKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<AlarmOverrideUnion> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde MASK_STORE_KEY_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde MASK_STORE_VALUE_SERDE = new Serdes.StringSerde();

    public MaskRule(String inputTopic, String outputTopic, String overridesOutputTopic) {
        super(inputTopic, outputTopic);
        this.overridesOutputTopic = overridesOutputTopic;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-mask");

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

        // TODO: Foreign key join on maskedBy field?  Parent active/normal status part of computation
        // Computing parent effective state might be too much (parent overrides) - just use actual parent active or not?
        KStream<String, IntermediateMonolog> maskOverrideMonolog = monologStream.filter(new Predicate<String, IntermediateMonolog>() {
            @Override
            public boolean test(String key, IntermediateMonolog value) {
                System.err.println("Filtering: " + key + ", value: " + value);
                return value.getNotification().getOverrides().getMasked() == null && value.getTransitions().getTransitionToActive();
            }
        });

        KStream<AlarmOverrideKey, AlarmOverrideUnion> maskOverrides = maskOverrideMonolog.map(new KeyValueMapper<String, IntermediateMonolog, KeyValue<AlarmOverrideKey, AlarmOverrideUnion>>() {
            @Override
            public KeyValue<AlarmOverrideKey, AlarmOverrideUnion> apply(String key, IntermediateMonolog value) {
                return new KeyValue<>(new AlarmOverrideKey(key, OverriddenAlarmType.Masked), new AlarmOverrideUnion(new MaskedOverride()));
            }
        });

        maskOverrides.to(overridesOutputTopic, Produced.as("Mask-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));


        KStream<String, IntermediateMonolog> unmaskOverrideMonolog = monologStream.filter(new Predicate<String, IntermediateMonolog>() {
            @Override
            public boolean test(String key, IntermediateMonolog value) {
                System.err.println("Filtering: " + key + ", value: " + value);
                return value.getNotification().getOverrides().getMasked() != null && value.getTransitions().getTransitionToNormal();
            }
        });

        KStream<AlarmOverrideKey, AlarmOverrideUnion> unmaskOverrides = maskOverrideMonolog.map(new KeyValueMapper<String, IntermediateMonolog, KeyValue<AlarmOverrideKey, AlarmOverrideUnion>>() {
            @Override
            public KeyValue<AlarmOverrideKey, AlarmOverrideUnion> apply(String key, IntermediateMonolog value) {
                return new KeyValue<>(new AlarmOverrideKey(key, OverriddenAlarmType.Masked), null);
            }
        });

        unmaskOverrides.to(overridesOutputTopic, Produced.as("Unmask-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));


        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("MaskStateStore"),
                MASK_STORE_KEY_SERDE,
                MASK_STORE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<String, IntermediateMonolog> passthrough = monologStream.process(
                new MyProcessorSupplier(storeBuilder.name()),
                Named.as("MaskTransitionProcessor"),
                storeBuilder.name());

        passthrough.to(outputTopic, Produced.as("Mask-Passthrough")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
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
                    System.err.println("Processing key = " + input.key() + ", value = " + input.value());

                    // TODO: store and compute both masking and unmasking state
                    boolean masking = false;
                    boolean unmasking = false;

                    if(input.value().getNotification().getOverrides().getMasked() != null) {

                        // Check if already mask in-progress
                        masking = store.get(input.key()) != null;

                        // Check if we need to mask
                        boolean needToMask = input.value().getTransitions().getTransitionToActive();

                        if (needToMask) {
                            masking = true;
                        }
                    }

                    store.put(input.key(), masking ? "y" : null);

                    if (masking) { // Update transition state
                        //value.getTransitions().setMasking(true);
                    }

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
