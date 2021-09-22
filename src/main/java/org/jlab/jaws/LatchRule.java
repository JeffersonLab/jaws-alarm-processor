package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    public static final Serdes.StringSerde ACTIVE_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<ActiveAlarm> ACTIVE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

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


        final StoreBuilder<KeyValueStore<String, ActiveAlarm>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("PreviousActiveStateStore"),
                ACTIVE_KEY_SERDE,
                ACTIVE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        // TODO: set headers inside transform
        final KStream<String, MonologValue> output = monologTable.toStream().transform(
                new MsgTransformerFactory(storeBuilder.name()),
                Named.as("MyStatefulBranchProcessor"),
                storeBuilder.name());

        output.to(OUTPUT_TOPIC_PASSTHROUGH, Produced.as("Latch-Passthrough")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        Topology top = builder.build();

        top.addSink("latch-override", OUTPUT_TOPIC_OVERRIDE, OVERRIDE_KEY_SERDE.serializer(),
                OVERRIDE_VALUE_SERDE.serializer(), "MyStatefulBranchProcessor");

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
                private KeyValueStore<String, MonologProcessingValue> store;
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.store = (KeyValueStore<String, MonologProcessingValue>) context.getStateStore(storeName);
                }

                @Override
                public KeyValue<String, MonologValue> transform(String key, MonologValue value) {
                    KeyValue<String, MonologValue> result = new KeyValue<>(key, value);

                    // Only process non-tombstone msg where latching is registered
                    if(value != null && value.getRegistered().getLatching() != null && value.getRegistered().getLatching()) {

                        String latchState = "INIT";

                        MonologProcessingValue previous = store.get(key);

                        if(previous == null) {
                            previous = new MonologProcessingValue(null, null, new ArrayList<>(), latchState);
                        } else {
                            latchState = previous.getLatchState();
                        }

                        // TODO: check if change from active to not-active occurred, then update latchState?
                        boolean unactivated = false;
                        boolean currentlyLatched = false;

                        for(OverriddenAlarmValue over: value.getOverrides()) { // Check if currently Latched
                            if(over.getMsg() instanceof LatchedAlarm) {
                                currentlyLatched = true;
                                break;
                            }
                        }

                        if(previous.getActive() != null && value.getActive() == null) {
                            unactivated = true;
                        }

                        if(previous.getLatchState().equals("LATCHED") && !currentlyLatched) { // Check if Acknowledged
                            if(unactivated) {
                                latchState = "INIT";
                            } else {
                                latchState = "ACKED_BUT_NEED_INACTIVATE";
                            }
                        } else if(currentlyLatched) {
                            latchState = "LATCHED";
                        }

                        if (!latchState.equals("LATCHED") && !latchState.equals("LATCHING") && // We're not LATCHED or LATCHING
                                (previous.getActive() == null) &&
                                value.getActive() != null) {  // And a change to active occurred
                                    latchState = "LATCHING";

                                    // Add Latch Record
                                    // TODO: This prob must be MonoValue format then map to overridden format outside
                                    //context.forward(new OverriddenAlarmKey(key, OverriddenAlarmType.Latched), new OverriddenAlarmValue(new LatchedAlarm()), To.child("latch-override"));
                        }

                        if(latchState.equals("LATCHING")) {
                            result = null; // Don't pass any more records through until latch override set!
                        }

                        // For next time
                        System.err.println(store + ", " + key + ", " + value);
                        store.put(key, new MonologProcessingValue(value.getRegistered(), value.getActive(), value.getOverrides(), latchState));

                    }

                    log.trace("Transformed: {}={} -> {}", key, value, result);

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
