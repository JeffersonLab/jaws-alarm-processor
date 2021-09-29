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

import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Streams rule to join all the alarm topics into a single topic that is ordered (single partition) such that
 * processing can be done.   A store of the previous active record for each alarm is used to determine
 * transitions from active to normal and back.
 */
public class MonologRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(MonologRule.class);

    public static final String OUTPUT_TOPIC = "monolog";

    public static final String INPUT_TOPIC_REGISTERED = "registered-alarms";
    public static final String INPUT_TOPIC_CLASSES = "registered-classes";
    public static final String INPUT_TOPIC_ACTIVE = "active-alarms";
    public static final String INPUT_TOPIC_OVERRIDDEN = "overridden-alarms";

    public static final Serdes.StringSerde INPUT_KEY_REGISTERED_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde INPUT_KEY_CLASSES_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde INPUT_KEY_ACTIVE_SERDE = new Serdes.StringSerde();

    public static final SpecificAvroSerde<RegisteredAlarm> INPUT_VALUE_REGISTERED_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<RegisteredClass> INPUT_VALUE_CLASSES_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarm> INPUT_VALUE_ACTIVE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<MonologValue> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverrideList> OVERRIDE_LIST_VALUE_SERDE = new SpecificAvroSerde<>();

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

        INPUT_VALUE_REGISTERED_SERDE.configure(config, false);
        INPUT_VALUE_CLASSES_SERDE.configure(config, false);
        INPUT_VALUE_ACTIVE_SERDE.configure(config, false);

        OVERRIDE_KEY_SERDE.configure(config, true);
        OVERRIDE_VALUE_SERDE.configure(config, false);

        MONOLOG_VALUE_SERDE.configure(config, false);
        OVERRIDE_LIST_VALUE_SERDE.configure(config, false);

        final KTable<String, RegisteredAlarm> registeredTable = builder.table(INPUT_TOPIC_REGISTERED,
                Consumed.as("Registered-Table").with(INPUT_KEY_REGISTERED_SERDE, INPUT_VALUE_REGISTERED_SERDE));
        final KTable<String, RegisteredClass> classesTable = builder.table(INPUT_TOPIC_CLASSES,
                Consumed.as("Classes-Table").with(INPUT_KEY_CLASSES_SERDE, INPUT_VALUE_CLASSES_SERDE));
        final KTable<String, ActiveAlarm> activeTable = builder.table(INPUT_TOPIC_ACTIVE,
                Consumed.as("Active-Table").with(INPUT_KEY_ACTIVE_SERDE, INPUT_VALUE_ACTIVE_SERDE));

        KTable<String, MonologValue> classesAndRegistered = registeredTable.leftJoin(classesTable,
                RegisteredAlarm::getClass$, new RegisteredClassJoiner(), Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE))
                .filter(new Predicate<String, MonologValue>() {
                    @Override
                    public boolean test(String key, MonologValue value) {
                        System.err.println("\n\nREGISTERED-CLASS JOIN RESULT: key: " + key + "\n\tregistered: " + value.getRegistered() + ", \n\tactive: " + value.getActive());
                        return true;
                    }
                });

        KTable<String, MonologValue> registeredAndActive = classesAndRegistered.outerJoin(activeTable,
                new RegisteredAndActiveJoiner(), Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE))
                .filter(new Predicate<String, MonologValue>() {
                    @Override
                    public boolean test(String key, MonologValue value) {
                        System.err.println("CLASS-ACTIVE JOIN RESULT: key: " + key + "\n\tregistered: " + value.getRegistered() + ", \n\tactive: " + value.getActive());
                        return true;
                    }
                });

        KTable<String, OverrideList> overriddenItems = getOverriddenViaGroupBy(builder);

        /*KStream<String, MonologValue> plusOverrides = registeredAndActive.toStream()
                .outerJoin(overriddenItems.toStream(),
                        new OverrideJoiner(),
                        JoinWindows.of(Duration.of(1, ChronoUnit.SECONDS)),
                        StreamJoined.with(Serdes.String(), MONOLOG_VALUE_SERDE, OVERRIDE_LIST_VALUE_SERDE))*/


        KTable<String, MonologValue> plusOverrides = registeredAndActive.outerJoin(overriddenItems, new OverrideJoiner())
                .filter(new Predicate<String, MonologValue>() {
                    @Override
                    public boolean test(String key, MonologValue value) {
                        System.err.println("ACTIVE-OVERRIDE JOIN RESULT: key: " + key + "\n\tregistered: " + value.getRegistered() + ", \n\tactive: " + value.getActive());
                        return true;
                    }
                });

        final StoreBuilder<KeyValueStore<String, ActiveAlarm>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("PreviousActiveStateStore"),
                INPUT_KEY_ACTIVE_SERDE,
                INPUT_VALUE_ACTIVE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<String, MonologValue> withTransitionState = plusOverrides.toStream()
                .transform(new MonologRule.MsgTransformerFactory(storeBuilder.name()),
                        Named.as("ActiveTransitionStateProcessor"),
                        storeBuilder.name());

        final KStream<String, MonologValue> withHeaders = withTransitionState
                .transform(new MonologAddHeadersFactory());

        withHeaders.to(OUTPUT_TOPIC, Produced.as("Monolog")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
    }

    public static RegisteredAlarm computeEffectiveRegistration(RegisteredAlarm registered, RegisteredClass clazz) {
        RegisteredAlarm effectiveRegistered = RegisteredAlarm.newBuilder(registered).build();
        if(clazz != null) {
            if (effectiveRegistered.getCategory() == null) effectiveRegistered.setCategory(clazz.getCategory());
            if (effectiveRegistered.getCorrectiveaction() == null)
                effectiveRegistered.setCorrectiveaction(clazz.getCorrectiveaction());
            if (effectiveRegistered.getLatching() == null) effectiveRegistered.setLatching(clazz.getLatching());
            if (effectiveRegistered.getFilterable() == null)
                effectiveRegistered.setFilterable(clazz.getFilterable());
            if (effectiveRegistered.getLocation() == null) effectiveRegistered.setLocation(clazz.getLocation());
            if (effectiveRegistered.getMaskedby() == null) effectiveRegistered.setMaskedby(clazz.getMaskedby());
            if (effectiveRegistered.getOffdelayseconds() == null)
                effectiveRegistered.setOffdelayseconds(clazz.getOffdelayseconds());
            if (effectiveRegistered.getOndelayseconds() == null)
                effectiveRegistered.setOndelayseconds(clazz.getOndelayseconds());
            if (effectiveRegistered.getPointofcontactusername() == null)
                effectiveRegistered.setPointofcontactusername(clazz.getPointofcontactusername());
            if (effectiveRegistered.getPriority() == null) effectiveRegistered.setPriority(clazz.getPriority());
            if (effectiveRegistered.getRationale() == null) effectiveRegistered.setRationale(clazz.getRationale());
            if (effectiveRegistered.getScreenpath() == null)
                effectiveRegistered.setScreenpath(clazz.getScreenpath());
        }

        return effectiveRegistered;
    }

    private final class RegisteredClassJoiner implements ValueJoiner<RegisteredAlarm, RegisteredClass, MonologValue> {

        public MonologValue apply(RegisteredAlarm registered, RegisteredClass clazz) {

            //System.err.println("class joiner: " + registered);

            RegisteredAlarm effectiveRegistered = computeEffectiveRegistration(registered, clazz);

            return MonologValue.newBuilder()
                    .setRegistered(registered)
                    .setClass$(clazz)
                    .setEffectiveRegistered(effectiveRegistered)
                    .setActive(null)
                    .setOverrides(new OverrideSet())
                    .setTransitions(new TransitionSet())
                    .build();
        }
    }

    private final class RegisteredAndActiveJoiner implements ValueJoiner<MonologValue, ActiveAlarm, MonologValue> {

        public MonologValue apply(MonologValue registered, ActiveAlarm active) {

            //System.err.println("active joiner: " + active + ", registered: " + registered);

            MonologValue result;

            if(registered != null) {
                result = MonologValue.newBuilder(registered).setActive(active).build();
            } else {
                result = MonologValue.newBuilder()
                        .setRegistered(null)
                        .setClass$(null)
                        .setEffectiveRegistered(null)
                        .setOverrides(new OverrideSet())
                        .setTransitions(new TransitionSet())
                        .setActive(active).build();
            }

            return result;
        }
    }

    private final class OverrideJoiner implements ValueJoiner<MonologValue, OverrideList, MonologValue> {

        public MonologValue apply(MonologValue registeredAndActive, OverrideList overrideList) {

            //System.err.println("override joiner: " + registeredAndActive);

            OverrideSet overrides = OverrideSet.newBuilder()
                    .setDisabled(null)
                    .setFiltered(null)
                    .setLatched(null)
                    .setMasked(null)
                    .setOffdelay(null)
                    .setOndelay(null)
                    .setShelved(null)
                    .build();

            if(overrideList != null) {
                for(OverriddenAlarmValue over: overrideList.getOverrides()) {
                    if(over.getMsg() instanceof DisabledAlarm) {
                        overrides.setDisabled((DisabledAlarm) over.getMsg());
                    }

                    if(over.getMsg() instanceof FilteredAlarm) {
                        overrides.setFiltered((FilteredAlarm) over.getMsg());
                    }
                }
            }

            registeredAndActive.setOverrides(overrides);

            return MonologValue.newBuilder(registeredAndActive).build();
        }
    }

    private static KTable<String, OverrideList> getOverriddenViaGroupBy(StreamsBuilder builder) {
        final KTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable = builder.table(INPUT_TOPIC_OVERRIDDEN,
                Consumed.as("Overridden-Table").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));

        final KTable<String, OverrideList> groupTable = overriddenTable
                .groupBy((key, value) -> groupOverride(key, value), Grouped.as("Grouped-Overrides")
                        .with(Serdes.String(), OVERRIDE_LIST_VALUE_SERDE))
                .aggregate(
                        () -> new OverrideList(new ArrayList<>()),
                        (key, newValue, aggregate) -> {
                            //System.err.println("add: " + key + ", " + newValue + ", " + aggregate);
                            if(newValue.getOverrides() != null && aggregate != null && aggregate.getOverrides() != null) {
                                aggregate.getOverrides().addAll(newValue.getOverrides());
                            }

                            return aggregate;
                        },
                        (key, oldValue, aggregate) -> {
                            //System.err.println("subtract: " + key + ", " + oldValue + ", " + aggregate);

                            ArrayList<OverriddenAlarmValue> tmp = new ArrayList<>(aggregate.getOverrides());

                            for(OverriddenAlarmValue oav: oldValue.getOverrides()) {
                                tmp.remove(oav);
                            }
                            return new OverrideList(tmp);
                        },
                        Materialized.as("Override-Criteria-Table").with(Serdes.String(), OVERRIDE_LIST_VALUE_SERDE));

        return groupTable;
    }

    private static KeyValue<String, OverrideList> groupOverride(OverriddenAlarmKey key, OverriddenAlarmValue value) {
        List<OverriddenAlarmValue> list = new ArrayList<>();
        list.add(value);
        return new KeyValue<>(key.getName(), new OverrideList(list));
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
                private KeyValueStore<String, ActiveAlarm> store;
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.store = (KeyValueStore<String, ActiveAlarm>) context.getStateStore(storeName);
                }

                @Override
                public KeyValue<String, MonologValue> transform(String key, MonologValue value) {
                    ActiveAlarm previous = store.get(key);
                    ActiveAlarm next = null;

                    //System.err.println("previous: " + previous);
                    //System.err.println("next: " + (value == null ? null : value.getActive()));

                    boolean transitionToActive = false;
                    boolean transitionToNormal = false;

                    if(value != null) {
                        next = value.getActive();
                    }

                    if (previous == null && next != null) {
                        //System.err.println("TRANSITION TO ACTIVE!");
                        transitionToActive = true;
                    } else if(previous != null && next == null) {
                        //System.err.println("TRANSITION TO NORMAL!");
                        transitionToNormal = true;
                    }

                    store.put(key, next);

                    if(value != null) {
                        value.getTransitions().setTransitionToActive(transitionToActive);
                        value.getTransitions().setTransitionToNormal(transitionToNormal);
                    }

                    log.trace("Transformed: {}={}", key, value);

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
