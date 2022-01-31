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
 * Streams rule to join the activation and override topics into a single topic that is ordered (single partition)
 * such that processing can be done.
 *
 * A store of the previous active record for each alarm is used to determine
 * transitions from active to normal and back.
 */
public class ActivationRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(ActivationRule.class);

    String inputTopicRegisteredMonolog;
    String inputTopicActive;
    String inputTopicOverridden;

    public static final Serdes.StringSerde ACTIVE_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<AlarmActivationUnion> ACTIVE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<AlarmOverrideUnion> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverrideList> OVERRIDE_LIST_VALUE_SERDE = new SpecificAvroSerde<>();

    public ActivationRule(String inputTopicRegisteredMonolog, String inputTopicActive, String inputTopicOverridden, String outputTopic) {
        super(null, outputTopic);
        this.inputTopicRegisteredMonolog = inputTopicRegisteredMonolog;
        this.inputTopicActive = inputTopicActive;
        this.inputTopicOverridden = inputTopicOverridden;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-activation");

        return props;
    }

    @Override
    public Topology constructTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        ACTIVE_VALUE_SERDE.configure(config, false);

        OVERRIDE_KEY_SERDE.configure(config, true);
        OVERRIDE_VALUE_SERDE.configure(config, false);

        MONOLOG_VALUE_SERDE.configure(config, false);
        OVERRIDE_LIST_VALUE_SERDE.configure(config, false);

        final KTable<String, IntermediateMonolog> registeredMonologTable = builder.table(inputTopicRegisteredMonolog,
                Consumed.as("Registered-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));
        final KTable<String, AlarmActivationUnion> activeTable = builder.table(inputTopicActive,
                Consumed.as("Active-Table").with(ACTIVE_KEY_SERDE, ACTIVE_VALUE_SERDE));


        KTable<String, IntermediateMonolog> registeredAndActive = registeredMonologTable.outerJoin(activeTable,
                new RegisteredAndActiveJoiner(), Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE))
                .filter(new Predicate<String, IntermediateMonolog>() {
                    @Override
                    public boolean test(String key, IntermediateMonolog value) {
                        log.debug("CLASS-ACTIVE JOIN RESULT: key: " + key + "\n\tregistered: " + value.getRegistration() + ", \n\tactive: " + value.getActivation());
                        return true;
                    }
                });

        KTable<String, OverrideList> overriddenItems = getOverriddenViaGroupBy(builder);

        KTable<String, IntermediateMonolog> plusOverrides = registeredAndActive.outerJoin(overriddenItems, new OverrideJoiner())
                .filter(new Predicate<String, IntermediateMonolog>() {
                    @Override
                    public boolean test(String key, IntermediateMonolog value) {
                        log.debug("ACTIVE-OVERRIDE JOIN RESULT: key: " + key + "\n\tregistered: " + value.getRegistration() + ", \n\tactive: " + value.getActivation());
                        return true;
                    }
                });

        final StoreBuilder<KeyValueStore<String, AlarmActivationUnion>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("PreviousActiveStateStore"),
                ACTIVE_KEY_SERDE,
                ACTIVE_VALUE_SERDE
        ).withCachingEnabled();

        builder.addStateStore(storeBuilder);

        // Ensure we always return non-null Alarm record and populate it with transition state
        final KStream<String, IntermediateMonolog> withTransitionState = plusOverrides.toStream()
                .transform(new ActivationRule.MsgTransformerFactory(storeBuilder.name()),
                        Named.as("ActiveTransitionStateProcessor"),
                        storeBuilder.name());

        final KStream<String, IntermediateMonolog> withHeaders = withTransitionState
                .transform(new MonologAddHeadersFactory());

        withHeaders.to(outputTopic, Produced.as("Monolog")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
    }

    private final class RegisteredAndActiveJoiner implements ValueJoiner<IntermediateMonolog, AlarmActivationUnion, IntermediateMonolog> {

        public IntermediateMonolog apply(IntermediateMonolog registered, AlarmActivationUnion active) {

            //System.err.println("active joiner: " + active + ", registered: " + registered);

            EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder()
                    .setClass$(null)
                    .setInstance(null)
                    .build();

            EffectiveActivation effectiveAct = EffectiveActivation.newBuilder()
                    .setActual(active)
                    .setOverrides(new AlarmOverrideSet())
                    .setState(AlarmState.Normal)
                    .build();

            IntermediateMonolog result = IntermediateMonolog.newBuilder()
                    .setRegistration(effectiveReg)
                    .setActivation(effectiveAct)
                    .setTransitions(new ProcessorTransitions())
                    .build();

            if(registered != null) {
                result.getRegistration().setInstance(registered.getRegistration().getInstance());
                result.getRegistration().setClass$(registered.getRegistration().getClass$());
            }

            return result;
        }
    }

    private final class OverrideJoiner implements ValueJoiner<IntermediateMonolog, OverrideList, IntermediateMonolog> {

        public IntermediateMonolog apply(IntermediateMonolog registeredAndActive, OverrideList overrideList) {

            //System.err.println("override joiner: " + registeredAndActive);

            AlarmOverrideSet overrides = AlarmOverrideSet.newBuilder()
                    .setDisabled(null)
                    .setFiltered(null)
                    .setLatched(null)
                    .setMasked(null)
                    .setOffdelayed(null)
                    .setOndelayed(null)
                    .setShelved(null)
                    .build();

            if(overrideList != null) {
                for(AlarmOverrideUnion over: overrideList.getOverrides()) {
                    if(over.getMsg() instanceof DisabledOverride) {
                        overrides.setDisabled((DisabledOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof FilteredOverride) {
                        overrides.setFiltered((FilteredOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof LatchedOverride) {
                        overrides.setLatched((LatchedOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof MaskedOverride) {
                        overrides.setMasked((MaskedOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof OnDelayedOverride) {
                        overrides.setOndelayed((OnDelayedOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof OffDelayedOverride) {
                        overrides.setOffdelayed((OffDelayedOverride) over.getMsg());
                    }

                    if(over.getMsg() instanceof ShelvedOverride) {
                        overrides.setShelved((ShelvedOverride) over.getMsg());
                    }
                }
            }

            IntermediateMonolog result;

            if(registeredAndActive != null) {
                result = IntermediateMonolog.newBuilder(registeredAndActive)
                        .build();

                        result.getActivation().setOverrides(overrides);
            } else {
                EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder()
                        .build();

                EffectiveActivation effectiveAct = EffectiveActivation.newBuilder()
                        .setOverrides(overrides)
                        .setState(AlarmState.Normal)
                        .build();

                result = IntermediateMonolog.newBuilder()
                        .setRegistration(effectiveReg)
                        .setActivation(effectiveAct)
                        .setTransitions(new ProcessorTransitions())
                        .build();
            }

            return result;
        }
    }

    private KTable<String, OverrideList> getOverriddenViaGroupBy(StreamsBuilder builder) {
        final KTable<OverriddenAlarmKey, AlarmOverrideUnion> overriddenTable = builder.table(inputTopicOverridden,
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

                            ArrayList<AlarmOverrideUnion> tmp = new ArrayList<>(aggregate.getOverrides());

                            for(AlarmOverrideUnion oav: oldValue.getOverrides()) {
                                tmp.remove(oav);
                            }
                            return new OverrideList(tmp);
                        },
                        Materialized.as("Override-Criteria-Table").with(Serdes.String(), OVERRIDE_LIST_VALUE_SERDE));

        return groupTable;
    }

    private static KeyValue<String, OverrideList> groupOverride(OverriddenAlarmKey key, AlarmOverrideUnion value) {
        List<AlarmOverrideUnion> list = new ArrayList<>();
        list.add(value);
        return new KeyValue<>(key.getName(), new OverrideList(list));
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
                private KeyValueStore<String, AlarmActivationUnion> store;
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.store = (KeyValueStore<String, AlarmActivationUnion>) context.getStateStore(storeName);
                }

                @Override
                public KeyValue<String, IntermediateMonolog> transform(String key, IntermediateMonolog value) {
                    AlarmActivationUnion previous = store.get(key);
                    AlarmActivationUnion next = null;

                    //System.err.println("previous: " + previous);
                    //System.err.println("next: " + (value == null ? null : value.getActive()));

                    boolean transitionToActive = false;
                    boolean transitionToNormal = false;

                    // Handle Scenario where only one of Registration or Activation and it just got tombstoned!
                    // Instead of forwarding IntermediateMonolog = null we always forward non-null IntermediateMonolog,
                    // but fields inside may be null
                    if(value == null) {
                        EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder().build();

                        EffectiveActivation effectiveAct = EffectiveActivation.newBuilder()
                                .setOverrides(new AlarmOverrideSet())
                                .setState(AlarmState.Normal)
                                .build();

                        value = IntermediateMonolog.newBuilder()
                                .setRegistration(effectiveReg)
                                .setActivation(effectiveAct)
                                .setTransitions(new ProcessorTransitions())
                                .build();
                    }

                    next = value.getActivation().getActual();

                    if (previous == null && next != null) {
                        //System.err.println("TRANSITION TO ACTIVE!");
                        transitionToActive = true;
                    } else if(previous != null && next == null) {
                        //System.err.println("TRANSITION TO NORMAL!");
                        transitionToNormal = true;
                    }

                    store.put(key, next);

                    value.getTransitions().setTransitionToActive(transitionToActive);
                    value.getTransitions().setTransitionToNormal(transitionToNormal);

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
