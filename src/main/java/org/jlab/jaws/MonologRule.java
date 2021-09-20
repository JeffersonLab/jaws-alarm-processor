package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Adds a Latched override for alarms registered as latching that become active.
 */
public class MonologRule extends AutoOverrideRule {

    private static final Logger log = LoggerFactory.getLogger(MonologRule.class);

    public static final String OUTPUT_TOPIC = "monolog";

    public static final String INPUT_TOPIC_REGISTERED = "registered-alarms";
    public static final String INPUT_TOPIC_ACTIVE = "active-alarms";
    public static final String INPUT_TOPIC_OVERRIDDEN = "overridden-alarms";

    public static final Serdes.StringSerde INPUT_KEY_REGISTERED_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde INPUT_KEY_ACTIVE_SERDE = new Serdes.StringSerde();

    public static final SpecificAvroSerde<RegisteredAlarm> INPUT_VALUE_REGISTERED_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarm> INPUT_VALUE_ACTIVE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<MonologValue> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde OVERRIDE_KV_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<OverrideKeyValue> OVERRIDE_KV_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde OVERRIDE_LIST_KEY_SERDE = new Serdes.StringSerde();
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
        INPUT_VALUE_ACTIVE_SERDE.configure(config, false);

        OVERRIDE_KEY_SERDE.configure(config, true);
        OVERRIDE_VALUE_SERDE.configure(config, false);

        MONOLOG_VALUE_SERDE.configure(config, false);
        OVERRIDE_KV_VALUE_SERDE.configure(config, false);
        OVERRIDE_LIST_VALUE_SERDE.configure(config, false);

        final KTable<String, RegisteredAlarm> registeredTable = builder.table(INPUT_TOPIC_REGISTERED,
                Consumed.as("Registered-Table").with(INPUT_KEY_REGISTERED_SERDE, INPUT_VALUE_REGISTERED_SERDE));
        final KTable<String, ActiveAlarm> activeTable = builder.table(INPUT_TOPIC_ACTIVE,
                Consumed.as("Active-Table").with(INPUT_KEY_ACTIVE_SERDE, INPUT_VALUE_ACTIVE_SERDE));


        KTable<String, MonologValue> registeredAndActive = registeredTable.join(activeTable,
                new RegisteredAndActiveJoiner(), Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE));

        KTable<String, OverrideList> overriddenItems = getOverriddenViaGroupBy(builder);

        KTable<String, MonologValue> plusOverrides = registeredAndActive
                .outerJoin(overriddenItems, new OverrideJoiner(),
                        Named.as("Plus-Overrides"));

        final KStream<String, MonologValue> transformed = plusOverrides.toStream()
                .transform(new MonologAddHeadersFactory());

        transformed.to(OUTPUT_TOPIC, Produced.as("Monolog")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
    }

    private final class RegisteredAndActiveJoiner implements ValueJoiner<RegisteredAlarm, ActiveAlarm, MonologValue> {

        public MonologValue apply(RegisteredAlarm registered, ActiveAlarm active) {
            return MonologValue.newBuilder()
                    .setRegistered(registered)
                    .setActive(active)
                    .setOverrides(new ArrayList<>())
                    .build();
        }
    }

    private final class OverrideJoiner implements ValueJoiner<MonologValue, OverrideList, MonologValue> {

        public MonologValue apply(MonologValue registeredAndActive, OverrideList overrideList) {

            System.err.println("join record before: " + registeredAndActive);

            if(overrideList == null) {
                registeredAndActive.setOverrides(new ArrayList<>());
            } else {
                registeredAndActive.setOverrides(overrideList.getOverrides());
            }

            System.err.println("join record after: " + registeredAndActive);

            return registeredAndActive;
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
                            System.err.println("add: " + key + ", " + newValue + ", " + aggregate);
                            if(newValue.getOverrides() != null && aggregate != null && aggregate.getOverrides() != null) {
                                aggregate.getOverrides().addAll(newValue.getOverrides());
                            }

                            return aggregate;
                        },
                        (key, oldValue, aggregate) -> {
                            System.err.println("subtract: " + key + ", " + oldValue + ", " + aggregate);

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
}
