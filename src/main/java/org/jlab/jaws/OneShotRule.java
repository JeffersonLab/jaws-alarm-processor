package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Removes Shelved override when alarm is no longer active for overrides configured as one-shot.
 */
public class OneShotRule extends AutoOverrideRule {

    public static final String OUTPUT_TOPIC = "overridden-alarms";
    public static final String INPUT_TOPIC_OVERRIDDEN = "overridden-alarms";
    public static final String INPUT_TOPIC_ACTIVE = "active-alarms";
    public static final SpecificAvroSerde<OverriddenAlarmKey> INPUT_KEY_OVERRIDDEN_SERDE = new SpecificAvroSerde<>();
    public static final Serdes.StringSerde INPUT_KEY_ACTIVE_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<OverriddenAlarmValue> INPUT_VALUE_OVERRIDDEN_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarm> INPUT_VALUE_ACTIVE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmKey> OUTPUT_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OUTPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final Serdes.StringSerde ONESHOT_JOIN_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<OneShotJoin> ONESHOT_JOIN_VALUE_SERDE = new SpecificAvroSerde<>();
    private static final Logger log = LoggerFactory.getLogger(OneShotRule.class);

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-auto-override-processor-oneshot");

        return props;
    }

    @Override
    public Topology constructTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        INPUT_KEY_OVERRIDDEN_SERDE.configure(config, true);
        INPUT_VALUE_OVERRIDDEN_SERDE.configure(config, false);

        INPUT_VALUE_ACTIVE_SERDE.configure(config, false);

        OUTPUT_KEY_SERDE.configure(config, true);
        OUTPUT_VALUE_SERDE.configure(config, false);

        ONESHOT_JOIN_VALUE_SERDE.configure(config, false);

        final KTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable = builder.table(INPUT_TOPIC_OVERRIDDEN,
                Consumed.as("Override-Table").with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));
        final KTable<String, ActiveAlarm> activeTable = builder.table(INPUT_TOPIC_ACTIVE,
                Consumed.as("Active-Table").with(INPUT_KEY_ACTIVE_SERDE, INPUT_VALUE_ACTIVE_SERDE));

        KGroupedTable<String, OverriddenAlarmValue> rekeyed = overriddenTable.filter(new Predicate<OverriddenAlarmKey, OverriddenAlarmValue>() {
            @Override
            public boolean test(OverriddenAlarmKey key, OverriddenAlarmValue value) {
                return key.getType() == OverriddenAlarmType.Shelved;
            }
        }).groupBy(
                new KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, OverriddenAlarmValue>>() {
                    @Override
                    public KeyValue<String, OverriddenAlarmValue> apply(OverriddenAlarmKey key, OverriddenAlarmValue value) {
                        log.info("Rekey: {}", key);
                        return new KeyValue<>(key.getName(), value);
                    }
                }, Grouped.with(ONESHOT_JOIN_KEY_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        KTable<String, OverriddenAlarmValue> grouped = rekeyed.aggregate(new Initializer<OverriddenAlarmValue>() {
            @Override
            public OverriddenAlarmValue apply() {
                return null;
            }
        }, new Aggregator<String, OverriddenAlarmValue, OverriddenAlarmValue>() {
            @Override
            public OverriddenAlarmValue apply(String key, OverriddenAlarmValue value, OverriddenAlarmValue aggregate) {
                log.info("adder: {}, {}, {}", key, value, aggregate);
                return value;
            }
        }, new Aggregator<String, OverriddenAlarmValue, OverriddenAlarmValue>() {
            @Override
            public OverriddenAlarmValue apply(String key, OverriddenAlarmValue value, OverriddenAlarmValue aggregate) {
                log.info("subtractor: {}, {}, {}", key, value, aggregate);
                return null;
            }
        }, Materialized.with(ONESHOT_JOIN_KEY_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        KTable<String, OneShotJoin> oneShotJoined = grouped.outerJoin(activeTable,
                new OneShotJoiner(), Materialized.with(ONESHOT_JOIN_KEY_SERDE, ONESHOT_JOIN_VALUE_SERDE));

        // Only allow messages indicating an alarm is both inactive (null / active tombstone) and oneshot to pass
        KStream<String, OneShotJoin> filtered = oneShotJoined.toStream().filter(new Predicate<String, OneShotJoin>() {
            @Override
            public boolean test(String key, OneShotJoin value) {
                log.info("filtering oneshot: {}={}", key, value);
                return value != null && !value.getActive() && value.getOneshot();
            }
        });

        // Now map into overridden-alarms topic format
        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> out = filtered.map(new KeyValueMapper<String, OneShotJoin, KeyValue<? extends OverriddenAlarmKey, ? extends OverriddenAlarmValue>>() {
            @Override
            public KeyValue<? extends OverriddenAlarmKey, ? extends OverriddenAlarmValue> apply(String key, OneShotJoin value) {
                return new KeyValue<>(new OverriddenAlarmKey(key, OverriddenAlarmType.Shelved), null);
            }
        }, Named.as("Map-OneShot"));

        out.to(OUTPUT_TOPIC, Produced.as("Overridden-Alarms-OneShot")
                .with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    public static class OneShotJoiner implements ValueJoiner<OverriddenAlarmValue, ActiveAlarm, OneShotJoin> {

        public OneShotJoin apply(OverriddenAlarmValue override, ActiveAlarm active) {
            boolean oneshot = false;

            if (override != null && override.getMsg() instanceof ShelvedAlarm) {
                ShelvedAlarm shelved = (ShelvedAlarm) override.getMsg();
                oneshot = shelved.getOneshot();
            }

            log.info("joining: {}, {}", override, active);

            return OneShotJoin.newBuilder()
                    .setActive(active != null)
                    .setOneshot(oneshot)
                    .build();
        }
    }
}
