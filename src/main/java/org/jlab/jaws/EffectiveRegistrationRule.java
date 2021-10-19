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
 * Streams rule to join alarm classes with alarm registrations such that null fields in an alarm
 * registration are filled in with class defaults.
 */
public class EffectiveRegistrationRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(EffectiveRegistrationRule.class);

    String inputTopicClasses;
    String inputTopicRegistered;
    String outputTopicEffective;
    String outputTopicMonolog;

    public static final Serdes.StringSerde INPUT_KEY_REGISTERED_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde INPUT_KEY_CLASSES_SERDE = new Serdes.StringSerde();

    public static final SpecificAvroSerde<AlarmRegistration> INPUT_VALUE_REGISTERED_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<AlarmClass> INPUT_VALUE_CLASSES_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde EFFECTIVE_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<AlarmRegistration> EFFECTIVE_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<Alarm> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public EffectiveRegistrationRule(String inputTopicClasses, String inputTopicRegistered, String outputTopicEffective, String outputTopicMonolog) {
        super(null, null);
        this.inputTopicClasses = inputTopicClasses;
        this.inputTopicRegistered = inputTopicRegistered;
        this.outputTopicEffective = outputTopicEffective;
        this.outputTopicMonolog = outputTopicMonolog;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-alarm-processor-monolog");

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

        EFFECTIVE_VALUE_SERDE.configure(config, false);
        MONOLOG_VALUE_SERDE.configure(config, false);

        final KTable<String, AlarmClass> classesTable = builder.table(inputTopicClasses,
                Consumed.as("Classes-Table").with(INPUT_KEY_CLASSES_SERDE, INPUT_VALUE_CLASSES_SERDE));
        final KTable<String, AlarmRegistration> registeredTable = builder.table(inputTopicRegistered,
                Consumed.as("Registered-Table").with(INPUT_KEY_REGISTERED_SERDE, INPUT_VALUE_REGISTERED_SERDE));

        KTable<String, Alarm> classesAndRegistered = registeredTable.leftJoin(classesTable,
                AlarmRegistration::getClass$, new AlarmClassJoiner(), Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE))
                .filter(new Predicate<String, Alarm>() {
                    @Override
                    public boolean test(String key, Alarm value) {
                        log.debug("\n\nREGISTERED-CLASS JOIN RESULT: key: " + key + "value: " + value);
                        return true;
                    }
                });

        final KStream<String, Alarm> withHeaders = classesAndRegistered.toStream()
                .transform(new MonologAddHeadersFactory());

        KStream<String, AlarmRegistration> effective = withHeaders.mapValues(new ValueMapper<Alarm, AlarmRegistration>() {
            @Override
            public AlarmRegistration apply(Alarm value) {
                return value.getEffectiveRegistration();
            }
        });

        effective.to(outputTopicEffective, Produced.as("EffectiveRegistration")
                .with(EFFECTIVE_KEY_SERDE, EFFECTIVE_VALUE_SERDE));

        withHeaders.to(outputTopicMonolog, Produced.as("MonologRegistration")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
    }

    public static AlarmRegistration computeEffectiveRegistration(AlarmRegistration registered, AlarmClass clazz) {
        AlarmRegistration effectiveRegistered = AlarmRegistration.newBuilder(registered).build();
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

    private final class AlarmClassJoiner implements ValueJoiner<AlarmRegistration, AlarmClass, Alarm> {

        public Alarm apply(AlarmRegistration registered, AlarmClass clazz) {

            //System.err.println("class joiner: " + registered);

            AlarmRegistration effectiveRegistered = computeEffectiveRegistration(registered, clazz);

            Alarm alarm = Alarm.newBuilder()
                    .setRegistration(registered)
                    .setClass$(clazz)
                    .setEffectiveRegistration(effectiveRegistered)
                    .setOverrides(new AlarmOverrideSet())
                    .setTransitions(new ProcessorTransitions())
                    .setState(AlarmState.Normal)
                    .build();

            return alarm;
        }
    }

    public final class AlarmRegistrationAddHeadersFactory implements TransformerSupplier<String, AlarmRegistration, KeyValue<String, AlarmRegistration>> {

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<String, AlarmRegistration, KeyValue<String, AlarmRegistration>> get() {
            return new Transformer<String, AlarmRegistration, KeyValue<String, AlarmRegistration>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, AlarmRegistration> transform(String key, AlarmRegistration value) {
                    log.debug("Handling message: {}={}", key, value);

                    setHeaders(context);

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
