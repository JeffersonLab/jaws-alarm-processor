package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Compute effective state given active and overridden state.
 */
public class EffectiveStateRule extends ProcessingRule {

    String effectiveAlarmTopic;
    String EffectiveNotificationTopic;

    private static final Logger log = LoggerFactory.getLogger(EffectiveStateRule.class);

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde EFFECTIVE_ALARM_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<EffectiveAlarm> EFFECTIVE_ALARM_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde EFFECTIVE_NOTIFICATION_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<EffectiveNotification> EFFECTIVE_NOTIFICATION_VALUE_SERDE = new SpecificAvroSerde<>();

    public EffectiveStateRule(String inputTopic, String EffectiveNotificationTopic, String effectiveAlarmTopic) {
        super(inputTopic, null);

        this.EffectiveNotificationTopic = EffectiveNotificationTopic;
        this.effectiveAlarmTopic = effectiveAlarmTopic;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-state");

        return props;
    }

    @Override
    public Topology constructTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        MONOLOG_VALUE_SERDE.configure(config, false);
        EFFECTIVE_ALARM_VALUE_SERDE.configure(config, false);
        EFFECTIVE_NOTIFICATION_VALUE_SERDE.configure(config, false);

        final KTable<String, IntermediateMonolog> monologTable = builder.table(inputTopic,
                Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        final KStream<String, IntermediateMonolog> monologStream = monologTable.toStream();


        final KStream<String, IntermediateMonolog> calculated = monologStream.transform(
                new EffectiveStateRule.MsgTransformerFactory(),
                Named.as("EffectiveStateTransitionProcessor"));

        final KStream<String, EffectiveAlarm> effectiveAlarms = calculated.mapValues(new ValueMapper<IntermediateMonolog, EffectiveAlarm>() {
            @Override
            public EffectiveAlarm apply(IntermediateMonolog value) {
                return EffectiveAlarm.newBuilder()
                        .setRegistration(value.getRegistration())
                        .setNotification(value.getNotification())
                        .build();
            }
        });

        effectiveAlarms.to(effectiveAlarmTopic, Produced.as("EFFECTIVE-ALARMS-OUTPUT")
                .with(EFFECTIVE_ALARM_KEY_SERDE, EFFECTIVE_ALARM_VALUE_SERDE));

        final KStream<String, EffectiveNotification> EffectiveNotifications = calculated.mapValues(new ValueMapper<IntermediateMonolog, EffectiveNotification>() {
            @Override
            public EffectiveNotification apply(IntermediateMonolog value) {
                return EffectiveNotification.newBuilder(value.getNotification())
                        .build();
            }
        });

        EffectiveNotifications.to(EffectiveNotificationTopic, Produced.as("EFFECTIVE-ACTIVATIONS-OUTPUT")
                .with(EFFECTIVE_NOTIFICATION_KEY_SERDE, EFFECTIVE_NOTIFICATION_VALUE_SERDE));

        return builder.build();
    }

    private static final class MsgTransformerFactory implements TransformerSupplier<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> {

        /**
         * Create a new MsgTransformerFactory.
         */
        public MsgTransformerFactory() {

        }

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>> get() {
            return new Transformer<String, IntermediateMonolog, KeyValue<String, IntermediateMonolog>>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, IntermediateMonolog> transform(String key, IntermediateMonolog value) {
                    log.debug("Processing key = {}, value = \n\tInst: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}", key, value.getRegistration().getInstance(),value.getNotification(),value.getNotification().getOverrides(),value.getTransitions());

                    // Note: overrides are evaluated in increasing precedence order (last item, disabled, has the highest precedence)

                    AlarmState state = AlarmState.Normal;

                    if(value.getNotification().getActivation() != null) {
                        state = AlarmState.Active;
                    }

                    if(value.getNotification().getOverrides().getOffdelayed() != null) {
                        state = AlarmState.ActiveOffDelayed;
                    }

                    if(value.getTransitions().getLatching() ||
                            value.getNotification().getOverrides().getLatched() != null) {
                        state = AlarmState.ActiveLatched;
                    }

                    if(value.getNotification().getOverrides().getOndelayed() != null) {
                        state = AlarmState.NormalOnDelayed;
                    }

                    if(value.getNotification().getOverrides().getShelved() != null &&
                            !value.getTransitions().getUnshelving()) {

                        if(value.getNotification().getOverrides().getShelved().getOneshot()) {
                            state = AlarmState.NormalOneShotShelved;
                        } else {
                            state = AlarmState.NormalContinuousShelved;
                        }
                    }

                    if(value.getNotification().getOverrides().getMasked() != null) {
                        state = AlarmState.NormalMasked;
                    }

                    if(value.getNotification().getOverrides().getFiltered() != null) {
                        state = AlarmState.NormalFiltered;
                    }

                    if(value.getNotification().getOverrides().getDisabled() != null) {
                        state = AlarmState.NormalDisabled;
                    }

                    value.getNotification().setState(state);

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
