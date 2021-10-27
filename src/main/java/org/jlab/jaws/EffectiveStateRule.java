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
    String effectiveActivationTopic;

    private static final Logger log = LoggerFactory.getLogger(EffectiveStateRule.class);

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde EFFECTIVE_ALARM_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<EffectiveAlarm> EFFECTIVE_ALARM_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde EFFECTIVE_ACTIVATION_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<EffectiveActivation> EFFECTIVE_ACTIVATION_VALUE_SERDE = new SpecificAvroSerde<>();

    public EffectiveStateRule(String inputTopic, String effectiveActivationTopic, String effectiveAlarmTopic) {
        super(inputTopic, null);

        this.effectiveActivationTopic = effectiveActivationTopic;
        this.effectiveAlarmTopic = effectiveAlarmTopic;
    }

    @Override
    public Properties constructProperties() {
        final Properties props = super.constructProperties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-alarm-processor-state");

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
        EFFECTIVE_ACTIVATION_VALUE_SERDE.configure(config, false);

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
                        .setActivation(value.getActivation())
                        .build();
            }
        });

        effectiveAlarms.to(effectiveAlarmTopic, Produced.as("EFFECTIVE-ALARMS-OUTPUT")
                .with(EFFECTIVE_ALARM_KEY_SERDE, EFFECTIVE_ALARM_VALUE_SERDE));

        final KStream<String, EffectiveActivation> effectiveActivations = calculated.mapValues(new ValueMapper<IntermediateMonolog, EffectiveActivation>() {
            @Override
            public EffectiveActivation apply(IntermediateMonolog value) {
                return EffectiveActivation.newBuilder(value.getActivation())
                        .build();
            }
        });

        effectiveActivations.to(effectiveActivationTopic, Produced.as("EFFECTIVE-ACTIVATIONS-OUTPUT")
                .with(EFFECTIVE_ACTIVATION_KEY_SERDE, EFFECTIVE_ACTIVATION_VALUE_SERDE));

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
                    log.debug("Processing key = {}, value = \n\tReg: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}", key, value.getRegistration().getCalculated(),value.getActivation(),value.getActivation().getOverrides(),value.getTransitions());

                    // Note: overrides are evaluated in increasing precedence order (last item, disabled, has the highest precedence)

                    AlarmState state = AlarmState.Normal;

                    if(value.getActivation().getActual() != null) {
                        state = AlarmState.Active;
                    }

                    if(value.getActivation().getOverrides().getOffdelayed() != null) {
                        state = AlarmState.ActiveOffDelayed;
                    }

                    if(value.getTransitions().getLatching() ||
                            value.getActivation().getOverrides().getLatched() != null) {
                        state = AlarmState.ActiveLatched;
                    }

                    if(value.getActivation().getOverrides().getOndelayed() != null) {
                        state = AlarmState.NormalOnDelayed;
                    }

                    if(value.getActivation().getOverrides().getShelved() != null &&
                            !value.getTransitions().getUnshelving()) {

                        if(value.getActivation().getOverrides().getShelved().getOneshot()) {
                            state = AlarmState.NormalOneShotShelved;
                        } else {
                            state = AlarmState.NormalContinuousShelved;
                        }
                    }

                    if(value.getActivation().getOverrides().getMasked() != null) {
                        state = AlarmState.NormalMasked;
                    }

                    if(value.getActivation().getOverrides().getFiltered() != null) {
                        state = AlarmState.NormalFiltered;
                    }

                    if(value.getActivation().getOverrides().getDisabled() != null) {
                        state = AlarmState.NormalDisabled;
                    }

                    value.getActivation().setState(state);

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
