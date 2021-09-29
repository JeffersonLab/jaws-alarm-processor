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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Compute effective state given active and overridden state.
 */
public class EffectiveStateRule extends ProcessingRule {

    private static final Logger log = LoggerFactory.getLogger(EffectiveStateRule.class);

    public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<Alarm> MONOLOG_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<OverriddenAlarmKey> OVERRIDE_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> OVERRIDE_VALUE_SERDE = new SpecificAvroSerde<>();

    public EffectiveStateRule(String inputTopic, String outputTopic) {
        super(inputTopic, outputTopic);
    }

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

        MONOLOG_VALUE_SERDE.configure(config, false);

        final KTable<String, Alarm> monologTable = builder.table(inputTopic,
                Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        final KStream<String, Alarm> monologStream = monologTable.toStream();


        final KStream<String, Alarm> output = monologStream.transform(
                new EffectiveStateRule.MsgTransformerFactory(),
                Named.as("EffectiveStateTransitionProcessor"));

        output.to(outputTopic, Produced.as("EFFECTIVE-STATE-OUTPUT")
                .with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

        return builder.build();
    }

    private static final class MsgTransformerFactory implements TransformerSupplier<String, Alarm, KeyValue<String, Alarm>> {

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
        public Transformer<String, Alarm, KeyValue<String, Alarm>> get() {
            return new Transformer<String, Alarm, KeyValue<String, Alarm>>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked") // https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<String, Alarm> transform(String key, Alarm value) {
                    System.err.println("Processing key = " + key + ", value = " + value);

                    // Note: criteria are evaluated in increasing precedence order (last item, disabled, has the highest precedence)

                    // Should we have an Unregistered state or always default to Normal?
                    AlarmState state = AlarmState.Normal;

                    if(value.getActive() != null) {
                        state = AlarmState.Active;
                    }

                    if(value.getOverrides().getOffdelay() != null) {
                        state = AlarmState.OffDelayed;
                    }

                    if(value.getTransitions().getLatching() ||
                            value.getOverrides().getLatched() != null) {
                        if(value.getActive() != null) {
                            state = AlarmState.Latched;
                        } else {
                            state = AlarmState.NormalLatched;
                        }
                    }

                    if(value.getOverrides().getOndelay() != null) {
                        state = AlarmState.OnDelayed;
                    }

                    if(value.getOverrides().getShelved() != null &&
                            !value.getTransitions().getUnshelving()) {

                        if(value.getOverrides().getShelved().getOneshot()) {
                            state = AlarmState.OneShotShelved;
                        } else {
                            if(value.getActive() != null) {
                                state = AlarmState.ContinuousShelved;
                            } else {
                                state = AlarmState.NormalContinuousShelved;
                            }
                        }
                    }

                    if(value.getOverrides().getMasked() != null) {
                        state = AlarmState.Masked;
                    }

                    if(value.getOverrides().getFiltered() != null) {
                        if(value.getActive() != null) {
                            state = AlarmState.Filtered;
                        } else {
                            state = AlarmState.NormalFiltered;
                        }
                    }

                    if(value.getOverrides().getDisabled() != null) {
                        if(value.getActive() != null) {
                            state = AlarmState.Disabled;
                        } else {
                            state = AlarmState.NormalDisabled;
                        }
                    }

                    value.setEffectiveState(state);

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
