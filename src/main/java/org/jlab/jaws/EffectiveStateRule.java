package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Compute effective state given active and overridden state. */
public class EffectiveStateRule extends ProcessingRule {

  String effectiveAlarmTopic;
  String EffectiveNotificationTopic;

  private static final Logger log = LoggerFactory.getLogger(EffectiveStateRule.class);

  public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
  public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public static final Serdes.StringSerde EFFECTIVE_ALARM_KEY_SERDE = new Serdes.StringSerde();
  public static final SpecificAvroSerde<EffectiveAlarm> EFFECTIVE_ALARM_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public static final Serdes.StringSerde EFFECTIVE_NOTIFICATION_KEY_SERDE =
      new Serdes.StringSerde();
  public static final SpecificAvroSerde<EffectiveNotification> EFFECTIVE_NOTIFICATION_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public EffectiveStateRule(
      String inputTopic, String EffectiveNotificationTopic, String effectiveAlarmTopic) {
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

    // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's
    // likely because you didn't set registry config
    Map<String, String> config = new HashMap<>();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

    MONOLOG_VALUE_SERDE.configure(config, false);
    EFFECTIVE_ALARM_VALUE_SERDE.configure(config, false);
    EFFECTIVE_NOTIFICATION_VALUE_SERDE.configure(config, false);

    final KTable<String, IntermediateMonolog> monologTable =
        builder.table(
            inputTopic, Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

    final KStream<String, IntermediateMonolog> monologStream = monologTable.toStream();

    final KStream<String, IntermediateMonolog> calculated =
        monologStream.process(
            new MyProcessorSupplier(), Named.as("EffectiveStateTransitionProcessor"));

    final KStream<String, EffectiveAlarm> effectiveAlarms =
        calculated.mapValues(
            new ValueMapper<IntermediateMonolog, EffectiveAlarm>() {
              @Override
              public EffectiveAlarm apply(IntermediateMonolog value) {
                return EffectiveAlarm.newBuilder()
                    .setRegistration(value.getRegistration())
                    .setNotification(value.getNotification())
                    .build();
              }
            });

    effectiveAlarms.to(
        effectiveAlarmTopic,
        Produced.as("EFFECTIVE-ALARMS-OUTPUT")
            .with(EFFECTIVE_ALARM_KEY_SERDE, EFFECTIVE_ALARM_VALUE_SERDE));

    final KStream<String, EffectiveNotification> EffectiveNotifications =
        calculated.mapValues(
            new ValueMapper<IntermediateMonolog, EffectiveNotification>() {
              @Override
              public EffectiveNotification apply(IntermediateMonolog value) {
                return EffectiveNotification.newBuilder(value.getNotification()).build();
              }
            });

    EffectiveNotifications.to(
        EffectiveNotificationTopic,
        Produced.as("EFFECTIVE-NOTIFICATIONS-OUTPUT")
            .with(EFFECTIVE_NOTIFICATION_KEY_SERDE, EFFECTIVE_NOTIFICATION_VALUE_SERDE));

    return builder.build();
  }

  private static final class MyProcessorSupplier
      implements ProcessorSupplier<String, IntermediateMonolog, String, IntermediateMonolog> {

    /** Create a new ProcessorSupplier. */
    public MyProcessorSupplier() {}

    /**
     * Return a new {@link Transformer} instance.
     *
     * @return a new {@link Transformer} instance
     */
    @Override
    public Processor<String, IntermediateMonolog, String, IntermediateMonolog> get() {
      return new Processor<>() {
        private ProcessorContext<String, IntermediateMonolog> context;

        @Override
        public void init(ProcessorContext<String, IntermediateMonolog> context) {
          this.context = context;
        }

        @Override
        public void process(Record<String, IntermediateMonolog> input) {
          log.debug(
              "Processing key = {}, value = \n\tInst: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}",
              input.key(),
              input.value().getRegistration().getAlarm(),
              input.value().getNotification(),
              input.value().getNotification().getOverrides(),
              input.value().getTransitions());

          Record<String, IntermediateMonolog> output = null;

          // If transitioning, we drop message as it's an intermediate message.
          // This could introduce substantial latency and high
          // frequency changes effectively result in denial-of-service.  However, transitioning
          // means
          // we've produced one or more messages on the alarm-overrides topic in response to the
          // current
          // message and an intermediate message quickly followed by computed message means active
          // segment of
          // notifications and alarms topics has double the number of messages (extra intermediate
          // message
          // generally can't be compacted as it's generally in the active segment with computed
          // message due to
          // proximity)
          if (input.value().getTransitions().getLatching()
              || input.value().getTransitions().getOffdelaying()
              || input.value().getTransitions().getOndelaying()
              || input.value().getTransitions().getUnshelving()
              || input.value().getTransitions().getMasking()
              || input.value().getTransitions().getUnmasking()) {

            // Forward nothing (null output)
          } else {

            long timestamp = System.currentTimeMillis();

            output = new Record<>(input.key(), input.value(), timestamp);

            // Note: overrides are evaluated in increasing precedence order (last item, disabled,
            // has the highest precedence)

            AlarmState state = AlarmState.Normal;

            if (output.value().getNotification().getActivation() != null
                && !(output.value().getNotification().getActivation().getUnion()
                    instanceof NoActivation)) {
              state = AlarmState.Active;
            }

            if (output.value().getNotification().getOverrides().getOffdelayed() != null) {
              state = AlarmState.ActiveOffDelayed;
            }

            if (output.value().getTransitions().getLatching()
                || output.value().getNotification().getOverrides().getLatched() != null) {
              state = AlarmState.ActiveLatched;
            }

            if (output.value().getNotification().getOverrides().getOndelayed() != null) {
              state = AlarmState.NormalOnDelayed;
            }

            if (output.value().getNotification().getOverrides().getShelved() != null
                && !output.value().getTransitions().getUnshelving()) {

              if (output.value().getNotification().getOverrides().getShelved().getOneshot()) {
                state = AlarmState.NormalOneShotShelved;
              } else {
                state = AlarmState.NormalContinuousShelved;
              }
            }

            if (output.value().getNotification().getOverrides().getMasked() != null) {
              state = AlarmState.NormalMasked;
            }

            if (output.value().getNotification().getOverrides().getFiltered() != null) {
              state = AlarmState.NormalFiltered;
            }

            if (output.value().getNotification().getOverrides().getDisabled() != null) {
              state = AlarmState.NormalDisabled;
            }

            output.value().getNotification().setState(state);

            populateHeaders(output);

            context.forward(output);
          }
        }

        @Override
        public void close() {
          // Nothing to do
        }
      };
    }
  }
}
