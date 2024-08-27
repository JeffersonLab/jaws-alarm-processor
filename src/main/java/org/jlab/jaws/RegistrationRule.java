package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams rule to join alarm classes with alarm instances such that null fields in an alarm
 * instance are filled in with class defaults.
 */
public class RegistrationRule extends ProcessingRule {

  private static final Logger log = LoggerFactory.getLogger(RegistrationRule.class);

  String inputTopicClasses;
  String inputTopicInstances;
  String outputTopicEffective;
  String outputTopicMonolog;

  public static final Serdes.StringSerde INPUT_KEY_INSTANCES_SERDE = new Serdes.StringSerde();
  public static final Serdes.StringSerde INPUT_KEY_CLASSES_SERDE = new Serdes.StringSerde();

  public static final SpecificAvroSerde<Alarm> INPUT_VALUE_INSTANCES_SERDE =
      new SpecificAvroSerde<>();
  public static final SpecificAvroSerde<AlarmAction> INPUT_VALUE_CLASSES_SERDE =
      new SpecificAvroSerde<>();

  public static final Serdes.StringSerde EFFECTIVE_KEY_SERDE = new Serdes.StringSerde();
  public static final SpecificAvroSerde<EffectiveRegistration> EFFECTIVE_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
  public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public RegistrationRule(
      String inputTopicClasses,
      String inputTopicInstances,
      String outputTopicEffective,
      String outputTopicMonolog) {
    super(null, null);
    this.inputTopicClasses = inputTopicClasses;
    this.inputTopicInstances = inputTopicInstances;
    this.outputTopicEffective = outputTopicEffective;
    this.outputTopicMonolog = outputTopicMonolog;
  }

  @Override
  public Properties constructProperties() {
    final Properties props = super.constructProperties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-registration");

    return props;
  }

  @Override
  public Topology constructTopology(Properties props) {
    final StreamsBuilder builder = new StreamsBuilder();

    // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's
    // likely because you didn't set registry config
    Map<String, String> config = new HashMap<>();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

    INPUT_VALUE_INSTANCES_SERDE.configure(config, false);
    INPUT_VALUE_CLASSES_SERDE.configure(config, false);

    EFFECTIVE_VALUE_SERDE.configure(config, false);
    MONOLOG_VALUE_SERDE.configure(config, false);

    final KTable<String, AlarmAction> classesTable =
        builder.table(
            inputTopicClasses,
            Consumed.as("Classes-Table").with(INPUT_KEY_CLASSES_SERDE, INPUT_VALUE_CLASSES_SERDE));
    final KTable<String, Alarm> registeredTable =
        builder.table(
            inputTopicInstances,
            Consumed.as("Instances-Table")
                .with(INPUT_KEY_INSTANCES_SERDE, INPUT_VALUE_INSTANCES_SERDE));

    KTable<String, IntermediateMonolog> classesAndRegistered =
        registeredTable
            .leftJoin(
                classesTable,
                Alarm::getAction,
                new AlarmClassJoiner(),
                Materialized.with(Serdes.String(), MONOLOG_VALUE_SERDE))
            .filter(
                new Predicate<String, IntermediateMonolog>() {
                  @Override
                  public boolean test(String key, IntermediateMonolog value) {
                    log.debug("\n\nREGISTERED-CLASS JOIN RESULT: key: " + key + "value: " + value);
                    return true;
                  }
                });

    final KStream<String, IntermediateMonolog> withHeaders =
        classesAndRegistered.toStream().process(new MonologAddHeadersFactory());

    KStream<String, EffectiveRegistration> effective =
        withHeaders.mapValues(
            new ValueMapper<IntermediateMonolog, EffectiveRegistration>() {
              @Override
              public EffectiveRegistration apply(IntermediateMonolog value) {
                EffectiveRegistration result = null;

                if (value != null) {
                  result = EffectiveRegistration.newBuilder(value.getRegistration()).build();
                }
                return result;
              }
            });

    effective.to(
        outputTopicEffective,
        Produced.as("EffectiveRegistration").with(EFFECTIVE_KEY_SERDE, EFFECTIVE_VALUE_SERDE));

    withHeaders.to(
        outputTopicMonolog,
        Produced.as("MonologRegistration").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

    return builder.build();
  }

  private final class AlarmClassJoiner
      implements ValueJoiner<Alarm, AlarmAction, IntermediateMonolog> {

    public IntermediateMonolog apply(Alarm alarm, AlarmAction action) {

      // System.err.println("class joiner: " + registered);

      EffectiveRegistration effectiveReg =
          EffectiveRegistration.newBuilder().setAction(action).setAlarm(alarm).build();

      EffectiveNotification effectiveNot =
          EffectiveNotification.newBuilder()
              .setActivation(null)
              .setOverrides(new AlarmOverrideSet())
              .setState(AlarmState.Normal)
              .build();

      IntermediateMonolog monolog =
          IntermediateMonolog.newBuilder()
              .setRegistration(effectiveReg)
              .setNotification(effectiveNot)
              .setTransitions(new ProcessorTransitions())
              .build();

      return monolog;
    }
  }
}
