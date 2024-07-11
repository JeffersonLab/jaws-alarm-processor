package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adds an OnDelayed override for alarms with on-delays that become active. */
public class OnDelayRule extends ProcessingRule {

  private static final Logger log = LoggerFactory.getLogger(OnDelayRule.class);

  String overridesOutputTopic;

  public static final Serdes.StringSerde MONOLOG_KEY_SERDE = new Serdes.StringSerde();
  public static final SpecificAvroSerde<IntermediateMonolog> MONOLOG_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public static final SpecificAvroSerde<AlarmOverrideKey> OVERRIDE_KEY_SERDE =
      new SpecificAvroSerde<>();
  public static final SpecificAvroSerde<AlarmOverrideUnion> OVERRIDE_VALUE_SERDE =
      new SpecificAvroSerde<>();

  public static final Serdes.StringSerde ONDELAY_STORE_KEY_SERDE = new Serdes.StringSerde();
  public static final Serdes.StringSerde ONDELAY_STORE_VALUE_SERDE = new Serdes.StringSerde();

  public OnDelayRule(String inputTopic, String outputTopic, String overridesOutputTopic) {
    super(inputTopic, outputTopic);
    this.overridesOutputTopic = overridesOutputTopic;
  }

  @Override
  public Properties constructProperties() {
    final Properties props = super.constructProperties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-ondelay");

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

    OVERRIDE_KEY_SERDE.configure(config, true);
    OVERRIDE_VALUE_SERDE.configure(config, false);

    final KTable<String, IntermediateMonolog> monologTable =
        builder.table(
            inputTopic, Consumed.as("Monolog-Table").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

    final KStream<String, IntermediateMonolog> monologStream = monologTable.toStream();

    KStream<String, IntermediateMonolog> ondelayOverrideMonolog =
        monologStream.filter(
            new Predicate<String, IntermediateMonolog>() {
              @Override
              public boolean test(String key, IntermediateMonolog value) {
                log.debug("Filtering: " + key + ", value: " + value);
                return value.getRegistration().getClass$() != null
                    && value.getRegistration().getClass$().getOndelayseconds() != null
                    && value.getRegistration().getClass$().getOndelayseconds() > 0
                    && value.getTransitions().getTransitionToActive();
              }
            });

    KStream<AlarmOverrideKey, AlarmOverrideUnion> ondelayOverrides =
        ondelayOverrideMonolog.map(
            new KeyValueMapper<
                String, IntermediateMonolog, KeyValue<AlarmOverrideKey, AlarmOverrideUnion>>() {
              @Override
              public KeyValue<AlarmOverrideKey, AlarmOverrideUnion> apply(
                  String key, IntermediateMonolog value) {
                Long expiration =
                    System.currentTimeMillis()
                        + (value.getRegistration().getClass$().getOndelayseconds() * 1000);
                return new KeyValue<>(
                    new AlarmOverrideKey(key, OverriddenAlarmType.OnDelayed),
                    new AlarmOverrideUnion(new OnDelayedOverride(expiration)));
              }
            });

    ondelayOverrides.to(
        overridesOutputTopic,
        Produced.as("OnDelay-Overrides").with(OVERRIDE_KEY_SERDE, OVERRIDE_VALUE_SERDE));

    final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("OnDelayStateStore"),
                ONDELAY_STORE_KEY_SERDE,
                ONDELAY_STORE_VALUE_SERDE)
            .withCachingEnabled();

    builder.addStateStore(storeBuilder);

    final KStream<String, IntermediateMonolog> passthrough =
        monologStream.process(
            new MyProcessorSupplier(storeBuilder.name()),
            Named.as("OnDelayTransitionProcessor"),
            storeBuilder.name());

    passthrough.to(
        outputTopic,
        Produced.as("OnDelay-Passthrough").with(MONOLOG_KEY_SERDE, MONOLOG_VALUE_SERDE));

    Topology top = builder.build();

    return top;
  }

  private static final class MyProcessorSupplier
      implements ProcessorSupplier<String, IntermediateMonolog, String, IntermediateMonolog> {

    private final String storeName;

    /**
     * Create a new ProcessorSupplier.
     *
     * @param storeName The state store name
     */
    public MyProcessorSupplier(String storeName) {
      this.storeName = storeName;
    }

    /**
     * Return a new {@link Processor} instance.
     *
     * @return a new {@link Processor} instance
     */
    @Override
    public Processor<String, IntermediateMonolog, String, IntermediateMonolog> get() {
      return new Processor<>() {
        private KeyValueStore<String, String> store;
        private ProcessorContext<String, IntermediateMonolog> context;

        @Override
        public void init(ProcessorContext<String, IntermediateMonolog> context) {
          this.context = context;
          this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, IntermediateMonolog> input) {
          log.debug(
              "Processing key = {}, value = \n\tInstance: {}\n\tClass: {}\n\tAct: {}\n\tOver: {}\n\tTrans: {}",
              input.key(),
              input.value().getRegistration().getInstance(),
              input.value().getRegistration().getClass$(),
              input.value().getNotification().getActivation(),
              input.value().getNotification().getOverrides(),
              input.value().getTransitions());

          long timestamp = System.currentTimeMillis();

          Record<String, IntermediateMonolog> output =
              new Record<>(input.key(), input.value(), timestamp);

          // Skip the filter unless ondelay is registered
          if (output.value().getRegistration().getClass$() != null
              && output.value().getRegistration().getClass$().getOndelayseconds() != null
              && output.value().getRegistration().getClass$().getOndelayseconds() > 0) {

            // Check if already ondelay in-progress
            boolean ondelaying = store.get(output.key()) != null;

            // Check if ondelayed
            boolean ondelayed =
                output.value().getNotification().getOverrides().getOndelayed() != null;

            // Check if we need to ondelay
            boolean needToOnDelay = output.value().getTransitions().getTransitionToActive();

            if (ondelayed) {
              ondelaying = false;
            } else if (needToOnDelay) {
              ondelaying = true;
            }

            if (ondelaying) { // Update transition state
              output.value().getTransitions().setOndelaying(true);
            }

            log.debug("ondelayed: {}", ondelayed);
            log.debug("needToOnDelay: {}", needToOnDelay);
            log.debug("ondelaying: {}", ondelaying);

            store.put(output.key(), ondelaying ? "y" : null);
          }

          populateHeaders(output);

          context.forward(output);
        }

        @Override
        public void close() {
          // Nothing to do
        }
      };
    }
  }
}
