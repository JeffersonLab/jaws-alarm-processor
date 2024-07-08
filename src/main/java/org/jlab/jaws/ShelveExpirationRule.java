package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Expires Shelved overrides by setting timers. */
public class ShelveExpirationRule extends ProcessingRule {

  private static final Logger log = LoggerFactory.getLogger(ShelveExpirationRule.class);

  public static final SpecificAvroSerde<AlarmOverrideKey> INPUT_KEY_SERDE =
      new SpecificAvroSerde<>();
  public static final SpecificAvroSerde<AlarmOverrideUnion> INPUT_VALUE_SERDE =
      new SpecificAvroSerde<>();
  public static final SpecificAvroSerde<AlarmOverrideKey> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
  public static final SpecificAvroSerde<AlarmOverrideUnion> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

  /**
   * Enumerations of all channels with expiration timers, mapped to the cancellable Executor handle.
   */
  public static Map<String, Cancellable> channelHandleMap = new ConcurrentHashMap<>();

  public ShelveExpirationRule(String inputTopic, String outputTopic) {
    super(inputTopic, outputTopic);
  }

  @Override
  public Properties constructProperties() {
    final Properties props = super.constructProperties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "jaws-effective-processor-shelve");

    return props;
  }

  /**
   * Create the Kafka Streams Domain Specific Language (DSL) Topology.
   *
   * @return The Topology
   */
  public Topology constructTopology(Properties props) {
    final StreamsBuilder builder = new StreamsBuilder();
    Map<String, String> config = new HashMap<>();

    String value = props.getProperty(SCHEMA_REGISTRY_URL_CONFIG);

    config.put(SCHEMA_REGISTRY_URL_CONFIG, value);
    INPUT_KEY_SERDE.configure(config, true);
    INPUT_VALUE_SERDE.configure(config, false);

    final KStream<AlarmOverrideKey, AlarmOverrideUnion> input =
        builder.stream(inputTopic, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

    final KStream<AlarmOverrideKey, AlarmOverrideUnion> shelvedOnly =
        input.filter(
            new Predicate<AlarmOverrideKey, AlarmOverrideUnion>() {
              @Override
              public boolean test(AlarmOverrideKey key, AlarmOverrideUnion value) {
                return key.getType() == OverriddenAlarmType.Shelved;
              }
            });

    final KStream<AlarmOverrideKey, AlarmOverrideUnion> output =
        shelvedOnly.process(new MyProcessorSupplier());

    output.to(outputTopic, Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

    return builder.build();
  }

  /**
   * Factory to create Kafka Streams Processor instances; references a stateStore to maintain
   * previous RegisteredAlarms.
   */
  private final class MyProcessorSupplier
      implements ProcessorSupplier<
          AlarmOverrideKey, AlarmOverrideUnion, AlarmOverrideKey, AlarmOverrideUnion> {

    /**
     * Return a new {@link Processor} instance.
     *
     * @return a new {@link Processor} instance
     */
    @Override
    public Processor<AlarmOverrideKey, AlarmOverrideUnion, AlarmOverrideKey, AlarmOverrideUnion>
        get() {
      return new Processor<>() {
        private ProcessorContext<AlarmOverrideKey, AlarmOverrideUnion> context;

        @Override
        public void init(ProcessorContext<AlarmOverrideKey, AlarmOverrideUnion> context) {
          this.context = context;
        }

        @Override
        public void process(Record<AlarmOverrideKey, AlarmOverrideUnion> input) {
          KeyValue<AlarmOverrideKey, AlarmOverrideUnion> result =
              null; // null returned to mean no record

          log.debug("Handling message: {}={}", input.key(), input.value());

          // Get (and remove) timer handle (if exists)
          Cancellable handle = channelHandleMap.remove(input.key().getName());

          // If exists, we always cancel timers
          if (handle != null) {
            log.debug("Timer Cancelled");
            handle.cancel();
          } else {
            log.debug("No Timer exists");
          }

          ShelvedOverride sa = null;

          if (input.value() != null && input.value().getUnion() instanceof ShelvedOverride) {
            sa = (ShelvedOverride) input.value().getUnion();
          }

          if (sa != null && sa.getExpiration() > 0) { // Set new timer
            Instant ts = Instant.ofEpochMilli(sa.getExpiration());
            Instant now = Instant.now();
            long delayInSeconds = Duration.between(now, ts).getSeconds();
            if (now.isAfter(ts)) {
              delayInSeconds =
                  1; // If expiration is in the past then expire immediately (zero is invalid)
            }
            log.debug("Scheduling {} for delay of: {} seconds ", input.key(), delayInSeconds);

            Cancellable newHandle =
                context.schedule(
                    Duration.ofSeconds(delayInSeconds),
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> {
                      log.debug("Punctuation triggered for: {}", input.key());

                      // Attempt to cancel timer immediately so only run once; can fail if schedule
                      // doesn't return fast enough before timer triggered!
                      Cancellable h = channelHandleMap.remove(input.key().getName());
                      if (h != null) {
                        h.cancel();
                      }

                      long time = System.currentTimeMillis();

                      Record<AlarmOverrideKey, AlarmOverrideUnion> output =
                          new Record<>(input.key(), null, time);

                      populateHeaders(output);

                      context.forward(output);
                    });

            Cancellable oldHandle = channelHandleMap.put(input.key().getName(), newHandle);

            // This is to ensure we cancel every timer before losing it's handle otherwise it'll run
            // forever (they repeat until cancelled)
            if (oldHandle
                != null) { // This should only happen if timer callback is unable to cancel future
              // runs (because handle assignment in map too slow)
              oldHandle.cancel();
            }
          } else {
            log.debug("Either null value or null expiration so no timer set!");
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
