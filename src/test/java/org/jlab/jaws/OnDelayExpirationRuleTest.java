package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OnDelayExpirationRuleTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<AlarmOverrideKey, AlarmOverrideUnion> inputTopic;
  private TestOutputTopic<AlarmOverrideKey, AlarmOverrideUnion> outputTopic;
  private OnDelayedOverride override1;
  private OnDelayedOverride override2;

  @Before
  public void setup() {
    final OnDelayExpirationRule rule =
        new OnDelayExpirationRule("alarm-overrides", "alarm-overrides");

    final Properties props = rule.constructProperties();
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
    final Topology top = rule.constructTopology(props);
    testDriver = new TopologyTestDriver(top, props);

    // setup test topics
    inputTopic =
        testDriver.createInputTopic(
            rule.inputTopic,
            OnDelayExpirationRule.INPUT_KEY_SERDE.serializer(),
            OnDelayExpirationRule.INPUT_VALUE_SERDE.serializer());
    outputTopic =
        testDriver.createOutputTopic(
            rule.outputTopic,
            OnDelayExpirationRule.OUTPUT_KEY_SERDE.deserializer(),
            OnDelayExpirationRule.OUTPUT_VALUE_SERDE.deserializer());

    override1 = new OnDelayedOverride();
    override1.setExpiration(Instant.now().plusSeconds(5).getEpochSecond() * 1000);

    override2 = new OnDelayedOverride();
    override2.setExpiration(Instant.now().plusSeconds(5).getEpochSecond() * 1000);
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void tombstoneMsg() throws InterruptedException {
    List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> keyValues = new ArrayList<>();
    keyValues.add(
        KeyValue.pair(
            new AlarmOverrideKey("alarm1", OverriddenAlarmType.OnDelayed),
            new AlarmOverrideUnion(override1)));
    keyValues.add(
        KeyValue.pair(
            new AlarmOverrideKey("alarm1", OverriddenAlarmType.OnDelayed),
            new AlarmOverrideUnion(override2)));
    inputTopic.pipeKeyValueList(keyValues, Instant.now(), Duration.ofSeconds(5));
    testDriver.advanceWallClockTime(Duration.ofSeconds(5));
    KeyValue<AlarmOverrideKey, AlarmOverrideUnion> result =
        outputTopic.readKeyValuesToList().get(0);
    Assert.assertNull(result.value);
  }

  @Test
  public void notYetExpired() {
    inputTopic.pipeInput(
        new AlarmOverrideKey("alarm1", OverriddenAlarmType.OnDelayed),
        new AlarmOverrideUnion(override1));
    testDriver.advanceWallClockTime(Duration.ofSeconds(10));
    inputTopic.pipeInput(
        new AlarmOverrideKey("alarm2", OverriddenAlarmType.OnDelayed),
        new AlarmOverrideUnion(override2));
    KeyValue<AlarmOverrideKey, AlarmOverrideUnion> result =
        outputTopic.readKeyValuesToList().get(0);
    Assert.assertEquals("alarm1", result.key.getName());
    Assert.assertNull(result.value);
  }

  @Test
  public void expired() {
    inputTopic.pipeInput(
        new AlarmOverrideKey("alarm1", OverriddenAlarmType.OnDelayed),
        new AlarmOverrideUnion(override1));
    testDriver.advanceWallClockTime(Duration.ofSeconds(10));
    KeyValue<AlarmOverrideKey, AlarmOverrideUnion> result = outputTopic.readKeyValue();
    Assert.assertEquals("alarm1", result.key.getName());
    Assert.assertNull(result.value);
  }
}
