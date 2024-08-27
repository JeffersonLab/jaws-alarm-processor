package org.jlab.jaws;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EffectiveStateRuleTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, IntermediateMonolog> inputTopic;
  private TestOutputTopic<String, EffectiveNotification> EffectiveNotificationTopic;
  private TestOutputTopic<String, EffectiveAlarm> effectiveAlarmTopic;
  private Alarm instance1;
  private Alarm instance2;
  private AlarmAction class1;
  private AlarmActivationUnion active1;
  private AlarmActivationUnion active2;
  private IntermediateMonolog mono1;
  private EffectiveNotification effectiveNot;

  @Before
  public void setup() {
    final EffectiveStateRule rule =
        new EffectiveStateRule("monolog", "effective-activations", "effective-alarms");

    final Properties props = rule.constructProperties();
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
    final Topology top = rule.constructTopology(props);

    // System.err.println(top.describe());

    testDriver = new TopologyTestDriver(top, props);

    // setup test topics
    inputTopic =
        testDriver.createInputTopic(
            rule.inputTopic,
            EffectiveStateRule.MONOLOG_KEY_SERDE.serializer(),
            EffectiveStateRule.MONOLOG_VALUE_SERDE.serializer());
    EffectiveNotificationTopic =
        testDriver.createOutputTopic(
            rule.EffectiveNotificationTopic,
            EffectiveStateRule.EFFECTIVE_NOTIFICATION_KEY_SERDE.deserializer(),
            EffectiveStateRule.EFFECTIVE_NOTIFICATION_VALUE_SERDE.deserializer());
    effectiveAlarmTopic =
        testDriver.createOutputTopic(
            rule.effectiveAlarmTopic,
            EffectiveStateRule.EFFECTIVE_ALARM_KEY_SERDE.deserializer(),
            EffectiveStateRule.EFFECTIVE_ALARM_VALUE_SERDE.deserializer());
    instance1 = new Alarm();
    instance2 = new Alarm();

    instance1.setAction("base");
    instance1.setSource(new Source());
    instance1.setLocation(Arrays.asList("NL"));

    instance2.setAction("base");
    instance2.setSource(new Source());
    instance2.setLocation(Arrays.asList("NL"));

    class1 = new AlarmAction();
    class1.setLatchable(true);
    class1.setSystem("CAMAC");
    class1.setFilterable(true);
    class1.setCorrectiveaction("fix it");
    class1.setPriority(AlarmPriority.P3_MINOR);
    class1.setRationale("because");

    active1 = new AlarmActivationUnion();
    active2 = new AlarmActivationUnion();

    active1.setUnion(new Activation());
    active2.setUnion(new Activation());

    EffectiveRegistration effectiveReg =
        EffectiveRegistration.newBuilder().setAction(class1).setAlarm(instance1).build();

    effectiveNot =
        EffectiveNotification.newBuilder()
            .setActivation(active1)
            .setOverrides(new AlarmOverrideSet())
            .setState(AlarmState.Normal)
            .build();

    mono1 = new IntermediateMonolog();
    mono1.setRegistration(effectiveReg);
    mono1.setNotification(effectiveNot);
    mono1.setTransitions(new ProcessorTransitions());
    mono1.getTransitions().setTransitionToActive(true);
    mono1.getTransitions().setTransitionToNormal(false);
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void notLatching() {
    mono1.getNotification().setActivation(null);
    mono1.getTransitions().setTransitionToActive(false);

    inputTopic.pipeInput("alarm1", mono1);
    List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

    Assert.assertEquals(1, stateResults.size());
    Assert.assertEquals("Normal", stateResults.get(0).value.getNotification().getState().name());
  }

  @Test
  public void latching() {
    mono1.getRegistration().getAction().setLatchable(true);
    mono1.getNotification().setActivation(null);
    mono1.getTransitions().setLatching(true); // should result in dropped message when transitioning

    inputTopic.pipeInput("alarm1", mono1);
    List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

    System.err.println("\n\nInitial State:");
    for (KeyValue<String, EffectiveAlarm> pass : stateResults) {
      System.err.println(pass);
    }

    System.err.println("\n");

    Assert.assertEquals(0, stateResults.size());

    IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

    mono2.getNotification().getOverrides().setLatched(new LatchedOverride());
    mono2
        .getTransitions()
        .setLatching(false); // We're no longer transitioning AND we have override!

    inputTopic.pipeInput("alarm1", mono2);

    stateResults = effectiveAlarmTopic.readKeyValuesToList();

    System.err.println("\n\nFinal State:");
    for (KeyValue<String, EffectiveAlarm> pass : stateResults) {
      System.err.println(pass);
    }

    KeyValue<String, EffectiveAlarm> passResult = stateResults.get(0);

    Assert.assertEquals(1, stateResults.size());
    Assert.assertEquals("ActiveLatched", passResult.value.getNotification().getState().name());
  }

  @Test
  public void shelved() {
    mono1.setNotification(effectiveNot);

    inputTopic.pipeInput("alarm1", mono1);
    List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

    Assert.assertEquals(1, stateResults.size());
    Assert.assertEquals("Active", stateResults.get(0).value.getNotification().getState().name());

    IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

    mono2
        .getNotification()
        .getOverrides()
        .setShelved(new ShelvedOverride(false, 12345l, ShelvedReason.Other, null));

    inputTopic.pipeInput("alarm1", mono2);

    stateResults = effectiveAlarmTopic.readKeyValuesToList();

    System.err.println("\n\nFinal State:");
    for (KeyValue<String, EffectiveAlarm> pass : stateResults) {
      System.err.println(pass);
    }

    Assert.assertEquals(1, stateResults.size());
    Assert.assertEquals(
        "NormalContinuousShelved", stateResults.get(0).value.getNotification().getState().name());
  }
}
