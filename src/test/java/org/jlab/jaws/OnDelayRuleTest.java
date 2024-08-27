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

public class OnDelayRuleTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, IntermediateMonolog> inputTopicMonolog;
  private TestOutputTopic<String, IntermediateMonolog> outputPassthroughTopic;
  private TestOutputTopic<AlarmOverrideKey, AlarmOverrideUnion> outputOverrideTopic;
  private Alarm instance1;
  private Alarm instance2;
  private AlarmAction class1;
  private AlarmActivationUnion active1;
  private AlarmActivationUnion active2;
  private IntermediateMonolog mono1;

  @Before
  public void setup() {
    final OnDelayRule rule = new OnDelayRule("monolog", "intermediate-ondelay", "alarm-overrides");

    final Properties props = rule.constructProperties();
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
    final Topology top = rule.constructTopology(props);

    // System.err.println(top.describe());

    testDriver = new TopologyTestDriver(top, props);

    // setup test topics
    inputTopicMonolog =
        testDriver.createInputTopic(
            rule.inputTopic,
            OnDelayRule.MONOLOG_KEY_SERDE.serializer(),
            OnDelayRule.MONOLOG_VALUE_SERDE.serializer());
    outputPassthroughTopic =
        testDriver.createOutputTopic(
            rule.outputTopic,
            OnDelayRule.MONOLOG_KEY_SERDE.deserializer(),
            OnDelayRule.MONOLOG_VALUE_SERDE.deserializer());
    outputOverrideTopic =
        testDriver.createOutputTopic(
            rule.overridesOutputTopic,
            OnDelayRule.OVERRIDE_KEY_SERDE.deserializer(),
            OnDelayRule.OVERRIDE_VALUE_SERDE.deserializer());

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

    EffectiveNotification effectiveNot =
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
  public void notOndelaying() {
    mono1.getRegistration().getAction().setOndelayseconds(null);

    inputTopicMonolog.pipeInput("alarm1", mono1);
    List<KeyValue<String, IntermediateMonolog>> passthroughResults =
        outputPassthroughTopic.readKeyValuesToList();
    List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> overrideResults =
        outputOverrideTopic.readKeyValuesToList();

    Assert.assertEquals(1, passthroughResults.size());
    Assert.assertEquals(0, overrideResults.size());
  }

  @Test
  public void ondelaying() {
    mono1.getRegistration().getAction().setOndelayseconds(10l);

    inputTopicMonolog.pipeInput("alarm1", mono1);
    // inputTopicMonolog.pipeInput("alarm2", mono1);
    List<KeyValue<String, IntermediateMonolog>> passthroughResults =
        outputPassthroughTopic.readKeyValuesToList();
    List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> overrideResults =
        outputOverrideTopic.readKeyValuesToList();

    System.err.println("\n\nInitial Passthrough:");
    for (KeyValue<String, IntermediateMonolog> pass : passthroughResults) {
      System.err.println(pass);
    }

    System.err.println("\n\nInitial Overrides:");
    for (KeyValue<AlarmOverrideKey, AlarmOverrideUnion> over : overrideResults) {
      System.err.println(over);
    }

    System.err.println("\n");

    Assert.assertEquals(1, passthroughResults.size());
    Assert.assertEquals(1, overrideResults.size());

    KeyValue<String, IntermediateMonolog> passResult = passthroughResults.get(0);

    Assert.assertEquals(true, passResult.value.getTransitions().getOndelaying());

    KeyValue<AlarmOverrideKey, AlarmOverrideUnion> result = overrideResults.get(0);

    Assert.assertEquals("alarm1", result.key.getName());
    Assert.assertEquals(OnDelayedOverride.class, result.value.getUnion().getClass());

    IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

    mono2.getNotification().getOverrides().setOndelayed(new OnDelayedOverride());
    mono2.getTransitions().setTransitionToActive(false);

    inputTopicMonolog.pipeInput("alarm1", mono2);

    passthroughResults = outputPassthroughTopic.readKeyValuesToList();
    overrideResults = outputOverrideTopic.readKeyValuesToList();

    System.err.println("\n\nFinal Passthrough:");
    for (KeyValue<String, IntermediateMonolog> pass : passthroughResults) {
      System.err.println(pass);
    }

    System.err.println("\n\nFinal Overrides:");
    for (KeyValue<AlarmOverrideKey, AlarmOverrideUnion> over : overrideResults) {
      System.err.println(over);
    }

    Assert.assertEquals(1, passthroughResults.size());
    Assert.assertEquals(0, overrideResults.size());
  }
}
