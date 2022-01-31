package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class LatchRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopicMonolog;
    private TestOutputTopic<String, IntermediateMonolog> outputPassthroughTopic;
    private TestOutputTopic<OverriddenAlarmKey, AlarmOverrideUnion> outputOverrideTopic;
    private AlarmInstance instance1;
    private AlarmInstance instance2;
    private AlarmClass class1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private IntermediateMonolog mono1;

    @Before
    public void setup() {
        final LatchRule rule = new LatchRule("monolog", "latch-processed", "overridden-alarms");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        //System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicMonolog = testDriver.createInputTopic(rule.inputTopic, LatchRule.MONOLOG_KEY_SERDE.serializer(), LatchRule.MONOLOG_VALUE_SERDE.serializer());
        outputPassthroughTopic = testDriver.createOutputTopic(rule.outputTopic, LatchRule.MONOLOG_KEY_SERDE.deserializer(), LatchRule.MONOLOG_VALUE_SERDE.deserializer());
        outputOverrideTopic = testDriver.createOutputTopic(rule.overridesOutputTopic, LatchRule.OVERRIDE_KEY_SERDE.deserializer(), LatchRule.OVERRIDE_VALUE_SERDE.deserializer());

        instance1 = new AlarmInstance();
        instance2 = new AlarmInstance();

        instance1.setClass$("base");
        instance1.setProducer(new SimpleProducer());
        instance1.setLocation(Arrays.asList("NL"));

        instance2.setClass$("base");
        instance2.setProducer(new SimpleProducer());
        instance2.setLocation(Arrays.asList("NL"));

        class1 = new AlarmClass();
        class1.setLatching(true);
        class1.setCategory("CAMAC");
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder()
                .setClass$(class1)
                .setInstance(instance1)
                .build();

        EffectiveActivation effectiveAct = EffectiveActivation.newBuilder()
                .setActual(active1)
                .setOverrides(new AlarmOverrideSet())
                .setState(AlarmState.Normal)
                .build();

        mono1 = new IntermediateMonolog();
        mono1.setRegistration(effectiveReg);
        mono1.setActivation(effectiveAct);
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
        mono1.getRegistration().getClass$().setLatching(false);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }

    @Test
    public void latching() {
        mono1.getRegistration().getClass$().setLatching(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nInitial Passthrough:");
        for(KeyValue<String, IntermediateMonolog> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nInitial Overrides:");
        for(KeyValue<OverriddenAlarmKey, AlarmOverrideUnion> over: overrideResults) {
            System.err.println(over);
        }

        System.err.println("\n");

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(1, overrideResults.size());

        KeyValue<String, IntermediateMonolog> passResult = passthroughResults.get(0);

        Assert.assertEquals(true, passResult.value.getTransitions().getLatching());

        KeyValue<OverriddenAlarmKey, AlarmOverrideUnion> result = overrideResults.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertEquals(new AlarmOverrideUnion(new LatchedOverride()), result.value);

        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        mono2.getActivation().getOverrides().setLatched(new LatchedOverride());
        mono2.getTransitions().setTransitionToActive(false);

        inputTopicMonolog.pipeInput("alarm1", mono2);

        passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nFinal Passthrough:");
        for(KeyValue<String, IntermediateMonolog> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nFinal Overrides:");
        for(KeyValue<OverriddenAlarmKey, AlarmOverrideUnion> over: overrideResults) {
            System.err.println(over);
        }

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }
}
