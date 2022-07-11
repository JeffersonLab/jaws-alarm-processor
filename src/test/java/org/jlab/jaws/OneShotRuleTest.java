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

public class OneShotRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopicMonolog;
    private TestOutputTopic<String, IntermediateMonolog> outputPassthroughTopic;
    private TestOutputTopic<AlarmOverrideKey, AlarmOverrideUnion> outputOverrideTopic;
    private AlarmInstance instance1;
    private AlarmInstance instance2;
    private AlarmClass class1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private IntermediateMonolog mono1;

    @Before
    public void setup() {
        final OneShotRule rule = new OneShotRule("latch-processed", "oneshot-processed", "overridden-alarms");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicMonolog = testDriver.createInputTopic(rule.inputTopic, OneShotRule.MONOLOG_KEY_SERDE.serializer(), OneShotRule.MONOLOG_VALUE_SERDE.serializer());
        outputPassthroughTopic = testDriver.createOutputTopic(rule.outputTopic, OneShotRule.MONOLOG_KEY_SERDE.deserializer(), OneShotRule.MONOLOG_VALUE_SERDE.deserializer());
        outputOverrideTopic = testDriver.createOutputTopic(rule.overridesOutputTopic, OneShotRule.OVERRIDE_KEY_SERDE.deserializer(), OneShotRule.OVERRIDE_VALUE_SERDE.deserializer());

        instance1 = new AlarmInstance();
        instance2 = new AlarmInstance();

        instance1.setAlarmclass("base");
        instance1.setSource(new Source());
        instance1.setLocation(Arrays.asList("NL"));

        instance2.setAlarmclass("base");
        instance2.setSource(new Source());
        instance2.setLocation(Arrays.asList("NL"));

        class1 = new AlarmClass();
        class1.setLatchable(true);
        class1.setCategory("CAMAC");
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setUnion(new Activation());
        active2.setUnion(new Activation());

        EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder()
                .setClass$(class1)
                .setInstance(instance1)
                .build();

        EffectiveNotification effectiveNot = EffectiveNotification.newBuilder()
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
    public void notOneshot() {
        ShelvedOverride shelved = new ShelvedOverride();
        shelved.setOneshot(false);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedReason.Other);
        mono1.getNotification().getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(1, passthroughResults.size());
    }

    @Test
    public void oneshot() {
        ShelvedOverride shelved = new ShelvedOverride();
        shelved.setOneshot(true);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedReason.Other);
        mono1.getNotification().getOverrides().setShelved(shelved);

        mono1.getTransitions().setTransitionToActive(false);
        mono1.getTransitions().setTransitionToNormal(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(1, overrideResults.size());
        Assert.assertEquals(1, passthroughResults.size());

        KeyValue<String, IntermediateMonolog> passResult = passthroughResults.get(0);

        Assert.assertEquals(true, passResult.value.getTransitions().getUnshelving());
    }

    @Test
    public void oneshotABunch() {
        ShelvedOverride shelved = new ShelvedOverride();
        shelved.setOneshot(true);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedReason.Other);
        mono1.getNotification().getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);


        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        inputTopicMonolog.pipeInput("alarm1", mono2);



        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<AlarmOverrideKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(2, passthroughResults.size());
    }
}
