package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OneShotRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopicMonolog;
    private TestOutputTopic<String, IntermediateMonolog> outputPassthroughTopic;
    private TestOutputTopic<OverriddenAlarmKey, AlarmOverrideUnion> outputOverrideTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
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

        registered1 = new AlarmRegistration();
        registered2 = new AlarmRegistration();

        registered1.setClass$("base");
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$("base");
        registered2.setProducer(new SimpleProducer());
        registered2.setLatching(false);

        class1 = new AlarmClass();
        class1.setLatching(true);
        class1.setCategory(AlarmCategory.CAMAC);
        class1.setFilterable(true);
        class1.setCorrectiveaction("fix it");
        class1.setLocation(AlarmLocation.A4);
        class1.setPriority(AlarmPriority.P3_MINOR);
        class1.setScreenpath("/tmp");
        class1.setPointofcontactusername("tester");
        class1.setRationale("because");

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        EffectiveRegistration effectiveReg = EffectiveRegistration.newBuilder()
                .setClass$(class1)
                .setActual(registered1)
                .setCalculated(EffectiveRegistrationRule.computeEffectiveRegistration(registered1, class1))
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
    public void notOneshot() {
        ShelvedOverride shelved = new ShelvedOverride();
        shelved.setOneshot(false);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedReason.Other);
        mono1.getActivation().getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(1, passthroughResults.size());
    }

    @Test
    public void oneshot() {
        ShelvedOverride shelved = new ShelvedOverride();
        shelved.setOneshot(true);
        shelved.setExpiration(1000);
        shelved.setReason(ShelvedReason.Other);
        mono1.getActivation().getOverrides().setShelved(shelved);

        mono1.getTransitions().setTransitionToActive(false);
        mono1.getTransitions().setTransitionToNormal(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

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
        mono1.getActivation().getOverrides().setShelved(shelved);

        inputTopicMonolog.pipeInput("alarm1", mono1);


        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        inputTopicMonolog.pipeInput("alarm1", mono2);



        List<KeyValue<String, IntermediateMonolog>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, AlarmOverrideUnion>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(0, overrideResults.size());
        Assert.assertEquals(2, passthroughResults.size());
    }
}
