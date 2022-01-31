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

public class EffectiveStateRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopic;
    private TestOutputTopic<String, EffectiveActivation> effectiveActivationTopic;
    private TestOutputTopic<String, EffectiveAlarm> effectiveAlarmTopic;
    private AlarmInstance instance1;
    private AlarmInstance instance2;
    private AlarmClass class1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private IntermediateMonolog mono1;
    private EffectiveActivation effectiveAct;

    @Before
    public void setup() {
        final EffectiveStateRule rule = new EffectiveStateRule("monolog", "effective-activations", "effective-alarms");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        //System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic(rule.inputTopic, EffectiveStateRule.MONOLOG_KEY_SERDE.serializer(), EffectiveStateRule.MONOLOG_VALUE_SERDE.serializer());
        effectiveActivationTopic = testDriver.createOutputTopic(rule.effectiveActivationTopic, EffectiveStateRule.EFFECTIVE_ACTIVATION_KEY_SERDE.deserializer(), EffectiveStateRule.EFFECTIVE_ACTIVATION_VALUE_SERDE.deserializer());
        effectiveAlarmTopic = testDriver.createOutputTopic(rule.effectiveAlarmTopic, EffectiveStateRule.EFFECTIVE_ALARM_KEY_SERDE.deserializer(), EffectiveStateRule.EFFECTIVE_ALARM_VALUE_SERDE.deserializer());
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

        effectiveAct = EffectiveActivation.newBuilder()
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
        mono1.getActivation().setActual(null);
        mono1.getTransitions().setTransitionToActive(false);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("Normal", stateResults.get(0).value.getActivation().getState().name());
    }

    @Test
    public void latching() {
        mono1.getRegistration().getClass$().setLatching(true);
        mono1.getActivation().setActual(null);
        mono1.getTransitions().setLatching(true);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

        System.err.println("\n\nInitial State:");
        for(KeyValue<String, EffectiveAlarm> pass: stateResults) {
            System.err.println(pass);
        }

        System.err.println("\n");

        Assert.assertEquals(1, stateResults.size());

        KeyValue<String, EffectiveAlarm> passResult = stateResults.get(0);

        Assert.assertEquals("ActiveLatched", passResult.value.getActivation().getState().name());

        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        mono2.getActivation().getOverrides().setLatched(new LatchedOverride());

        inputTopic.pipeInput("alarm1", mono2);

        stateResults = effectiveAlarmTopic.readKeyValuesToList();

        System.err.println("\n\nFinal State:");
        for(KeyValue<String, EffectiveAlarm> pass: stateResults) {
            System.err.println(pass);
        }

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("ActiveLatched", passResult.value.getActivation().getState().name());
    }

    @Test
    public void shelved() {
        mono1.setActivation(effectiveAct);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, EffectiveAlarm>> stateResults = effectiveAlarmTopic.readKeyValuesToList();

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("Active", stateResults.get(0).value.getActivation().getState().name());


        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        mono2.getActivation().getOverrides().setShelved(new ShelvedOverride(false, 12345l, ShelvedReason.Other, null));

        inputTopic.pipeInput("alarm1", mono2);

        stateResults = effectiveAlarmTopic.readKeyValuesToList();

        System.err.println("\n\nFinal State:");
        for(KeyValue<String, EffectiveAlarm> pass: stateResults) {
            System.err.println(pass);
        }

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("NormalContinuousShelved", stateResults.get(0).value.getActivation().getState().name());
    }
}
