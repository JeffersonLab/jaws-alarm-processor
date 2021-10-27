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

public class EffectiveStateRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopic;
    private TestOutputTopic<String, IntermediateMonolog> outputTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private AlarmClass class1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private IntermediateMonolog mono1;
    private EffectiveActivation effectiveAct;

    @Before
    public void setup() {
        final EffectiveStateRule rule = new EffectiveStateRule("monolog", "alarms");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);

        //System.err.println(top.describe());

        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic(rule.inputTopic, EffectiveStateRule.MONOLOG_KEY_SERDE.serializer(), EffectiveStateRule.MONOLOG_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, EffectiveStateRule.MONOLOG_KEY_SERDE.deserializer(), EffectiveStateRule.MONOLOG_VALUE_SERDE.deserializer());
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
        List<KeyValue<String, IntermediateMonolog>> stateResults = outputTopic.readKeyValuesToList();

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("Normal", stateResults.get(0).value.getActivation().getState().name());
    }

    @Test
    public void latching() {
        mono1.getRegistration().getCalculated().setLatching(true);
        mono1.getActivation().setActual(null);
        mono1.getTransitions().setLatching(true);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, IntermediateMonolog>> stateResults = outputTopic.readKeyValuesToList();

        System.err.println("\n\nInitial State:");
        for(KeyValue<String, IntermediateMonolog> pass: stateResults) {
            System.err.println(pass);
        }

        System.err.println("\n");

        Assert.assertEquals(1, stateResults.size());

        KeyValue<String, IntermediateMonolog> passResult = stateResults.get(0);

        Assert.assertEquals("ActiveLatched", passResult.value.getActivation().getState().name());

        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        mono2.getActivation().getOverrides().setLatched(new LatchedOverride());

        inputTopic.pipeInput("alarm1", mono2);

        stateResults = outputTopic.readKeyValuesToList();

        System.err.println("\n\nFinal State:");
        for(KeyValue<String, IntermediateMonolog> pass: stateResults) {
            System.err.println(pass);
        }

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("ActiveLatched", passResult.value.getActivation().getState().name());
    }

    @Test
    public void shelved() {
        mono1.setActivation(effectiveAct);

        inputTopic.pipeInput("alarm1", mono1);
        List<KeyValue<String, IntermediateMonolog>> stateResults = outputTopic.readKeyValuesToList();

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("Active", stateResults.get(0).value.getActivation().getState().name());


        IntermediateMonolog mono2 = IntermediateMonolog.newBuilder(mono1).build();

        mono2.getActivation().getOverrides().setShelved(new ShelvedOverride(false, 12345l, ShelvedReason.Other, null));

        inputTopic.pipeInput("alarm1", mono2);

        stateResults = outputTopic.readKeyValuesToList();

        System.err.println("\n\nFinal State:");
        for(KeyValue<String, IntermediateMonolog> pass: stateResults) {
            System.err.println(pass);
        }

        Assert.assertEquals(1, stateResults.size());
        Assert.assertEquals("NormalContinuousShelved", stateResults.get(0).value.getActivation().getState().name());
    }
}
