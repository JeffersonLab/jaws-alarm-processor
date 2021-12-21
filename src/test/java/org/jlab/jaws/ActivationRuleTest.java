package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ActivationRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, IntermediateMonolog> inputTopicRegisteredMonolog;
    private TestInputTopic<String, AlarmActivationUnion> inputTopicActive;
    private TestInputTopic<OverriddenAlarmKey, AlarmOverrideUnion> inputTopicOverridden;
    private TestOutputTopic<String, IntermediateMonolog> outputTopic;
    private AlarmInstance registered1;
    private AlarmInstance registered2;
    private AlarmClass class1;
    private AlarmInstance effectiveRegistered1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private IntermediateMonolog registeredMonolog1;
    private EffectiveRegistration effectiveReg;
    EffectiveActivation effectiveAct;

    @Before
    public void setup() {
        final ActivationRule rule = new ActivationRule("effective-registrations", "active-alarms", "overridden-alarms", "monolog");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicRegisteredMonolog = testDriver.createInputTopic(rule.inputTopicRegisteredMonolog, ActivationRule.MONOLOG_KEY_SERDE.serializer(), ActivationRule.MONOLOG_VALUE_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(rule.inputTopicActive, ActivationRule.ACTIVE_KEY_SERDE.serializer(), ActivationRule.ACTIVE_VALUE_SERDE.serializer());
        inputTopicOverridden = testDriver.createInputTopic(rule.inputTopicOverridden, ActivationRule.OVERRIDE_KEY_SERDE.serializer(), ActivationRule.OVERRIDE_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, ActivationRule.MONOLOG_KEY_SERDE.deserializer(), ActivationRule.MONOLOG_VALUE_SERDE.deserializer());

        registered1 = new AlarmInstance();
        registered2 = new AlarmInstance();

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

        class1.setMaskedby("alarm1");
        class1.setOffdelayseconds(5l);
        class1.setOndelayseconds(5l);

        effectiveRegistered1 = RegistrationRule.computeEffectiveRegistration(registered1, class1);

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        effectiveReg = EffectiveRegistration.newBuilder()
                .setClass$(class1)
                .setActual(registered1)
                .setCalculated(RegistrationRule.computeEffectiveRegistration(registered1, class1))
                .build();

        effectiveAct = EffectiveActivation.newBuilder()
                .setActual(active1)
                .setOverrides(new AlarmOverrideSet())
                .setState(AlarmState.Normal)
                .build();

        registeredMonolog1 = IntermediateMonolog.newBuilder()
                .setRegistration(effectiveReg)
                .setActivation(effectiveAct)
                .setTransitions(new ProcessorTransitions())
                .build();
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void noRegistrationOrActiveButOverride() {
        AlarmOverrideUnion AlarmOverrideUnion1 = new AlarmOverrideUnion();
        LatchedOverride latchedOverride = new LatchedOverride();
        AlarmOverrideUnion1.setMsg(latchedOverride);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), AlarmOverrideUnion1);

        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void count() {
        inputTopicActive.pipeInput("alarm1", active1);
        inputTopicRegisteredMonolog.pipeInput("alarm1", registeredMonolog1);
        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void unsetActiveWithNoRegistration() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicActive.pipeInput("alarm1", null);
        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, IntermediateMonolog> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(1).value);
    }

    @Test
    public void content() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegisteredMonolog.pipeInput("alarm1", registeredMonolog1);
        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, IntermediateMonolog> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(2, results.size());

        KeyValue<String, IntermediateMonolog> result2 = results.get(1);

        IntermediateMonolog expected = new IntermediateMonolog(effectiveReg, effectiveAct, new ProcessorTransitions());

        System.err.println(expected);
        System.err.println(result2.value);

        Assert.assertEquals("alarm1", result2.key);
        Assert.assertEquals(expected, result2.value);
    }

    @Test
    public void addOverride() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegisteredMonolog.pipeInput("alarm1", registeredMonolog1);

        AlarmOverrideUnion AlarmOverrideUnion1 = new AlarmOverrideUnion();
        LatchedOverride latchedOverride = new LatchedOverride();
        AlarmOverrideUnion1.setMsg(latchedOverride);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), AlarmOverrideUnion1);

        AlarmOverrideUnion AlarmOverrideUnion2 = new AlarmOverrideUnion();
        DisabledOverride disabledOverride = new DisabledOverride();
        disabledOverride.setComments("Testing");
        AlarmOverrideUnion2.setMsg(disabledOverride);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), AlarmOverrideUnion2);


        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), null);


        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, IntermediateMonolog> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(5, results.size());

        KeyValue<String, IntermediateMonolog> result = results.get(4);

        AlarmOverrideSet overrides = AlarmOverrideSet.newBuilder()
                .setLatched(new LatchedOverride())
                .build();

        EffectiveActivation ea = EffectiveActivation.newBuilder(effectiveAct).build();
        ea.setOverrides(overrides);

        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, new ProcessorTransitions()), result.value);
    }

    @Test
    public void transitions() {
        inputTopicRegisteredMonolog.pipeInput("alarm1", registeredMonolog1);

        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        inputTopicActive.pipeInput("alarm1", null);

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicActive.pipeInput("alarm1", active1);

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicActive.pipeInput("alarm1", active1);

        List<KeyValue<String, IntermediateMonolog>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(5, results.size());



        System.err.println("\n\n\n");
        for(KeyValue<String, IntermediateMonolog> result: results) {
            System.err.println(result);
        }

        KeyValue<String, IntermediateMonolog> result0 = results.get(0);
        KeyValue<String, IntermediateMonolog> result1 = results.get(1);
        KeyValue<String, IntermediateMonolog> result2 = results.get(2);
        KeyValue<String, IntermediateMonolog> result3 = results.get(3);
        KeyValue<String, IntermediateMonolog> result4 = results.get(4);

        Assert.assertEquals("alarm1", result0.key);

        AlarmOverrideSet overrides = AlarmOverrideSet.newBuilder()
                .build();

        ProcessorTransitions transitions = ProcessorTransitions.newBuilder().build();
        EffectiveActivation ea = EffectiveActivation.newBuilder(effectiveAct).build();

        ea.setActual(null);
        ea.setOverrides(overrides);
        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, transitions), result0.value);

        ProcessorTransitions transitions2 = ProcessorTransitions.newBuilder().setTransitionToActive(true).build();
        ea.setActual(active1);
        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, transitions2), result1.value);

        ProcessorTransitions transitions3 = ProcessorTransitions.newBuilder().setTransitionToNormal(true).build();
        ea.setActual(null);
        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, transitions3), result2.value);

        ea.setActual(active1);
        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, transitions2), result3.value);

        Assert.assertEquals(new IntermediateMonolog(effectiveReg, ea, transitions), result4.value);
    }
}
