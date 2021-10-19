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

public class MonologRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Alarm> inputTopicRegisteredMonolog;
    private TestInputTopic<String, AlarmActivationUnion> inputTopicActive;
    private TestInputTopic<OverriddenAlarmKey, AlarmOverrideUnion> inputTopicOverridden;
    private TestOutputTopic<String, Alarm> outputTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private AlarmClass class1;
    private AlarmRegistration effectiveRegistered1;
    private AlarmActivationUnion active1;
    private AlarmActivationUnion active2;
    private Alarm registeredMonolog1;

    @Before
    public void setup() {
        final MonologRule rule = new MonologRule("effective-registrations", "active-alarms", "overridden-alarms", "monolog");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicRegisteredMonolog = testDriver.createInputTopic(rule.inputTopicRegisteredMonolog, MonologRule.MONOLOG_KEY_SERDE.serializer(), MonologRule.MONOLOG_VALUE_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(rule.inputTopicActive, MonologRule.ACTIVE_KEY_SERDE.serializer(), MonologRule.ACTIVE_VALUE_SERDE.serializer());
        inputTopicOverridden = testDriver.createInputTopic(rule.inputTopicOverridden, MonologRule.OVERRIDE_KEY_SERDE.serializer(), MonologRule.OVERRIDE_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, MonologRule.MONOLOG_KEY_SERDE.deserializer(), MonologRule.MONOLOG_VALUE_SERDE.deserializer());

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

        class1.setMaskedby("alarm1");
        class1.setOffdelayseconds(5l);
        class1.setOndelayseconds(5l);

        effectiveRegistered1 = EffectiveRegistrationRule.computeEffectiveRegistration(registered1, class1);

        active1 = new AlarmActivationUnion();
        active2 = new AlarmActivationUnion();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        registeredMonolog1 = Alarm.newBuilder()
                .setRegistration(registered1)
                .setClass$(class1)
                .setEffectiveRegistration(effectiveRegistered1)
                .setOverrides(new AlarmOverrideSet())
                .setTransitions(new ProcessorTransitions())
                .setState(AlarmState.Normal)
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

        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void count() {
        inputTopicActive.pipeInput("alarm1", active1);
        inputTopicRegisteredMonolog.pipeInput("alarm1", registeredMonolog1);
        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void unsetActiveWithNoRegistration() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicActive.pipeInput("alarm1", null);
        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
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
        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(2, results.size());

        KeyValue<String, Alarm> result2 = results.get(1);

        Alarm expected = new Alarm(registered1, class1, effectiveRegistered1, active1, new AlarmOverrideSet(), new ProcessorTransitions(), AlarmState.Normal);

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


        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(5, results.size());

        KeyValue<String, Alarm> result = results.get(4);

        AlarmOverrideSet overrides = AlarmOverrideSet.newBuilder()
                .setLatched(new LatchedOverride())
                .build();

        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, new ProcessorTransitions(), AlarmState.Normal), result.value);
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

        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(5, results.size());



        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
            System.err.println(result);
        }

        KeyValue<String, Alarm> result0 = results.get(0);
        KeyValue<String, Alarm> result1 = results.get(1);
        KeyValue<String, Alarm> result2 = results.get(2);
        KeyValue<String, Alarm> result3 = results.get(3);
        KeyValue<String, Alarm> result4 = results.get(4);

        Assert.assertEquals("alarm1", result0.key);

        AlarmOverrideSet overrides = AlarmOverrideSet.newBuilder()
                .build();

        ProcessorTransitions transitions = ProcessorTransitions.newBuilder().build();

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, null, overrides, transitions, AlarmState.Normal), result0.value);

        ProcessorTransitions transitions2 = ProcessorTransitions.newBuilder().setTransitionToActive(true).build();
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions2, AlarmState.Normal), result1.value);

        ProcessorTransitions transitions3 = ProcessorTransitions.newBuilder().setTransitionToNormal(true).build();
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, null, overrides, transitions3, AlarmState.Normal), result2.value);

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions2, AlarmState.Normal), result3.value);

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions, AlarmState.Normal), result4.value);
    }
}
