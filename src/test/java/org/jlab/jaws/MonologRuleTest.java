package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class MonologRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, RegisteredAlarm> inputTopicRegistered;
    private TestInputTopic<String, RegisteredClass> inputTopicClasses;
    private TestInputTopic<String, AlarmActivation> inputTopicActive;
    private TestInputTopic<OverriddenAlarmKey, OverriddenAlarmValue> inputTopicOverridden;
    private TestOutputTopic<String, Alarm> outputTopic;
    private RegisteredAlarm registered1;
    private RegisteredAlarm registered2;
    private RegisteredClass class1;
    private RegisteredAlarm effectiveRegistered1;
    private AlarmActivation active1;
    private AlarmActivation active2;

    @Before
    public void setup() {
        final MonologRule rule = new MonologRule("registered-classes", "registered-alarms", "active-alarms", "overridden-alarms", "monolog");

        final Properties props = rule.constructProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = rule.constructTopology(props);
        testDriver = new TopologyTestDriver(top, props);

        // setup test topics
        inputTopicClasses = testDriver.createInputTopic(rule.inputTopicClasses, MonologRule.INPUT_KEY_CLASSES_SERDE.serializer(), MonologRule.INPUT_VALUE_CLASSES_SERDE.serializer());
        inputTopicRegistered = testDriver.createInputTopic(rule.inputTopicRegistered, MonologRule.INPUT_KEY_REGISTERED_SERDE.serializer(), MonologRule.INPUT_VALUE_REGISTERED_SERDE.serializer());
        inputTopicActive = testDriver.createInputTopic(rule.inputTopicActive, MonologRule.INPUT_KEY_ACTIVE_SERDE.serializer(), MonologRule.INPUT_VALUE_ACTIVE_SERDE.serializer());
        inputTopicOverridden = testDriver.createInputTopic(rule.inputTopicOverridden, MonologRule.OVERRIDE_KEY_SERDE.serializer(), MonologRule.OVERRIDE_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(rule.outputTopic, MonologRule.MONOLOG_KEY_SERDE.deserializer(), MonologRule.MONOLOG_VALUE_SERDE.deserializer());

        registered1 = new RegisteredAlarm();
        registered2 = new RegisteredAlarm();

        registered1.setClass$("base");
        registered1.setProducer(new SimpleProducer());
        registered1.setLatching(true);

        registered2.setClass$("base");
        registered2.setProducer(new SimpleProducer());
        registered2.setLatching(false);

        class1 = new RegisteredClass();
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

        effectiveRegistered1 = MonologRule.computeEffectiveRegistration(registered1, class1);

        active1 = new AlarmActivation();
        active2 = new AlarmActivation();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void count() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered2);
        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void content() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered1);
        inputTopicClasses.pipeInput("base", class1);
        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(3, results.size());

        KeyValue<String, Alarm> result2 = results.get(2);

        Assert.assertEquals("alarm1", result2.key);
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, new OverrideSet(), new TransitionSet(), AlarmState.Normal), result2.value);
    }

    @Test
    public void addOverride() {
        inputTopicActive.pipeInput("alarm1", active1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopicRegistered.pipeInput("alarm1", registered1);

        inputTopicClasses.pipeInput("base", class1);

        OverriddenAlarmValue overriddenAlarmValue1 = new OverriddenAlarmValue();
        LatchedAlarm latchedAlarm = new LatchedAlarm();
        overriddenAlarmValue1.setMsg(latchedAlarm);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), overriddenAlarmValue1);

        OverriddenAlarmValue overriddenAlarmValue2 = new OverriddenAlarmValue();
        DisabledAlarm disabledAlarm = new DisabledAlarm();
        disabledAlarm.setComments("Testing");
        overriddenAlarmValue2.setMsg(disabledAlarm);
        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), overriddenAlarmValue2);


        inputTopicOverridden.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), null);


        List<KeyValue<String, Alarm>> results = outputTopic.readKeyValuesToList();

        System.err.println("\n\n\n");
        for(KeyValue<String, Alarm> result: results) {
            System.err.println(result);
        }

        Assert.assertEquals(6, results.size());

        KeyValue<String, Alarm> result = results.get(5);

        OverrideSet overrides = OverrideSet.newBuilder()
                .build();

        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, new TransitionSet(), AlarmState.Normal), result.value);
    }

    @Test
    public void transitions() {
        inputTopicClasses.pipeInput("base", class1);
        inputTopicRegistered.pipeInput("alarm1", registered1);

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

        OverrideSet overrides = OverrideSet.newBuilder()
                .build();

        TransitionSet transitions = TransitionSet.newBuilder().build();

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, null, overrides, transitions, AlarmState.Normal), result0.value);

        TransitionSet transitions2 = TransitionSet.newBuilder().setTransitionToActive(true).build();
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions2, AlarmState.Normal), result1.value);

        TransitionSet transitions3 = TransitionSet.newBuilder().setTransitionToNormal(true).build();
        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, null, overrides, transitions3, AlarmState.Normal), result2.value);

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions2, AlarmState.Normal), result3.value);

        Assert.assertEquals(new Alarm(registered1, class1, effectiveRegistered1, active1, overrides, transitions, AlarmState.Normal), result4.value);
    }
}
