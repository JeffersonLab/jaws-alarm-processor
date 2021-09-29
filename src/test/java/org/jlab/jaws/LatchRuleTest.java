package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.MonitorInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class LatchRuleTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Alarm> inputTopicMonolog;
    private TestOutputTopic<String, Alarm> outputPassthroughTopic;
    private TestOutputTopic<OverriddenAlarmKey, OverriddenAlarmValue> outputOverrideTopic;
    private AlarmRegistration registered1;
    private AlarmRegistration registered2;
    private RegisteredClass class1;
    private AlarmActivation active1;
    private AlarmActivation active2;
    private Alarm mono1;

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

        registered1 = new AlarmRegistration();
        registered2 = new AlarmRegistration();

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

        active1 = new AlarmActivation();
        active2 = new AlarmActivation();

        active1.setMsg(new SimpleAlarming());
        active2.setMsg(new SimpleAlarming());

        mono1 = new Alarm();
        mono1.setActivation(active1);
        mono1.setClass$(class1);
        mono1.setRegistration(registered1);
        mono1.setEffectiveRegistration(MonologRule.computeEffectiveRegistration(registered1, class1));
        mono1.setOverrides(new OverrideSet());
        mono1.setTransitions(new TransitionSet());
        mono1.getTransitions().setTransitionToActive(true);
        mono1.getTransitions().setTransitionToNormal(false);
        mono1.setState(AlarmState.Normal);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void notLatching() {
        mono1.getEffectiveRegistration().setLatching(false);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        List<KeyValue<String, Alarm>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }

    @Test
    public void latching() {
        mono1.getEffectiveRegistration().setLatching(true);

        inputTopicMonolog.pipeInput("alarm1", mono1);
        //inputTopicMonolog.pipeInput("alarm2", mono1);
        List<KeyValue<String, Alarm>> passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        List<KeyValue<OverriddenAlarmKey, OverriddenAlarmValue>> overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nInitial Passthrough:");
        for(KeyValue<String, Alarm> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nInitial Overrides:");
        for(KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> over: overrideResults) {
            System.err.println(over);
        }

        System.err.println("\n");

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(1, overrideResults.size());

        KeyValue<String, Alarm> passResult = passthroughResults.get(0);

        Assert.assertEquals(true, passResult.value.getTransitions().getLatching());

        KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> result = overrideResults.get(0);

        Assert.assertEquals("alarm1", result.key.getName());
        Assert.assertEquals(new OverriddenAlarmValue(new LatchedAlarm()), result.value);

        Alarm mono2 = Alarm.newBuilder(mono1).build();

        mono2.getOverrides().setLatched(new LatchedAlarm());
        mono2.getTransitions().setTransitionToActive(false);

        inputTopicMonolog.pipeInput("alarm1", mono2);

        passthroughResults = outputPassthroughTopic.readKeyValuesToList();
        overrideResults = outputOverrideTopic.readKeyValuesToList();

        System.err.println("\n\nFinal Passthrough:");
        for(KeyValue<String, Alarm> pass: passthroughResults) {
            System.err.println(pass);
        }

        System.err.println("\n\nFinal Overrides:");
        for(KeyValue<OverriddenAlarmKey, OverriddenAlarmValue> over: overrideResults) {
            System.err.println(over);
        }

        Assert.assertEquals(1, passthroughResults.size());
        Assert.assertEquals(0, overrideResults.size());
    }
}
